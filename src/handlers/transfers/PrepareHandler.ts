import { IPositionProducer, INotificationProducer, IMessageCommitter, ProcessResult } from '../../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../../shared/logger';
import * as Metrics from '@mojaloop/central-services-metrics';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';

const rethrow = Util.rethrow;
const { createFSPIOPError } = ErrorHandler.Factory;
const { FSPIOPErrorCodes } = ErrorHandler.Enums;

export interface PrepareHandlerDependencies {
  positionProducer: IPositionProducer;
  notificationProducer: INotificationProducer;
  committer: IMessageCommitter;
  config: any;
  
  // Business logic dependencies - these will come from existing modules
  validator: any;
  transferService: any;
  proxyCache: any;
  comparators: any;
  createRemittanceEntity: any;
  transferObjectTransform: any;
}

export class PrepareHandler {
  constructor(private deps: PrepareHandlerDependencies) {}

  async handle(error: any, message: any): Promise<void> {
    if (error) {
      rethrow.rethrowAndCountFspiopError(error, { operation: 'PrepareHandler.handle' });
    }

    // Convert single message to array format for compatibility with existing logic
    const messages = [message];
    const input = this.extractMessageData(message);
    
    const histTimerEnd = Metrics.getHistogram(
      input.metric,
      `Consume a ${input.metric} message from the kafka topic and process it accordingly`,
      ['success', 'fspId']
    ).startTimer();

    try {
      // Process the transfer business logic
      const result = await this.processTransfer(input, message);
      
      // Commit the incoming message
      await this.deps.committer.commit(message);
      
      // Handle the result - send downstream messages
      await this.handleResult(result, input);
      
      histTimerEnd({ success: true, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId });
      return
    } catch (err) {
      histTimerEnd({ success: false, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId });
      
      // Even on error, commit the message so we don't reprocess it
      await this.deps.committer.commit(message);
      
      // Send error notification
      await this.handleError(err, input, message);
      return
    }
  }

  private extractMessageData(message: any) {
    const { payload } = message.value.content;
    const headers = message.value.content.headers || {};
    const transferId = payload.transferId;
    const action = message.value.metadata?.event?.action || 'prepare';
    
    // Determine if this is an FX transfer or bulk transfer
    const isFx = action.toLowerCase().includes('fx');
    const isBulk = action.toLowerCase().includes('bulk');
    const isForwarded = action.toLowerCase().includes('forward');

    return {
      message,
      payload,
      headers,
      transferId,
      action,
      isFx,
      isBulk,
      isForwarded,
      metric: `handler_transfers_${action.toLowerCase()}`,
      functionality: Enum.Events.Event.Type.TRANSFER,
      actionEnum: this.getActionEnum(action)
    };
  }

  private getActionEnum(action: string): string {
    const actionUpper = action.toUpperCase();
    return Enum.Events.Event.Action[actionUpper] || actionUpper;
  }

  private async processTransfer(input: any, message: any): Promise<ProcessResult> {
    const { payload, transferId, isFx, action, functionality } = input;

    // 1. Calculate proxy obligations
    const proxyObligation = await this.calculateProxyObligation(payload, isFx, input);

    // 2. Check for duplicates
    const duplication = await this.checkDuplication(payload, transferId, isFx);
    if (duplication.hasDuplicateId) {
      return await this.processDuplication(duplication, input);
    }

    // 3. Validate the transfer
    const validation = await this.validateTransfer(payload, input.headers, isFx, proxyObligation);
    if (!validation.validationPassed) {
      throw createFSPIOPError(FSPIOPErrorCodes.VALIDATION_ERROR, validation.reasons.join(', '));
    }

    // 4. Save the transfer
    await this.saveTransfer(payload, validation, isFx, proxyObligation);

    // 5. Calculate position data
    const positionData = await this.calculatePositionData(payload, isFx, proxyObligation);

    return {
      type: 'success',
      transferId,
      positionData
    };
  }

  private async handleResult(result: ProcessResult, input: any): Promise<void> {
    switch (result.type) {
      case 'success':
        await this.handleSuccess(result, input);
        break;
      case 'duplicate':
        await this.handleDuplicate(result, input);
        break;
      case 'error':
        await this.handleErrorResult(result, input);
        break;
    }
  }

  private async handleSuccess(result: ProcessResult, input: any): Promise<void> {
    if (!result.positionData) {
      logger.warn('No position data for successful transfer', { transferId: result.transferId });
      return;
    }

    const positionMessage = {
      transferId: result.transferId!,
      participantCurrencyId: result.positionData.participantCurrencyId,
      amount: result.positionData.amount,
      currency: result.positionData.currency,
      action: this.getPositionAction(input.action),
      cyrilResult: result.positionData.cyrilResult,
      messageKey: result.positionData.messageKey
    };

    // Send to position handler depending on message type
    if (input.isFx) {
      await this.deps.positionProducer.sendFxPrepare(positionMessage);
    } else if (input.isBulk) {
      await this.deps.positionProducer.sendBulkPrepare(positionMessage);
    } else {
      await this.deps.positionProducer.sendPrepare(positionMessage);
    }

    logger.info('Successfully sent message to position handler', {
      transferId: result.transferId,
      action: positionMessage.action,
      participantCurrencyId: positionMessage.participantCurrencyId
    });
  }

  private async handleDuplicate(result: ProcessResult, input: any): Promise<void> {
    // Handle duplicate transfer notifications
    if (result.data?.isFinalized) {
      await this.deps.notificationProducer.sendDuplicate({
        transferId: result.transferId!,
        action: input.action + '_DUPLICATE',
        to: input.payload.payerFsp,
        from: this.deps.config.HUB_NAME,
        payload: result.data.transformedPayload
      });
    }
    
    logger.info('Handled duplicate transfer', {
      transferId: result.transferId,
      isFinalized: result.data?.isFinalized
    });
  }

  private async handleError(error: any, input: any, message: any): Promise<void> {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(error);
    
    await this.deps.notificationProducer.sendError({
      transferId: input.transferId,
      fspiopError: fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
      action: input.action,
      to: message.value.from,
      from: this.deps.config.HUB_NAME
    });

    logger.error('Handled transfer error', {
      transferId: input.transferId,
      error: fspiopError.message
    });
  }

  private async handleErrorResult(result: ProcessResult, input: any): Promise<void> {
    await this.handleError(result.error, input, input.message);
  }

  private getPositionAction(action: string): 'PREPARE' | 'COMMIT' | 'ABORT' | 'FX_PREPARE' | 'BULK_PREPARE' {
    const actionUpper = action.toUpperCase();
    if (actionUpper.includes('FX')) return 'FX_PREPARE';
    if (actionUpper.includes('BULK')) return 'BULK_PREPARE';
    return 'PREPARE';
  }

  // Business logic methods - these delegate to existing implementations
  private async calculateProxyObligation(payload: any, isFx: boolean, input: any) {
    // Delegate to existing implementation
    const { calculateProxyObligation } = require('./prepare');
    return await calculateProxyObligation({
      payload,
      isFx,
      params: { message: input.message },
      functionality: input.functionality,
      action: input.actionEnum
    });
  }

  private async checkDuplication(payload: any, transferId: string, isFx: boolean) {
    // Delegate to existing implementation
    const { checkDuplication } = require('./prepare');
    return await checkDuplication({
      payload,
      isFx,
      ID: transferId,
      location: { module: 'PrepareHandler', method: 'checkDuplication', path: '' }
    });
  }

  private async processDuplication(duplication: any, input: any): Promise<ProcessResult> {
    // Delegate to existing implementation for now
    const { processDuplication } = require('./prepare');
    const result = await processDuplication({
      duplication,
      isFx: input.isFx,
      ID: input.transferId,
      functionality: input.functionality,
      action: input.actionEnum,
      actionLetter: input.action[0].toUpperCase(),
      params: { message: input.message },
      location: { module: 'PrepareHandler', method: 'processDuplication', path: '' }
    });

    return {
      type: 'duplicate',
      transferId: input.transferId,
      data: { isFinalized: result }
    };
  }

  private async validateTransfer(payload: any, headers: any, isFx: boolean, proxyObligation: any) {
    // Delegate to existing validator
    return await this.deps.validator.validatePrepare(payload, headers, isFx, null, proxyObligation);
  }

  private async saveTransfer(payload: any, validation: any, isFx: boolean, proxyObligation: any) {
    // Delegate to existing implementation
    const { savePreparedRequest } = require('./prepare');
    return await savePreparedRequest({
      validationPassed: validation.validationPassed,
      reasons: validation.reasons,
      payload,
      isFx,
      functionality: Enum.Events.Event.Type.TRANSFER,
      params: { message: null }, // Not used in current implementation
      location: { module: 'PrepareHandler', method: 'saveTransfer', path: '' },
      determiningTransferCheckResult: null,
      proxyObligation
    });
  }

  private async calculatePositionData(payload: any, isFx: boolean, proxyObligation: any) {
    // Delegate to existing implementation
    const { definePositionParticipant } = require('./prepare');
    const result = await definePositionParticipant({
      payload: proxyObligation.payloadClone,
      isFx,
      determiningTransferCheckResult: null,
      proxyObligation
    });

    return {
      participantCurrencyId: result.messageKey,
      amount: payload.amount.amount,
      currency: payload.amount.currency,
      cyrilResult: result.cyrilResult,
      messageKey: result.messageKey
    };
  }
}