import { IPositionProducer, INotificationProducer, IMessageCommitter, ProcessResult } from '../../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../../shared/logger';
import * as Metrics from '@mojaloop/central-services-metrics';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import assert from 'assert';
import createRemittanceEntity from './createRemittanceEntity';
import { CreateTransferDto } from '../types';

const { decodePayload } = Util.StreamingProtocol


const rethrow = Util.rethrow;
const { createFSPIOPError } = ErrorHandler.Factory;
const { FSPIOPErrorCodes } = ErrorHandler.Enums;

export interface PrepareHandlerDependencies {
  positionProducer: IPositionProducer;
  notificationProducer: INotificationProducer;
  committer: IMessageCommitter;
  config: any;
  
  // TODO(LD): remove these I think, but interesting to keep a track of them!
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

  // TODO(LD): add some typing
  async handle(error: any, messages: any): Promise<void> {
    if (error) {
      rethrow.rethrowAndCountFspiopError(error, { operation: 'PrepareHandler.handle' });
    }

    // TODO(LD): how should we deal with errors related to message validation?
    assert(Array.isArray(messages))
    assert.equal(messages.length, 1, 'Expected exactly only 1 message from consumers')
    const message = messages[0]
    const input = this.extractMessageData(message);
    
    const histTimerEnd = Metrics.getHistogram(
      input.metric,
      `Consume a ${input.metric} message from the kafka topic and process it accordingly`,
      ['success', 'fspId']
    ).startTimer();

    try {
      // Process the transfer business logic
      const result = await this.processTransfer(input, message);
      await this.deps.committer.commit(message);
      
      // Handle the result - send downstream messages
      await this.handleResult(result, input);
      
      histTimerEnd({ success: true, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId });
      return
    } catch (err) {
      histTimerEnd({ success: false, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId });
      
      await this.deps.committer.commit(message);
    
      await this.handleError(err, input, message);
      return
    }
  }

  // TODO(LD): add types here
  private extractMessageData(message: any) {
    assert(message)
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.headers)
    assert(message.value.metadata)
    assert(message.value.metadata.event)
    assert(message.value.metadata.event.action)
    const payloadEncoded  = message.value.content.payload
    const payload = decodePayload(payloadEncoded, {}) as unknown as CreateTransferDto
    const headers = message.value.content.headers

    const transferId = payload.transferId


    // TODO(LD): copied from dto.js but it's a bad idea
    // const isFx = !payload.transferId
    const action = message.value.metadata?.event?.action || 'prepare';
  
    // TODO(LD): I really don't like passing around booleans like this
    // copied from other parts of the code but this really needs a refactor
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

    // Proxy obligations, copied from original prepare.js, not sure I understand why we have it

    const proxyObligation = await this.calculateProxyObligation(payload, isFx, input);

    // 2. Check for duplicates
    // TODO(LD): refactor this - needs to be moved to a different place to line up with TigerBeetle flow
    const duplication = await this.checkDuplication(payload, transferId, isFx);
    if (duplication.hasDuplicateId) {
      return await this.processDuplication(duplication, input);
    }

    // `determiningTransferCheckResult`, copied from original prepare.js, not sure I understand why we have it
    const determiningTransferCheckResult = await createRemittanceEntity(isFx)
      .checkIfDeterminingTransferExists(proxyObligation.payloadClone, proxyObligation)

    // 3. Validate the transfer
    // TODO(LD): Validation should be before we process I think
    const validation = await this.validateTransfer(
      payload, 
      input.headers, 
      isFx, 
      determiningTransferCheckResult,
      proxyObligation,
    );
    if (validation.validationPassed === false) {
      throw createFSPIOPError(FSPIOPErrorCodes.VALIDATION_ERROR, validation.reasons.join(', '));
    }

    // 4. Save the transfer
    await this.saveTransfer(payload, validation, isFx, determiningTransferCheckResult, proxyObligation);

    // 5. Calculate position data
    const positionData = await this.calculatePositionData(payload, isFx, determiningTransferCheckResult, proxyObligation);

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
      throw new Error(`No position data for successful transfer: ${result.transferId}`)
    }

    const positionMessage = {
      transferId: result.transferId,
      participantCurrencyId: result.positionData.participantCurrencyId,
      amount: result.positionData.amount,
      currency: result.positionData.currency,
      action: this.getPositionAction(input.action),
      cyrilResult: result.positionData.cyrilResult,
      messageKey: result.positionData.messageKey,
      from: input.message.value.from,
      to: input.message.value.to,
      headers: input.headers,
      payload: input.message.value.content.payload, // base64 encoded payload from original message
      metadata: input.message.value.metadata
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
    // Duplicate handling is already done in processDuplication method
    // This method is called when the result type is 'duplicate'
    logger.info('Handled duplicate transfer', {
      transferId: result.transferId,
      isFinalized: result.data?.isFinalized
    });
  }

  private async handleError(error: any, input: any, message: any): Promise<void> {
    // if (error.stack) {
    //   logger.error
    // }
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(error);
    
    await this.deps.notificationProducer.sendError({
      transferId: input.transferId,
      fspiopError: fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
      action: input.action,
      to: message.value.from,
      from: this.deps.config.HUB_NAME,
      headers: input.headers,
      metadata: message.value.metadata
    });

    logger.error('Handled transfer error', {
      transferId: input.transferId,
      error: fspiopError.message,
      stack: error.stack
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
    if (!duplication.hasDuplicateId) {
      return {
        type: 'duplicate',
        transferId: input.transferId,
        data: { isFinalized: false }
      };
    }

    const { action, actionEnum, isFx, transferId, payload } = input;
    const actionLetter = action[0].toUpperCase();

    // Handle hash mismatch or bulk prepare duplicates with errors
    let error;
    if (!duplication.hasDuplicateHash) {
      logger.warn(`callbackErrorModified1--${actionLetter}5 for transfer ${transferId}`);
      error = createFSPIOPError(FSPIOPErrorCodes.MODIFIED_REQUEST);
    } else if (actionEnum === Enum.Events.Event.Action.BULK_PREPARE) {
      logger.info(`validationError1--${actionLetter}2 for transfer ${transferId}`);
      error = createFSPIOPError('Individual transfer prepare duplicate');
    }

    if (error) {
      await this.deps.notificationProducer.sendError({
        transferId,
        fspiopError: error.toApiErrorObject(this.deps.config.ERROR_HANDLING),
        action: actionEnum,
        to: input.message.value.from,
        from: this.deps.config.HUB_NAME,
        headers: input.headers,
        metadata: input.message.value.metadata
      });
      throw error;
    }

    logger.info('handleResend for transfer', { transferId });

    // Get transfer state to determine if it's finalized
    const transfer = await createRemittanceEntity(isFx).getByIdLight(transferId);
    const finalizedState = [
      Enum.Transfers.TransferState.COMMITTED,
      Enum.Transfers.TransferState.ABORTED,
      Enum.Transfers.TransferState.RESERVED
    ];
    
    const isFinalized = 
      finalizedState.includes(transfer?.transferStateEnumeration) ||
      finalizedState.includes(transfer?.fxTransferStateEnumeration);
    
    const isPrepare = [
      Enum.Events.Event.Action.PREPARE,
      Enum.Events.Event.Action.FX_PREPARE,
      Enum.Events.Event.Action.FORWARDED,
      Enum.Events.Event.Action.FX_FORWARDED
    ].includes(actionEnum);

    if (isFinalized) {
      if (isPrepare) {
        logger.info(`finalized callback--${actionLetter}1 for transfer ${transferId}`);
        
        // Transform payload for duplicate response  
        const transformedPayload = this.deps.transferObjectTransform.toFulfil(transfer, isFx);
        const duplicateAction = isFx ? 
          Enum.Events.Event.Action.FX_PREPARE_DUPLICATE : 
          Enum.Events.Event.Action.PREPARE_DUPLICATE;

        await this.deps.notificationProducer.sendDuplicate({
          transferId,
          action: duplicateAction,
          to: input.message.value.from,
          from: this.deps.config.HUB_NAME,
          payload: transformedPayload,
          headers: input.headers,
          metadata: input.message.value.metadata
        });

        return {
          type: 'duplicate',
          transferId,
          data: { 
            isFinalized: true,
            transformedPayload
          }
        };
      } else if (actionEnum === Enum.Events.Event.Action.BULK_PREPARE) {
        logger.info(`validationError1--${actionLetter}2 for transfer ${transferId}`);
        const fspiopError = createFSPIOPError(FSPIOPErrorCodes.MODIFIED_REQUEST, 'Individual transfer prepare duplicate');
        
        await this.deps.notificationProducer.sendError({
          transferId,
          fspiopError: fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
          action: Enum.Events.Event.Action.PREPARE_DUPLICATE,
          to: input.message.value.from,
          from: this.deps.config.HUB_NAME,
          headers: input.headers,
          metadata: input.message.value.metadata
        });
        throw fspiopError;
      }
    } else {
      logger.info('inProgress for transfer', { transferId });
      if (actionEnum === Enum.Events.Event.Action.BULK_PREPARE) {
        logger.info(`validationError2--${actionLetter}4 for transfer ${transferId}`);
        const fspiopError = createFSPIOPError(FSPIOPErrorCodes.MODIFIED_REQUEST, 'Individual transfer prepare duplicate');
        
        await this.deps.notificationProducer.sendError({
          transferId,
          fspiopError: fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
          action: Enum.Events.Event.Action.PREPARE_DUPLICATE,
          to: input.message.value.from,
          from: this.deps.config.HUB_NAME,
          headers: input.headers,
          metadata: input.message.value.metadata
        });
        throw fspiopError;
      } else {
        // For regular prepare duplicates that are in progress, just ignore
        logger.info(`ignore--${actionLetter}3 for transfer ${transferId}`);
        return {
          type: 'duplicate',
          transferId,
          data: { isFinalized: false }
        };
      }
    }

    return {
      type: 'duplicate',
      transferId,
      data: { isFinalized: true }
    };
  }

  private async validateTransfer(payload: any, headers: any, isFx: boolean, determiningTransferCheckResult: any, proxyObligation: any) {
    // Delegate to existing validator
    return await this.deps.validator.validatePrepare(payload, headers, isFx, determiningTransferCheckResult, proxyObligation);
  }

  private async saveTransfer(payload: any, validation: any, isFx: boolean, determiningTransferCheckResult: any, proxyObligation: any) {
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
      determiningTransferCheckResult,
      proxyObligation
    });
  }

  private async calculatePositionData(payload: any, isFx: boolean, determiningTransferCheckResult: any, proxyObligation: any) {
    // Delegate to existing implementation
    const { definePositionParticipant } = require('./prepare');
    const result = await definePositionParticipant({
      payload: proxyObligation.payloadClone,
      isFx,
      determiningTransferCheckResult,
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