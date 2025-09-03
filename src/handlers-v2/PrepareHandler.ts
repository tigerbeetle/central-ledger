import { IPositionProducer, INotificationProducer, IMessageCommitter, ProcessResult } from '../messaging/types';
import CentralServicesShared, { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../shared/logger';
import * as Metrics from '@mojaloop/central-services-metrics';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import assert from 'assert';
import createRemittanceEntity from '../handlers/transfers/createRemittanceEntity';
import { CreateTransferDto } from './types';

const { decodePayload } = Util.StreamingProtocol

const rethrow = Util.rethrow;
const { createFSPIOPError } = ErrorHandler.Factory;
const { FSPIOPErrorCodes } = ErrorHandler.Enums;

// Type definitions
export interface DuplicationCheckResult {
  hasDuplicateId: boolean;
  hasDuplicateHash: boolean;
}

export interface Location {
  module: string;
  method: string;
  path: string;
}

export interface ProxyObligation {
  isFx: boolean;
  payloadClone: CreateTransferDto;
  isInitiatingFspProxy: boolean;
  isCounterPartyFspProxy: boolean;
  initiatingFspProxyOrParticipantId: any;
  counterPartyFspProxyOrParticipantId: any;
}

export interface TransferCheckResult {
  watchListRecords: any[];
  participantCurrencyValidationList: any[];
}

export interface ValidationResult {
  validationPassed: boolean;
  reasons: string[];
}

export interface DefinePositionParticipantResult {
  messageKey: string;
  cyrilResult: any; // Complex object from position participant calculation
}

export interface PositionData {
  participantCurrencyId: string;
  amount: string;
  currency: string;
  cyrilResult: any;
  messageKey: string;
}
export interface PrepareHandlerDependencies {
  positionProducer: IPositionProducer;
  notificationProducer: INotificationProducer;
  committer: IMessageCommitter;
  config: any;
  
  // Business logic dependencies - injected from existing modules
  validator: {
    validatePrepare: (payload: CreateTransferDto, headers: any, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation) => Promise<ValidationResult>;
    [key: string]: any;
  };
  transferService: any;
  proxyCache: any;
  comparators: any;
  createRemittanceEntity: any;
  transferObjectTransform: any;
  
  // Business logic functions from prepare.js
  checkDuplication: (args: { payload: CreateTransferDto, isFx: boolean, ID: string, location: Location }) => Promise<DuplicationCheckResult>;
  savePreparedRequest: (args: { 
    validationPassed: boolean, 
    reasons: string[], 
    payload: CreateTransferDto, 
    isFx: boolean, 
    functionality: any, 
    params: any, 
    location: Location, 
    determiningTransferCheckResult: TransferCheckResult, 
    proxyObligation: ProxyObligation 
  }) => Promise<void>;
  definePositionParticipant: (args: { payload: CreateTransferDto, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation }) => Promise<DefinePositionParticipantResult>;
}

export interface PrepareMessageInput {
    message: any;
    payload: CreateTransferDto;
    headers: any;
    transferId: string;
    action: any;
    isFx: boolean;
    isBulk: boolean;
    isForwarded: boolean;
    metric: string;
    functionality: CentralServicesShared.EventTypeEnum.TRANSFER;
    actionEnum: string;
}

export class PrepareHandler {
  constructor(private deps: PrepareHandlerDependencies) {}

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

  private extractMessageData(message: any): PrepareMessageInput {
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

  private async processTransfer(input: PrepareMessageInput, message: any): Promise<ProcessResult> {
    const { payload, transferId, isFx, action, functionality } = input;

    // Check for duplicates
    const duplication = await this.checkDuplication(payload, transferId, isFx);
    if (duplication.hasDuplicateId) {
      return await this.processDuplication(duplication, input);
    }

    // Create minimal objects for validation compatibility
    const proxyObligation = this.createMinimalProxyObligation(payload, isFx);
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult();

    // Validate the transfer
    const validation = await this.validateTransfer(
      payload, 
      input.headers, 
      isFx, 
      determiningTransferCheckResult,
      proxyObligation,
    );
    // Save the transfer with minimal objects
    await this.saveTransfer(payload, validation, isFx, determiningTransferCheckResult, proxyObligation);

    if (validation.validationPassed === false) {
      throw createFSPIOPError(FSPIOPErrorCodes.VALIDATION_ERROR, validation.reasons.join(', '));
    }

    // Calculate position data with minimal objects  
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

  private getPositionAction(action: string): 'PREPARE' | 'FX_PREPARE' | 'BULK_PREPARE' {
    const actionUpper = action.toUpperCase();
    if (actionUpper.includes('FX')) return 'FX_PREPARE';
    if (actionUpper.includes('BULK')) return 'BULK_PREPARE';
    return 'PREPARE';
  }

  // Helper methods to create minimal objects for validation compatibility
  private createMinimalProxyObligation(payload: CreateTransferDto, isFx: boolean): ProxyObligation {
    return {
      isFx,
      payloadClone: { ...payload },
      isInitiatingFspProxy: false,
      isCounterPartyFspProxy: false,
      initiatingFspProxyOrParticipantId: null,
      counterPartyFspProxyOrParticipantId: null
    };
  }

  private createMinimalTransferCheckResult(): TransferCheckResult {
    return {
      watchListRecords: [],
      participantCurrencyValidationList: []
    };
  }

  private async checkDuplication(payload: CreateTransferDto, transferId: string, isFx: boolean): Promise<DuplicationCheckResult> {
    return await this.deps.checkDuplication({
      payload,
      isFx,
      ID: transferId,
      location: { module: 'PrepareHandler', method: 'checkDuplication', path: '' }
    });
  }

  private async processDuplication(duplication: DuplicationCheckResult, input: PrepareMessageInput): Promise<ProcessResult> {
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
    if (!duplication.hasDuplicateHash) {
      logger.warn(`callbackErrorModified1--${actionLetter}5 for transfer ${transferId}`);
      throw createFSPIOPError(FSPIOPErrorCodes.MODIFIED_REQUEST);
    } else if (actionEnum === Enum.Events.Event.Action.BULK_PREPARE) {
      logger.info(`validationError1--${actionLetter}2 for transfer ${transferId}`);
      throw createFSPIOPError('Individual transfer prepare duplicate');
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
    ].includes(actionEnum as any);

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

  private async validateTransfer(payload: CreateTransferDto, headers: any, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation): Promise<ValidationResult> {
    // Delegate to existing validator
    return await this.deps.validator.validatePrepare(payload, headers, isFx, determiningTransferCheckResult, proxyObligation);
  }

  private async saveTransfer(payload: CreateTransferDto, validation: ValidationResult, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation): Promise<void> {
    return await this.deps.savePreparedRequest({
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

  private async calculatePositionData(payload: CreateTransferDto, isFx: boolean, determiningTransferCheckResult: TransferCheckResult, proxyObligation: ProxyObligation): Promise<PositionData> {
    const result = await this.deps.definePositionParticipant({
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