import { IMessageCommitter, ProcessResult } from '../../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../../shared/logger';
import * as Metrics from '@mojaloop/central-services-metrics';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as EventSdk from '@mojaloop/event-sdk';
import assert from 'assert';
import { AdminTransferDto } from '../types';

const { decodePayload } = Util.StreamingProtocol;
const rethrow = Util.rethrow;

export interface AdminHandlerDependencies {
  committer: IMessageCommitter;
  config: any;

  // Business logic dependencies
  transferService: any;
  comparators: any;
  db: any;
}

export interface AdminMessageInput {
  message: any;
  payload: AdminTransferDto;
  transferId: string;
  action: string;
  eventType: string;
  kafkaTopic: string;
  metadata: any;
  transactionTimestamp: string;
  enums: any;
}

export class AdminHandler {
  private readonly httpPostRelatedActions = [
    Enum.Events.Event.Action.RECORD_FUNDS_IN, 
    Enum.Events.Event.Action.RECORD_FUNDS_OUT_PREPARE_RESERVE
  ];

  private readonly httpPutRelatedActions = [
    Enum.Events.Event.Action.RECORD_FUNDS_OUT_COMMIT, 
    Enum.Events.Event.Action.RECORD_FUNDS_OUT_ABORT
  ];

  private readonly allowedActions = [...this.httpPostRelatedActions, ...this.httpPutRelatedActions];

  constructor(private deps: AdminHandlerDependencies) {}

  async handle(error: any, messages: any): Promise<void> {
    const histTimerEnd = Metrics.getHistogram(
      'admin_transfer',
      'Consume an admin transfer message from the kafka topic and process it accordingly',
      ['success', 'action']
    ).startTimer();

    if (error) {
      histTimerEnd({ success: false, action: 'error' });
      rethrow.rethrowAndCountFspiopError(error, { operation: 'adminTransfer' });
      return;
    }

    assert(Array.isArray(messages));
    assert.equal(messages.length, 1, 'Expected exactly only 1 message from consumers');

    const message = messages[0];
    const input = this.extractMessageData(message);
    let span: any;

    try {
      const contextFromMessage = EventSdk.Tracer.extractContextFromMessage(message.value);
      span = EventSdk.Tracer.createChildSpanFromContext('cl_admin_transfer', contextFromMessage);
      await span.audit(message, EventSdk.AuditEventAction.start);

      // Process the admin transfer message
      const result = await this.processAdminTransfer(input, message);
      await this.deps.committer.commit(message);

      // Handle the result
      await this.handleResult(result, input);

      histTimerEnd({
        success: true,
        action: input.action
      });

      if (span && !span.isFinished) {
        await span.finish();
      }

    } catch (err) {
      histTimerEnd({
        success: false,
        action: input?.action || 'unknown'
      });

      await this.deps.committer.commit(message);
      await this.handleError(err, input, message);

      if (span && !span.isFinished) {
        const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(err);
        const state = new EventSdk.EventStateMetadata(EventSdk.EventStatusType.failed, fspiopError.apiErrorCode.code, fspiopError.apiErrorCode.message);
        await span.error(fspiopError, state);
        await span.finish(fspiopError.message, state);
      }
    }
  }

  private extractMessageData(message: any): AdminMessageInput {
    assert(message);
    assert(message.value);
    assert(message.value.content);
    assert(message.value.metadata);

    const payload = message.value.content.payload;
    const metadata = message.value.metadata;
    const transferId = message.value.id;
    const action = metadata.event.action;
    const eventType = metadata.event.type;

    if (!payload) {
      throw new Error('AdminTransferHandler::validationFailed - no payload provided');
    }

    // Add participant currency ID from metadata
    payload.participantCurrencyId = metadata.request?.params?.id;
    const enums = metadata.request?.enums;
    const transactionTimestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

    return {
      message,
      payload: payload as AdminTransferDto,
      transferId,
      action,
      eventType,
      kafkaTopic: message.topic,
      metadata,
      transactionTimestamp,
      enums
    };
  }

  private async processAdminTransfer(input: AdminMessageInput, message: any): Promise<ProcessResult> {
    const { payload, transferId, action, transactionTimestamp, enums } = input;

    try {
      // Validate action is allowed
      if (!this.allowedActions.includes(payload.action as any)) {
        logger.info(`AdminTransferHandler::${payload.action}::invalidPayloadAction`);
        
        return {
          type: 'error',
          transferId,
          error: new Error(`Invalid payload action: ${payload.action}`)
        };
      }

      logger.info(`AdminTransferHandler::${action}::${transferId}`);

      if (this.httpPostRelatedActions.includes(payload.action as any)) {
        return await this.processPostAction(input);
      } else {
        return await this.processPutAction(input);
      }

    } catch (error) {
      return {
        type: 'error',
        transferId,
        error
      };
    }
  }

  private async processPostAction(input: AdminMessageInput): Promise<ProcessResult> {
    const { payload, transferId, transactionTimestamp, enums } = input;

    // Check for duplicates
    const { hasDuplicateId, hasDuplicateHash } = await this.deps.comparators.duplicateCheckComparator(
      transferId, 
      payload, 
      this.deps.transferService.getTransferDuplicateCheck, 
      this.deps.transferService.saveTransferDuplicateCheck
    );

    if (!hasDuplicateId) {
      // New transfer - create it
      logger.info(`AdminTransferHandler::${payload.action}::transfer does not exist`);
      await this.createRecordFundsInOut(payload, transactionTimestamp, enums);
      
      return {
        type: 'success',
        transferId,
        data: { action: 'created', duplicateCheck: 'new' }
      };

    } else if (hasDuplicateHash) {
      // Duplicate with same hash - handle existing transfer
      await this.transferExists(payload, transferId);
      
      return {
        type: 'success',
        transferId,
        data: { action: 'duplicate_same', duplicateCheck: 'same_hash' }
      };

    } else {
      // Duplicate with different hash - log and continue
      logger.info(`AdminTransferHandler::${payload.action}::dupcheck::existsNotMatching::request exists with different parameters`);
      
      return {
        type: 'success',
        transferId,
        data: { action: 'duplicate_different', duplicateCheck: 'different_hash' }
      };
    }
  }

  private async processPutAction(input: AdminMessageInput): Promise<ProcessResult> {
    const { payload, transferId, transactionTimestamp, enums } = input;

    await this.changeStatusOfRecordFundsOut(payload, transferId, transactionTimestamp, enums);
    
    return {
      type: 'success',
      transferId,
      data: { action: 'status_changed' }
    };
  }

  private async createRecordFundsInOut(payload: AdminTransferDto, transactionTimestamp: string, enums: any): Promise<void> {
    const knex = this.deps.db.getKnex();

    logger.info(`AdminTransferHandler::${payload.action}::validationPassed::newEntry`);

    if (payload.action === (Enum.Events.Event.Action.RECORD_FUNDS_IN as string)) {
      logger.info(`AdminTransferHandler::${payload.action}::validationPassed::newEntry::RECORD_FUNDS_IN`);
      return await this.deps.transferService.recordFundsIn(payload, transactionTimestamp, enums);
    } else {
      logger.info(`AdminTransferHandler::${payload.action}::validationPassed::newEntry::RECORD_FUNDS_OUT_PREPARE_RESERVE`);
      return knex.transaction(async (trx: any) => {
        try {
          await this.deps.transferService.reconciliationTransferPrepare(payload, transactionTimestamp, enums, trx);
          await this.deps.transferService.reconciliationTransferReserve(payload, transactionTimestamp, enums, trx);
        } catch (err) {
          rethrow.rethrowAndCountFspiopError(err, { operation: 'adminCreateRecordFundsInOut' });
        }
      });
    }
  }

  private async changeStatusOfRecordFundsOut(
    payload: AdminTransferDto, 
    transferId: string, 
    transactionTimestamp: string, 
    enums: any
  ): Promise<boolean> {
    const existingTransfer = await this.deps.transferService.getTransferById(transferId);
    const transferState = await this.deps.transferService.getTransferState(transferId);

    if (!existingTransfer) {
      logger.info(`AdminTransferHandler::${payload.action}::validationFailed::notFound`);
    } else if (transferState.transferStateId !== Enum.Transfers.TransferState.RESERVED) {
      logger.info(`AdminTransferHandler::${payload.action}::validationFailed::nonReservedState`);
    } else if (new Date(existingTransfer.expirationDate) <= new Date()) {
      logger.info(`AdminTransferHandler::${payload.action}::validationFailed::transferExpired`);
    } else {
      logger.info(`AdminTransferHandler::${payload.action}::validationPassed`);
      
      if (payload.action === (Enum.Events.Event.Action.RECORD_FUNDS_OUT_COMMIT as string)) {
        logger.info(`AdminTransferHandler::${payload.action}::validationPassed::RECORD_FUNDS_OUT_COMMIT`);
        await this.deps.transferService.reconciliationTransferCommit(payload, transactionTimestamp, enums);
      } else if (payload.action === (Enum.Events.Event.Action.RECORD_FUNDS_OUT_ABORT as string)) {
        logger.info(`AdminTransferHandler::${payload.action}::validationPassed::RECORD_FUNDS_OUT_ABORT`);
        payload.amount = {
          amount: existingTransfer.amount,
          currency: existingTransfer.currencyId
        };
        await this.deps.transferService.reconciliationTransferAbort(payload, transactionTimestamp, enums);
      }
    }
    
    return true;
  }

  private async transferExists(payload: AdminTransferDto, transferId: string): Promise<boolean> {
    logger.info(`AdminTransferHandler::${payload.action}::dupcheck::existsMatching`);
    
    const currentTransferState = await this.deps.transferService.getTransferStateChange(transferId);
    
    if (!currentTransferState || !currentTransferState.enumeration) {
      logger.info(`AdminTransferHandler::${payload.action}::dupcheck::existsMatching::transfer state not found`);
    } else {
      const transferStateEnum = currentTransferState.enumeration;
      
      if (transferStateEnum === Enum.Transfers.TransferState.COMMITTED || 
          transferStateEnum === Enum.Transfers.TransferInternalState.ABORTED_REJECTED) {
        logger.info(`AdminTransferHandler::${payload.action}::dupcheck::existsMatching::request already finalized`);
      } else if (transferStateEnum === Enum.Transfers.TransferInternalState.RECEIVED_PREPARE || 
                 transferStateEnum === Enum.Transfers.TransferState.RESERVED) {
        logger.info(`AdminTransferHandler::${payload.action}::dupcheck::existsMatching::previous request still in progress do nothing`);
      }
    }
    
    return true;
  }

  private async handleResult(result: ProcessResult, input: AdminMessageInput): Promise<void> {
    switch (result.type) {
      case 'success':
        await this.handleSuccess(result, input);
        break;
      case 'error':
        await this.handleErrorResult(result, input);
        break;
    }
  }

  private async handleSuccess(result: ProcessResult, input: AdminMessageInput): Promise<void> {
    logger.info('Admin transfer processing completed successfully', {
      transferId: result.transferId,
      action: input.action,
      data: result.data
    });
  }

  private async handleErrorResult(result: ProcessResult, input: AdminMessageInput): Promise<void> {
    await this.handleError(result.error, input, input.message);
  }

  private async handleError(error: any, input: AdminMessageInput, message: any): Promise<void> {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(error);

    logger.error('Admin transfer processing error', {
      transferId: input.transferId,
      error: fspiopError.message,
      action: input.action
    });

    // For admin transfers, we typically don't send notifications back
    // The admin operations are internal and responses go through the API layer
  }
}