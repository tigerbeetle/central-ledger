import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as Metrics from '@mojaloop/central-services-metrics';
import { Enum, EventActionEnum, Util } from '@mojaloop/central-services-shared';
import * as EventSdk from '@mojaloop/event-sdk';
import assert from 'assert';
import { IMessageCommitter, INotificationProducer, ProcessResult } from '../../messaging/types';
import { logger } from '../../shared/logger';
import { CommitTransferDto, CreateTransferDto } from '../types';

const { decodePayload } = Util.StreamingProtocol;
const rethrow = Util.rethrow;

export interface FulfilHandlerDependencies {
  notificationProducer: INotificationProducer;
  committer: IMessageCommitter;
  config: any;

  // Business logic dependencies
  transferService: any;
  validator: any;
  comparators: any;
  fxService: any;
  transferObjectTransform: any;
}

export interface FulfilMessageInput {
  message: any;
  payload: CommitTransferDto;
  headers: Record<string, any>;
  transferId: string;
  action: EventActionEnum;
  eventType: string;
  isBulk: boolean;
  kafkaTopic: string;
}

export class FulfilHandler {
  constructor(private deps: FulfilHandlerDependencies) { }

  async handle(error: any, messages: any): Promise<void> {
    const histTimerEnd = Metrics.getHistogram(
      'transfer_fulfil',
      'Consume a fulfil transfer message from the kafka topic and process it accordingly',
      ['success', 'fspId', 'action']
    ).startTimer();

    if (error) {
      histTimerEnd({ success: false, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId, action: 'error' });
      rethrow.rethrowAndCountFspiopError(error, { operation: 'fulfilHandler' });
      return;
    }

    assert(Array.isArray(messages));
    assert.equal(messages.length, 1, 'Expected exactly only 1 message from consumers');

    const message = messages[0];
    const input = this.extractMessageData(message);
    let span: any;

    try {
      const contextFromMessage = EventSdk.Tracer.extractContextFromMessage(message.value);
      span = EventSdk.Tracer.createChildSpanFromContext('cl_transfer_fulfil', contextFromMessage);
      await span.audit(message, EventSdk.AuditEventAction.start);

      // Process the fulfil message
      const result = await this.processFulfil(input, message);
      await this.deps.committer.commit(message);

      // Handle the result
      await this.handleResult(result, input);

      histTimerEnd({
        success: true,
        fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId,
        action: input.action
      });

      if (span && !span.isFinished) {
        await span.finish();
      }

    } catch (err) {
      histTimerEnd({
        success: false,
        fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId,
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

  private extractMessageData(message: any): FulfilMessageInput {
    assert(message);
    assert(message.value);
    assert(message.value.content);
    assert(message.value.metadata);
    assert(message.value.metadata.event);

    const payloadEncoded = message.value.content.payload;
    const headers = message.value.content.headers;
    const action = message.value.metadata.event.action;
    const eventType = message.value.metadata.event.type;

    // Fulfil messages always use CommitTransferDto
    const payload = decodePayload(payloadEncoded, {}) as CommitTransferDto;

    assert(message.value.content.uriParams);
    assert(message.value.content.uriParams.id);
    const transferId = message.value.content.uriParams.id;

    assert(transferId, 'could not parse transferId');

    const isBulk = action && (
      action.toLowerCase().includes('bulk') ||
      [
        Enum.Events.Event.Action.BULK_COMMIT,
        Enum.Events.Event.Action.BULK_ABORT
      ].includes(action as any)
    );

    return {
      message,
      payload,
      headers,
      transferId,
      action,
      eventType,
      isBulk,
      kafkaTopic: message.topic
    };
  }

  private async processFulfil(input: FulfilMessageInput, message: any): Promise<ProcessResult> {
    const { payload, transferId, action, eventType, isBulk } = input;

    try {
      const result = await this.executeFulfilLogic(input, message);

      return {
        type: 'success',
        transferId,
        data: result
      };
    } catch (error) {
      return {
        type: 'error',
        transferId,
        error
      };
    }
  }

  private async executeFulfilLogic(input: FulfilMessageInput, message: any): Promise<any> {
    const { payload, transferId, action, eventType, isBulk, kafkaTopic } = input;

    if (eventType === Enum.Events.Event.Type.FULFIL &&
      (action === Enum.Events.Event.Action.COMMIT || action === Enum.Events.Event.Action.BULK_COMMIT)) {

      return await this.handleFulfilCommit(input, message);
    }

    if (eventType === Enum.Events.Event.Type.FULFIL &&
      (action === Enum.Events.Event.Action.ABORT || action === Enum.Events.Event.Action.BULK_ABORT)) {

      return await this.handleFulfilAbort(input, message);
    }

    if (eventType === Enum.Events.Event.Type.FULFIL &&
      action === Enum.Events.Event.Action.RESERVE) {

      return await this.handleFulfilReserve(input, message);
    }

    throw new Error(`Unsupported fulfil action: ${action} for eventType: ${eventType}`);
  }

  private async handleFulfilCommit(input: FulfilMessageInput, message: any): Promise<any> {
    const { transferId, payload, headers } = input;

    logger.info(`Processing fulfil commit for transfer: ${transferId}`);

    try {
      // Validate participant
      if (!await this.deps.validator.validateParticipantByName(message.value.from)) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND, 'Participant not found');
      }

      // Get transfer details
      const transfer = await this.deps.transferService.getById(transferId);
      if (!transfer) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 'Transfer ID not found');
      }

      // Validate transfer participant
      if (!await this.deps.validator.validateParticipantTransferId(message.value.from, transferId)) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Participant not associated with transfer');
      }

      // Validate headers (FSPIOP source/destination)
      if (headers[Enum.Http.Headers.FSPIOP.SOURCE] && !transfer.payeeIsProxy &&
        (headers[Enum.Http.Headers.FSPIOP.SOURCE].toLowerCase() !== transfer.payeeFsp.toLowerCase())) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Source header does not match transfer payee');
      }

      if (headers[Enum.Http.Headers.FSPIOP.DESTINATION] && !transfer.payerIsProxy &&
        (headers[Enum.Http.Headers.FSPIOP.DESTINATION].toLowerCase() !== transfer.payerFsp.toLowerCase())) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Destination header does not match transfer payer');
      }

      // Check for duplicates
      const dupCheckResult = await this.deps.comparators.duplicateCheckComparator(
        transferId,
        payload,
        this.deps.transferService.getTransferFulfilmentDuplicateCheck,
        this.deps.transferService.saveTransferFulfilmentDuplicateCheck
      )

      if (dupCheckResult.hasDuplicateId && dupCheckResult.hasDuplicateHash) {
        // Handle duplicate fulfil
        logger.info(`Duplicate fulfil detected for transfer: ${transferId}`);
        await this.sendDuplicateNotification(input, message, transfer);
        return { status: 'duplicate', transferId };
      }

      if (dupCheckResult.hasDuplicateId && !dupCheckResult.hasDuplicateHash) {
        // Different fulfil for same transfer
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.MODIFIED_REQUEST, 'Transfer fulfil has been modified');
      }

      // Validate fulfilment condition
      if (payload.fulfilment && !this.deps.validator.validateFulfilCondition(payload.fulfilment, transfer.condition)) {
        const fspiopError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'Invalid fulfilment');
        const apiError = fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING);

        await this.deps.transferService.handlePayeeResponse(transferId, payload, Enum.Events.Event.Action.ABORT_VALIDATION, apiError);
        await this.sendErrorNotification(input, message, apiError);

        throw fspiopError;
      }

      // Process the fulfil
      await this.deps.transferService.handlePayeeResponse(transferId, payload, input.action);

      // Handle FX processing
      const cyrilResult = await this.deps.fxService.Cyril.processFulfilMessage(transferId, payload, transfer);

      // Send to position topic
      await this.sendToPositionTopic(input, message, transfer, cyrilResult);

      logger.info(`Fulfil commit processed successfully for transfer: ${transferId}`);

      return {
        status: 'committed',
        transferId,
        cyrilResult
      };

    } catch (error) {
      logger.error(`Fulfil commit failed for transfer: ${transferId}`, { error: error.message });
      throw error;
    }
  }

  private async handleFulfilAbort(input: FulfilMessageInput, message: any): Promise<any> {
    const { transferId, payload } = input;

    logger.info(`Processing fulfil abort for transfer: ${transferId}`);

    try {
      // Similar validation as commit but for abort
      const transfer = await this.deps.transferService.getByIdLight(transferId);
      if (!transfer) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 'Transfer ID not found');
      }

      await this.deps.transferService.handlePayeeResponse(transferId, payload, input.action);

      // Send to position topic
      await this.sendToPositionTopic(input, message, transfer);

      logger.info(`Fulfil abort processed successfully for transfer: ${transferId}`);

      return {
        status: 'aborted',
        transferId
      };

    } catch (error) {
      logger.error(`Fulfil abort failed for transfer: ${transferId}`, { error: error.message });
      throw error;
    }
  }

  private async handleFulfilReserve(input: FulfilMessageInput, message: any): Promise<any> {
    const { transferId, payload, headers } = input;

    // Check for v1.0 content-type with RESERVED state - fail silently
    if (headers['content-type'] && headers['content-type'].split('=')[1] === '1.0' &&
      payload.transferState === 'RESERVED') {

      logger.info(`Ignoring RESERVE action for v1.0 client: ${transferId}`);
      return { status: 'ignored', transferId, reason: 'v1.0 RESERVE not allowed' };
    }

    // Process similar to commit but for reserve
    logger.info(`Processing fulfil reserve for transfer: ${transferId}`);

    const transfer = await this.deps.transferService.getByIdLight(transferId);
    if (!transfer) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 'Transfer ID not found');
    }

    await this.deps.transferService.handlePayeeResponse(transferId, payload, input.action);
    await this.sendToPositionTopic(input, message, transfer);

    return {
      status: 'reserved',
      transferId
    };
  }

  private async sendToPositionTopic(input: FulfilMessageInput, message: any, transfer: any, cyrilResult?: any): Promise<void> {
    // This would send the message to the position topic for position processing
    // Implementation would depend on the position producer setup
    logger.debug(`Sending to position topic for transfer: ${input.transferId}`, { action: input.action });

    // Add cyril result to message context if available
    if (cyrilResult && cyrilResult.isFx) {
      message.value.content.context = {
        ...message.value.content.context,
        cyrilResult
      };
    }

    // The actual sending to position topic would happen here
    // This is typically handled by the Kafka.proceed function in the original implementation
  }

  private async sendDuplicateNotification(input: FulfilMessageInput, message: any, transfer: any): Promise<void> {
    logger.debug(`Sending duplicate notification for fulfil: ${input.transferId}`);

    try {
      const transformedPayload = this.deps.transferObjectTransform.toFulfil(transfer);

      await this.deps.notificationProducer.sendDuplicate({
        transferId: input.transferId,
        action: input.action,
        to: message.value.from,  // Back to originator
        from: this.deps.config.HUB_NAME,
        payload: transformedPayload,
        headers: input.headers,
        metadata: message.value.metadata
      });

      logger.debug(`Duplicate notification sent for transfer: ${input.transferId}`);

    } catch (error) {
      logger.error(`Failed to send duplicate notification for transfer: ${input.transferId}`, { error });
    }
  }

  private async sendErrorNotification(input: FulfilMessageInput, message: any, fspiopApiError: any): Promise<void> {
    logger.debug(`Sending error notification for fulfil: ${input.transferId}`, {
      errorCode: fspiopApiError.errorInformation.errorCode
    });

    try {
      await this.deps.notificationProducer.sendError({
        transferId: input.transferId,
        fspiopError: fspiopApiError,
        action: input.action,
        to: message.value.from,  // Back to originator
        from: this.deps.config.HUB_NAME,
        headers: input.headers,
        metadata: message.value.metadata
      });

      logger.debug(`Error notification sent for transfer: ${input.transferId}`);

    } catch (error) {
      logger.error(`Failed to send error notification for transfer: ${input.transferId}`, { error });
    }
  }

  private async handleResult(result: ProcessResult, input: FulfilMessageInput): Promise<void> {
    switch (result.type) {
      case 'success':
        await this.handleSuccess(result, input);
        break;
      case 'error':
        await this.handleErrorResult(result, input);
        break;
    }
  }

  private async handleSuccess(result: ProcessResult, input: FulfilMessageInput): Promise<void> {
    logger.info('Fulfil processing completed successfully', {
      transferId: result.transferId,
      action: input.action
    });
  }

  private async handleErrorResult(result: ProcessResult, input: FulfilMessageInput): Promise<void> {
    await this.handleError(result.error, input, input.message);
  }

  private async handleError(error: any, input: FulfilMessageInput, message: any): Promise<void> {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(error);

    logger.error('Fulfil processing error', {
      transferId: input.transferId,
      error: fspiopError.message,
      action: input.action
    });

    // Send error notification
    if (input.transferId && message?.value?.from) {
      try {
        await this.deps.notificationProducer.sendError({
          transferId: input.transferId,
          fspiopError: fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
          action: input.action,
          to: message.value.from,
          from: this.deps.config.HUB_NAME,
          headers: input.headers,
          metadata: message.value.metadata
        });
      } catch (notificationError) {
        logger.error('Failed to send error notification', { notificationError });
      }
    }
  }
}