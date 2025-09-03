import { INotificationProducer, IMessageCommitter, ProcessResult } from '../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../shared/logger';
import * as Metrics from '@mojaloop/central-services-metrics';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as EventSdk from '@mojaloop/event-sdk';
import assert from 'assert';

const { decodePayload } = Util.StreamingProtocol;
const rethrow = Util.rethrow;

export interface GetHandlerDependencies {
  notificationProducer: INotificationProducer;
  committer: IMessageCommitter;
  config: any;

  // Business logic dependencies
  validator: any;
  transferService: any;
  fxTransferModel: any;
  transferObjectTransform: any;
}

export interface GetMessageInput {
  message: any;
  transferId: string;
  action: string;
  eventType: string;
  isFx: boolean;
  kafkaTopic: string;
  headers: Record<string, any>;
}

export class GetHandler {
  constructor(private deps: GetHandlerDependencies) {}

  async handle(error: any, messages: any): Promise<void> {
    const histTimerEnd = Metrics.getHistogram(
      'transfer_get',
      'Consume a get transfer message from the kafka topic and process it accordingly',
      ['success', 'fspId']
    ).startTimer();

    if (error) {
      histTimerEnd({ success: false, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId });
      rethrow.rethrowAndCountFspiopError(error, { operation: 'getTransfer' });
      return;
    }

    assert(Array.isArray(messages));
    assert.equal(messages.length, 1, 'Expected exactly only 1 message from consumers');

    const message = messages[0];
    const input = this.extractMessageData(message);
    let span: any;

    try {
      const contextFromMessage = EventSdk.Tracer.extractContextFromMessage(message.value);
      span = EventSdk.Tracer.createChildSpanFromContext('cl_transfer_get', contextFromMessage);
      await span.audit(message, EventSdk.AuditEventAction.start);

      // Process the get transfer message
      const result = await this.processGetTransfer(input, message);
      await this.deps.committer.commit(message);

      // Handle the result
      await this.handleResult(result, input, message);

      histTimerEnd({
        success: true,
        fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId
      });

      if (span && !span.isFinished) {
        await span.finish();
      }

    } catch (err) {
      histTimerEnd({
        success: false,
        fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId
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

  private extractMessageData(message: any): GetMessageInput {
    assert(message);
    assert(message.value);
    assert(message.value.content);
    assert(message.value.metadata);
    assert(message.value.metadata.event);

    const headers = message.value.content.headers;
    const action = message.value.metadata.event.action;
    const eventType = message.value.metadata.event.type;
    const transferId = message.value.content.uriParams?.id;

    assert(transferId, 'could not parse transferId from uriParams');

    const isFx = action === Enum.Events.Event.Action.FX_GET;

    return {
      message,
      transferId,
      action,
      eventType,
      isFx,
      kafkaTopic: message.topic,
      headers
    };
  }

  private async processGetTransfer(input: GetMessageInput, message: any): Promise<ProcessResult> {
    const { transferId, isFx } = input;

    try {
      // Validate participant exists
      if (!await this.deps.validator.validateParticipantByName(message.value.from)) {
        logger.info(`Participant does not exist: ${message.value.from} for transfer: ${transferId}`);
        
        return {
          type: 'success', // This is actually successful processing (invalid participant is handled gracefully)
          transferId,
          data: { skipProcessing: true }
        };
      }

      if (isFx) {
        return await this.processFxGet(input, message);
      } else {
        return await this.processTransferGet(input, message);
      }

    } catch (error) {
      return {
        type: 'error',
        transferId,
        error
      };
    }
  }

  private async processFxGet(input: GetMessageInput, message: any): Promise<ProcessResult> {
    const { transferId } = input;

    // Get FX transfer
    const fxTransfer = await this.deps.fxTransferModel.fxTransfer.getByIdLight(transferId);
    if (!fxTransfer) {
      const fspiopError = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 
        'Provided commitRequest ID was not found on the server.'
      );
      
      return {
        type: 'error',
        transferId,
        error: fspiopError
      };
    }

    // Validate participant has access to this FX transfer
    if (!await this.deps.validator.validateParticipantForCommitRequestId(message.value.from, transferId)) {
      const fspiopError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR);
      
      return {
        type: 'error',
        transferId,
        error: fspiopError
      };
    }

    // Transform the FX transfer for response
    message.value.content.payload = this.deps.transferObjectTransform.toFulfil(fxTransfer, true);

    return {
      type: 'success',
      transferId,
      data: { fxTransfer, payload: message.value.content.payload }
    };
  }

  private async processTransferGet(input: GetMessageInput, message: any): Promise<ProcessResult> {
    const { transferId } = input;

    // Get regular transfer
    const transfer = await this.deps.transferService.getByIdLight(transferId);
    if (!transfer) {
      const fspiopError = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 
        'Provided Transfer ID was not found on the server.'
      );
      
      return {
        type: 'error',
        transferId,
        error: fspiopError
      };
    }

    // Validate participant has access to this transfer
    if (!await this.deps.validator.validateParticipantTransferId(message.value.from, transferId)) {
      const fspiopError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR);
      
      return {
        type: 'error',
        transferId,
        error: fspiopError
      };
    }

    // Transform the transfer for response
    message.value.content.payload = this.deps.transferObjectTransform.toFulfil(transfer);

    return {
      type: 'success',
      transferId,
      data: { transfer, payload: message.value.content.payload }
    };
  }

  private async handleResult(result: ProcessResult, input: GetMessageInput, message: any): Promise<void> {
    switch (result.type) {
      case 'success':
        await this.handleSuccess(result, input, message);
        break;
      case 'error':
        await this.handleErrorResult(result, input, message);
        break;
    }
  }

  private async handleSuccess(result: ProcessResult, input: GetMessageInput, message: any): Promise<void> {
    // Skip processing means we handled an invalid participant gracefully
    if (result.data?.skipProcessing) {
      return;
    }

    logger.info('Get transfer processing completed successfully', {
      transferId: result.transferId,
      action: input.action,
      isFx: input.isFx
    });

    // Send the successful response back via notification producer
    await this.deps.notificationProducer.sendSuccess({
      transferId: input.transferId,
      action: input.action,
      to: message.value.from,  // Response goes back to requester
      from: this.deps.config.HUB_NAME,
      payload: message.value.content.payload,
      headers: input.headers,
      metadata: message.value.metadata
    });
  }

  private async handleErrorResult(result: ProcessResult, input: GetMessageInput, message: any): Promise<void> {
    await this.handleError(result.error, input, message);
  }

  private async handleError(error: any, input: GetMessageInput, message: any): Promise<void> {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(error);

    logger.error('Get transfer processing error', {
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