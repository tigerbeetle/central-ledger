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

export interface PositionHandlerDependencies {
  notificationProducer: INotificationProducer;
  committer: IMessageCommitter;
  config: any;

  // Business logic dependencies
  transferService: any;
  positionService: any;
  participantFacade: any;
  settlementModelCached: any;
  transferObjectTransform: any;
}

export interface PositionMessageInput {
  message: any;
  payload: CreateTransferDto | CommitTransferDto;
  headers: Record<string, any>;
  transferId: string;
  action: EventActionEnum
  eventType: string;
  isBulk: boolean;
  kafkaTopic: string;
}

export class PositionHandler {
  constructor(private deps: PositionHandlerDependencies) { }

  async handle(error: any, messages: any): Promise<void> {
    const histTimerEnd = Metrics.getHistogram(
      'transfer_position',
      'Consume a prepare transfer message from the kafka topic and process it accordingly',
      ['success', 'fspId', 'action']
    ).startTimer();

    if (error) {
      histTimerEnd({ success: false, fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId, action: 'error' });
      rethrow.rethrowAndCountFspiopError(error, { operation: 'positionsHandler' });
      return;
    }

    assert(Array.isArray(messages));
    assert.equal(messages.length, 1, 'Expected exactly only 1 message from consumers');

    const message = messages[0];
    const input = this.extractMessageData(message);
    let span: any;

    try {
      const contextFromMessage = EventSdk.Tracer.extractContextFromMessage(message.value);
      span = EventSdk.Tracer.createChildSpanFromContext('cl_transfer_position', contextFromMessage);
      await span.audit(message, EventSdk.AuditEventAction.start);

      // Process the position message
      const result = await this.processPosition(input, message);
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

  private extractMessageData(message: any): PositionMessageInput {
    assert(message);
    assert(message.value);
    assert(message.value.content);
    assert(message.value.metadata);
    assert(message.value.metadata.event);

    const payloadEncoded = message.value.content.payload;
    const headers = message.value.content.headers;
    const action = message.value.metadata.event.action
    const eventType = message.value.metadata.event.type;

    // Decode and type the payload based on action
    let payload: CreateTransferDto | CommitTransferDto;
    let transferId: string

    if (action === Enum.Events.Event.Action.COMMIT || action === Enum.Events.Event.Action.BULK_COMMIT) {
      payload = decodePayload(payloadEncoded, {}) as CommitTransferDto;
      assert(message.value.content.uriParams)
      assert(message.value.content.uriParams.id)
      transferId = message.value.content.uriParams.id
    } else {
      // Default to CreateTransferDto for PREPARE and other actions
      payload = decodePayload(payloadEncoded, {}) as CreateTransferDto;
      transferId = payload.transferId
    }

    assert(transferId)

    const isBulk = action && (
      action.toLowerCase().includes('bulk') ||
      [
        Enum.Events.Event.Action.BULK_PREPARE,
        Enum.Events.Event.Action.BULK_COMMIT,
        Enum.Events.Event.Action.BULK_TIMEOUT_RESERVED,
        Enum.Events.Event.Action.BULK_ABORT
      ].includes(action)
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

  private async processPosition(input: PositionMessageInput, message: any): Promise<ProcessResult> {
    const { payload, transferId, action, eventType, isBulk } = input;

    // Delegate to the original position handling logic
    const { positions } = require('./handler');

    try {
      // Call the original positions function but extract only the business logic we need
      // This is a temporary approach while we migrate
      const result = await this.executePositionLogic(input, message);

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

  private async executePositionLogic(input: PositionMessageInput, message: any): Promise<any> {
    const { payload, transferId, action, eventType, isBulk, kafkaTopic } = input;

    if (eventType === Enum.Events.Event.Type.POSITION &&
      (action === Enum.Events.Event.Action.PREPARE || action === Enum.Events.Event.Action.BULK_PREPARE)) {

      return await this.handlePositionPrepare(input, message);
    } 

    if (eventType === Enum.Events.Event.Type.POSITION &&
      (action === Enum.Events.Event.Action.COMMIT || action === Enum.Events.Event.Action.BULK_COMMIT)) {

      return await this.handlePositionCommit(input, message);
    }

    throw new Error(`Unsupported position action: ${action} for eventType: ${eventType}`);
  }

  private async handlePositionPrepare(input: PositionMessageInput, message: any): Promise<any> {
    const { transferId, isBulk, payload } = input;

    logger.info(`Processing position prepare for transfer: ${transferId}`, { isBulk });

    try {
      // Call the position service to calculate prepare positions

      // This is a hacky way to do this, but the existing implementation need to have the whole message
      // including all metadata etc.
      message.value.content.payload = input.payload
      const prepareBatch = [message];
      // maybe need to set message.value.content.payload to input.payload
      const { preparedMessagesList } = await this.deps.positionService.calculatePreparePositionsBatch(prepareBatch);

      assert(Array.isArray(preparedMessagesList))
      assert(preparedMessagesList.length === 1)

      // Process the prepared messages results
      const prepareMessage = preparedMessagesList[0];
      const { transferState, fspiopError } = prepareMessage;

      if (transferState.transferStateId === Enum.Transfers.TransferState.RESERVED) {
        // Success case - funds reserved successfully
        logger.info(`Position prepare successful - funds reserved for transfer: ${transferId}`);

        await this.sendSuccessNotification(input, message);

        return {
          status: 'success',
          transferId,
          transferState: transferState.transferStateId
        };

      }

      // Failure case - insufficient liquidity
      logger.info(`Position prepare failed - insufficient liquidity for transfer: ${transferId}`);

      const responseFspiopError = fspiopError || ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR
      );

      // Log the transfer error
      const fspiopApiError = responseFspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING);
      await this.deps.transferService.logTransferError(
        transferId,
        fspiopApiError.errorInformation.errorCode,
        fspiopApiError.errorInformation.errorDescription
      );

      await this.sendErrorNotification(input, message, fspiopApiError);

    } catch (error) {
      logger.error(`Position prepare failed for transfer: ${transferId}`, { error: error.message });
      throw error;
    }
  }

  private async handlePositionCommit(input: PositionMessageInput, message: any): Promise<any> {
    // Implement position commit logic here
    logger.info(`Processing position commit for transfer: ${input.transferId}`);

    // Placeholder - implement actual business logic
    return { status: 'committed', transferId: input.transferId };
  }

  private async sendSuccessNotification(input: PositionMessageInput, message: any): Promise<void> {
    // For position prepare success, we pass through the message with original from/to
    // The switch is just confirming the position change was successful

    logger.debug(`Sending success notification for position prepare: ${input.transferId}`);

    try {
      await this.deps.notificationProducer.sendSuccess({
        transferId: input.transferId,
        action: input.action,
        to: message.value.to,        // Keep original destination
        from: message.value.from,    // Keep original source
        payload: message.value.content.payload, // Pass through the original payload
        headers: input.headers,
        metadata: message.value.metadata
      });

      logger.debug(`Success notification sent for transfer: ${input.transferId}`);

    } catch (error) {
      logger.error(`Failed to send success notification for transfer: ${input.transferId}`, { error });
      // Don't throw here as the position operation was successful
    }
  }

  private async sendErrorNotification(input: PositionMessageInput, message: any, fspiopApiError: any): Promise<void> {
    // For error notifications, the Hub is reporting the error back to the originating FSP
    logger.debug(`Sending error notification for position prepare: ${input.transferId}`, {
      errorCode: fspiopApiError.errorInformation.errorCode
    });

    try {
      await this.deps.notificationProducer.sendError({
        transferId: input.transferId,
        fspiopError: fspiopApiError,
        action: input.action,
        to: message.value.from,      // Error goes back to the originator
        from: this.deps.config.HUB_NAME,  // Hub is reporting the error
        headers: input.headers,
        metadata: message.value.metadata
      });

      logger.debug(`Error notification sent for transfer: ${input.transferId}`);

    } catch (error) {
      logger.error(`Failed to send error notification for transfer: ${input.transferId}`, { error });
      // Continue with the error throw in the calling method
    }
  }

  private async handleResult(result: ProcessResult, input: PositionMessageInput): Promise<void> {
    switch (result.type) {
      case 'success':
        await this.handleSuccess(result, input);
        break;
      case 'error':
        await this.handleErrorResult(result, input);
        break;
    }
  }

  private async handleSuccess(result: ProcessResult, input: PositionMessageInput): Promise<void> {
    logger.info('Position processing completed successfully', {
      transferId: result.transferId,
      action: input.action
    });

    // Send downstream notifications if needed
    // This would depend on the specific position action
  }

  private async handleErrorResult(result: ProcessResult, input: PositionMessageInput): Promise<void> {
    await this.handleError(result.error, input, input.message);
  }

  private async handleError(error: any, input: PositionMessageInput, message: any): Promise<void> {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(error);

    logger.error('Position processing error', {
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