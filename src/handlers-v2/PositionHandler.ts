import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as Metrics from '@mojaloop/central-services-metrics';
import { Enum, EventActionEnum, Util } from '@mojaloop/central-services-shared';
import * as EventSdk from '@mojaloop/event-sdk';
import assert from 'assert';
import { AbortTransferDto, CommitTransferDto, CreateTransferDto } from '../handlers-v2/types';
import { IMessageCommitter, INotificationProducer, ProcessResult } from '../messaging/types';
import { logger } from '../shared/logger';

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

  // For settlement notifications
  kafkaUtil: any;
  positionProducer: any;
}

export interface PositionMessageInput {
  message: any;
  payload: CreateTransferDto | CommitTransferDto | AbortTransferDto
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
    let payload: CreateTransferDto | CommitTransferDto | AbortTransferDto
    let transferId: string

    if (action === Enum.Events.Event.Action.COMMIT ||
      action === Enum.Events.Event.Action.BULK_COMMIT ||
      action === Enum.Events.Event.Action.RESERVE
    ) {
      payload = decodePayload(payloadEncoded, {}) as CommitTransferDto;
      assert(message.value.content.uriParams)
      assert(message.value.content.uriParams.id)
      transferId = message.value.content.uriParams.id
    } else if (action === Enum.Events.Event.Action.TIMEOUT_RESERVED) {
      assert(message.value.content.uriParams)
      assert(message.value.content.uriParams.id)
      transferId = message.value.content.uriParams.id
      payload = decodePayload(payloadEncoded, {}) as AbortTransferDto
    }
    else {
      // Default to CreateTransferDto for PREPARE and other actions
      try {
        payload = decodePayload(payloadEncoded, {}) as CreateTransferDto;
        transferId = payload.transferId
      } catch (err) {
        logger.error(`decode payload failed - action is: ${action}`)
      }
    }

    assert(transferId, 'could not parse transferId')

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
    const { transferId } = input;

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
    const { action, eventType } = input;

    if (eventType !== Enum.Events.Event.Type.POSITION) {
      throw new Error(`executePositionLogic - unexpected eventType: ${eventType}`)
    }

    switch (action) {
      case Enum.Events.Event.Action.PREPARE:
      case Enum.Events.Event.Action.BULK_PREPARE: {
        return await this.handlePositionPrepare(input, message);
      }
      case Enum.Events.Event.Action.COMMIT:
      case Enum.Events.Event.Action.BULK_COMMIT:
      case Enum.Events.Event.Action.RESERVE: {
        return await this.handlePositionCommit(input, message);
      }
      case Enum.Events.Event.Action.TIMEOUT_RESERVED:
      case Enum.Events.Event.Action.FX_TIMEOUT_RESERVED:
      case Enum.Events.Event.Action.BULK_TIMEOUT_RESERVED: {
        return await this.handlePositionTimeout(input, message);
      }
      default: {
        throw new Error(`executePositionLogic action: ${action}`);
      }
    }
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
    const { transferId, action } = input;

    // Validate payload
    const validatePayload = input.payload as CommitTransferDto
    assert(validatePayload.transferState)
    if (validatePayload.transferState !== 'COMMITTED' && validatePayload.transferState !== 'RESERVED') {
      throw new Error('handlePositionCommit validation error - expected payload to be a `CommitTransferDto`')
    }

    logger.info(`Processing position commit for transfer: ${transferId}`);

    try {
      // Get transfer info to change position for PAYEE
      const transferInfo = await this.deps.transferService.getTransferInfoToChangePosition(
        transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYEE_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      // Get participant currency info
      const participantCurrency = await this.deps.participantFacade.getByIDAndCurrency(
        transferInfo.participantId,
        transferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      // Validate transfer state - must be RECEIVED_FULFIL
      if (transferInfo.transferStateId !== Enum.Transfers.TransferInternalState.RECEIVED_FULFIL) {
        const expectedState = Enum.Transfers.TransferInternalState.RECEIVED_FULFIL;
        const fspiopError = ErrorHandler.Factory.createInternalServerFSPIOPError(
          `Invalid State: ${transferInfo.transferStateId} - expected: ${expectedState}`
        );

        logger.error(`Position commit validation failed - invalid state for transfer: ${transferId}`, {
          currentState: transferInfo.transferStateId,
          expectedState
        });

        await this.sendErrorNotification(input, message, fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING));
        throw fspiopError;
      }

      logger.info(`Position commit validation passed for transfer: ${transferId}`);

      // Change participant position (not a reversal for commit)
      const isReversal = false;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferState.COMMITTED
      };

      await this.deps.positionService.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      // For RESERVE action, transform the payload
      if (action === Enum.Events.Event.Action.RESERVE) {
        const transfer = await this.deps.transferService.getById(transferInfo.transferId);
        message.value.content.payload = this.deps.transferObjectTransform.toFulfil(transfer);
      }

      // Send success notification
      await this.sendSuccessNotification(input, message);

      // Send settlement notification
      await this.sendSettlementNotification(input, message, transferInfo);

      logger.info(`Position commit processed successfully for transfer: ${transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      });

      return {
        status: 'committed',
        transferId,
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      };

    } catch (error) {
      logger.error(`Position commit failed for transfer: ${transferId}`, { error: error.message });
      throw error;
    }
  }

  private async sendSuccessNotification(input: PositionMessageInput, message: any): Promise<void> {
    logger.debug(`Sending success notification for position prepare: ${input.transferId}`);

    try {
      await this.deps.notificationProducer.sendSuccess({
        transferId: input.transferId,
        action: input.action,
        to: message.value.to,
        from: message.value.from,
        payload: message.value.content.payload,
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
  }

  private async handleErrorResult(result: ProcessResult, input: PositionMessageInput): Promise<void> {
    await this.handleError(result.error, input, input.message);
  }

  private async handlePositionTimeout(input: PositionMessageInput, message: any): Promise<any> {
    const { transferId, action } = input;

    logger.info(`Processing position timeout for transfer: ${transferId}`, { action });

    try {
      // lookup the participants to notify
      const { payeeFsp, payerFsp } = await this.deps.transferService.getById(transferId)

      // Get transfer info to reverse the position for PAYER (who had funds reserved)
      const transferInfo = await this.deps.transferService.getTransferInfoToChangePosition(
        transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYER_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      // Get participant currency info
      const participantCurrency = await this.deps.participantFacade.getByIDAndCurrency(
        transferInfo.participantId,
        transferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      // Reverse the position change (abort the reserved amounts)
      const isReversal = true;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferInternalState.EXPIRED_RESERVED,
        reason: ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED.message
      }

      await this.deps.positionService.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      // Create timeout error for notification (preserve extensionList from original payload)
      const extensionList = (input.payload as any)?.extensionList || null;
      const timeoutError = ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED,
        null, null, null,
        extensionList
      );
      const fspiopApiError = timeoutError.toApiErrorObject(this.deps.config.ERROR_HANDLING);

      // Determine the notification action based on the original timeout action
      let notificationAction: string;
      if (action === Enum.Events.Event.Action.FX_TIMEOUT_RESERVED) {
        notificationAction = Enum.Events.Event.Action.FX_TIMEOUT_RECEIVED;
      } else if (action === Enum.Events.Event.Action.BULK_TIMEOUT_RESERVED) {
        notificationAction = Enum.Events.Event.Action.BULK_TIMEOUT_RECEIVED;
      } else {
        notificationAction = Enum.Events.Event.Action.TIMEOUT_RECEIVED;
      }

      // Send timeout error notification to the originating DFSP
      await this.sendTimeoutErrorNotifications(
        input, message, fspiopApiError, notificationAction, payerFsp, payeeFsp
      );

      logger.info(`Position timeout processed successfully for transfer: ${transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount,
        isReversal: true
      });

      return {
        status: 'aborted',
        transferId,
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount,
        reason: 'timeout'
      };

    } catch (error) {
      logger.error(`Position timeout processing failed for transfer: ${transferId}`, { error: error.message });
      throw error;
    }
  }

  private async sendTimeoutErrorNotifications(
    input: PositionMessageInput,
    message: any,
    fspiopApiError: any,
    notificationAction: string,
    payerFsp: string,
    payeeFsp: string,
  ): Promise<void> {
    logger.debug(`Sending timeout error notification for transfer: ${input.transferId}`, {
      errorCode: fspiopApiError.errorInformation.errorCode,
      notificationAction
    });

    try {
      // Create timeout error notification message
      // The message structure should match what the timeout handler would send
      const timeoutMessage = { ...message };

      // Add context for timeout notification (payer/payee info)
      if (!timeoutMessage.value.content.context) {
        timeoutMessage.value.content.context = {};
      }

      timeoutMessage.value.content.context.payer = payerFsp;
      timeoutMessage.value.content.context.payee = payeeFsp;

      await this.deps.notificationProducer.sendError({
        transferId: input.transferId,
        fspiopError: fspiopApiError,
        action: notificationAction,
        to: payerFsp,  // Timeout error goes back to the payer FSP
        from: this.deps.config.HUB_NAME,
        headers: input.headers,
        metadata: timeoutMessage.value.metadata,
        payload: timeoutMessage.value.content.payload  // Use original timeout message payload
      });

      await this.deps.notificationProducer.sendError({
        transferId: input.transferId,
        fspiopError: fspiopApiError,
        action: notificationAction,
        to: payeeFsp,  // Timeout error goes back to the payer FSP
        from: this.deps.config.HUB_NAME,
        headers: input.headers,
        metadata: timeoutMessage.value.metadata,
        payload: timeoutMessage.value.content.payload  // Use original timeout message payload
      });


      logger.debug(`Timeout error notifications sent for transfer: ${input.transferId}`);

    } catch (error) {
      logger.error(`Failed to send timeout error notification for transfer: ${input.transferId}`, { error });
      // Don't throw here as the position reversal was successful
    }
  }

  private async sendSettlementNotification(input: PositionMessageInput, message: any, transferInfo: any): Promise<void> {
    try {
      // Replicate the legacy Kafka.proceed call for settlement service integration

      // Build message in the same format as legacy handler
      const kafkaMessage = {
        value: {
          id: message.value.id,
          from: message.value.from,
          to: message.value.to,
          type: Enum.Events.Event.Type.POSITION,
          content: {
            headers: message.value.content.headers,
            payload: message.value.content.payload
          },
          metadata: {
            event: {
              id: message.value.metadata.event.id,
              type: Enum.Events.Event.Type.POSITION,
              action: input.action,
              createdAt: new Date().toISOString(),
              state: {
                status: 'success',
                code: 0
              }
            }
          }
        },
        key: message.key,
        topic: message.topic,
        partition: message.partition,
        offset: message.offset
      };

      // Use KafkaUtil to transform to participant-specific topic
      // This matches what the legacy Kafka.proceed was doing
      const participantTopicName = this.deps.kafkaUtil.transformAccountToTopicName(
        message.value.to, // participant name
        Enum.Events.Event.Type.NOTIFICATION,
        input.action
      );

      logger.debug(`Sending settlement notification to topic: ${participantTopicName}`, {
        transferId: input.transferId,
        action: input.action,
        participantName: message.value.to
      });

      // Use the underlying producer directly since PositionProducer doesn't expose a generic sendMessage
      await this.deps.positionProducer.producer.sendMessage(
        kafkaMessage,
        {
          topicName: participantTopicName,
          opaqueKey: input.transferId
        }
      );

      logger.info(`Settlement notification sent for transfer ${input.transferId}`, {
        topic: participantTopicName,
        action: input.action
      });

    } catch (error) {
      logger.error(`Failed to send settlement notification for transfer ${input.transferId}`, { error });
      // Don't throw here - the position change was successful, just log the notification error
    }
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