import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as Metrics from '@mojaloop/central-services-metrics';
import { Enum, EventActionEnum, Util } from '@mojaloop/central-services-shared';
import assert from 'assert';
import LegacyCompatibleLedger, { FulfilResult, FulfilResultType } from '../domain/ledger-v2/LegacyCompatibleLedger';
import { IMessageCommitter, INotificationProducer, IPositionProducer } from '../messaging/types';
import { ApplicationConfig } from '../shared/config';
import { logger } from '../shared/logger';
import { CommitTransferDto } from './types';

import * as EventSdk from '@mojaloop/event-sdk';

const { decodePayload } = Util.StreamingProtocol
const rethrow = Util.rethrow;
const { createFSPIOPError } = ErrorHandler.Factory;
const { FSPIOPErrorCodes } = ErrorHandler.Enums;


export interface FusedFulfilHandlerDependencies {
  positionProducer: IPositionProducer
  notificationProducer: INotificationProducer
  committer: IMessageCommitter
  config: ApplicationConfig
  ledger: LegacyCompatibleLedger
}

export type SupportedFulfilHandlerAction = EventActionEnum.ABORT | EventActionEnum.COMMIT | EventActionEnum.RESERVE;

export interface FusedFulfilHandlerInput {
  message: any;
  payload: CommitTransferDto;
  headers: Record<string, any>;
  transferId: string;
  action: SupportedFulfilHandlerAction;
  eventType: string;
  kafkaTopic: string;
}

export class FusedFulfilHandler {
  constructor(private deps: FusedFulfilHandlerDependencies) {

  }

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

    try {
      assert.equal(input.eventType, Enum.Events.Event.Type.FULFIL, 'Expected event type to be `FULFIL`')
      const contextFromMessage = EventSdk.Tracer.extractContextFromMessage(message.value);
      
      // Process the fulfil message
      const result = await this.deps.ledger.fulfil(input)
      await this.deps.committer.commit(message);

      // Handle the result
      await this.handleResult(result, input);

      histTimerEnd({
        success: true,
        fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId,
        action: input.action
      });
    } catch (err) {
      histTimerEnd({
        success: false,
        fspId: this.deps.config.INSTRUMENTATION_METRICS_LABELS.fspId,
        action: input.action
      });

      await this.deps.committer.commit(message);
      await this.handleError(err, input, message);
    }
  }

  private extractMessageData(message: any): FusedFulfilHandlerInput {
    assert(message);
    assert(message.value);
    assert(message.value.content);
    assert(message.value.metadata);
    assert(message.value.metadata.event);

    const payloadEncoded = message.value.content.payload;
    const headers = message.value.content.headers;
    
    const eventType = message.value.metadata.event.type;

    // Fulfil messages always use CommitTransferDto
    const payload = decodePayload(payloadEncoded, {}) as CommitTransferDto;

    assert(message.value.content.uriParams);
    assert(message.value.content.uriParams.id);
    const transferId = message.value.content.uriParams.id;
    assert(transferId, 'could not parse transferId');

    // TODO(LD): what should action be?
    const actionStr = message.value.metadata.event.action;
    assert(actionStr)
    let action: SupportedFulfilHandlerAction
    switch (actionStr) {
      case Enum.Events.Event.Action.ABORT:
      case Enum.Events.Event.Action.COMMIT:
      case Enum.Events.Event.Action.RESERVE:
        action = actionStr as SupportedFulfilHandlerAction
        break;
      case Enum.Events.Event.Action.BULK_ABORT:
      case Enum.Events.Event.Action.BULK_COMMIT:
        throw new Error(`FusedFulfilHandler.extractMessageData() - action: ${action} not currently supported `)
      default:
        throw new Error(`FusedFulfilHandler.extractMessageData() - unexpected action: ${action}.`)
    }

    return {
      message,
      payload,
      headers,
      transferId,
      action,
      eventType,
      kafkaTopic: message.topic
    };
  }

  private async handleResult(result: FulfilResult, input: FusedFulfilHandlerInput): Promise<void> {
    switch (result.type) {
      case FulfilResultType.PASS: {
        // TODO(LD): better typing elsewhere
        assert(input.message)
        assert(input.message.value)
        assert(input.message.value.to)
        assert(input.message.value.from)
        assert(input.message.value.content)
        assert(input.message.value.content.payload)
        assert(input.message.value.metadata)

        // TODO: we need to send a message to the payee when action === 'RESERVED'

        await this.deps.notificationProducer.sendSuccess({
          transferId: input.transferId,
          action: input.action,
          to: input.message.value.to,
          from: input.message.value.from,
          payload: input.message.value.content.payload,
          headers: input.headers,
          metadata: input.message.value.metadata
        });
        break;
      }
      case FulfilResultType.FAIL_VALIDATION:
      case FulfilResultType.FAIL_OTHER: {
        throw new Error('not implemented')
      }
    }
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
}