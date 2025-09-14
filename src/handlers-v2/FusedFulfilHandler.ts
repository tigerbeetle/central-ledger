import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as Metrics from '@mojaloop/central-services-metrics';
import { Enum, EventActionEnum, Util } from '@mojaloop/central-services-shared';
import assert from 'assert';
import { IMessageCommitter, INotificationProducer, IPositionProducer } from '../messaging/types';
import { ApplicationConfig } from '../shared/config';
import { logger } from '../shared/logger';
import { CommitTransferDto } from './types';

import { Ledger } from '../domain/ledger-v2/Ledger';
import { FulfilResult, FulfilResultType } from '../domain/ledger-v2/types';

const { decodePayload } = Util.StreamingProtocol
const rethrow = Util.rethrow;


export interface FusedFulfilHandlerDependencies {
  positionProducer: IPositionProducer
  notificationProducer: INotificationProducer
  committer: IMessageCommitter
  config: ApplicationConfig
  ledger: Ledger
}

export type SupportedFulfilHandlerAction = EventActionEnum.ABORT 
  | EventActionEnum.COMMIT 
  | EventActionEnum.RESERVE;

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
    if (error) {
      rethrow.rethrowAndCountFspiopError(error, { operation: 'fulfilHandler' });
      return;
    }

    assert(Array.isArray(messages));

    if (messages.length === 0) {
      logger.debug('FusedFulfilHandler.handle() - received empty batch, nothing to process');
      return;
    }

    logger.debug(`FusedFulfilHandler.handle() - processing batch of ${messages.length} messages`)

    // Extract message data for all messages
    const inputs = messages.map(message => ({
      message,
      input: this.extractMessageData(message)
    }));

    // Validate all messages have FULFIL event type
    for (const { input } of inputs) {
      assert.equal(input.eventType, Enum.Events.Event.Type.FULFIL, 'Expected event type to be `FULFIL`')
    }

    // Process all ledger operations in parallel
    const results = await Promise.allSettled(
      inputs.map(async ({ input }) => this.deps.ledger.fulfil(input))
    );

    // Combine inputs with their results
    const processedResults = inputs.map(({ message, input }, index) => ({
      message,
      input,
      result: results[index].status === 'fulfilled' ? results[index].value : undefined,
      error: results[index].status === 'rejected' ? results[index].reason : undefined
    }));

    // Commit all messages at once
    try {
      await Promise.all(processedResults.map(({ message }) => this.deps.committer.commit(message)));
    } catch (commitError) {
      logger.error('Failed to commit batch of messages', {
        batchSize: messages.length,
        error: commitError
      });
      throw commitError;
    }

    // Send responses in parallel after successful commits
    await Promise.allSettled(
      processedResults.map(async ({ message, input, result, error }) => {
        try {
          if (error) {
            await this.handleError(error, input, message);
          } else {
            await this.handleResult(result, input);
          }
        } catch (responseError) {
          logger.error('Failed to send response for message', {
            transferId: input.transferId,
            error: responseError
          });
        }
      })
    );
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