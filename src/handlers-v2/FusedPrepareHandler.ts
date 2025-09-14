import { IPositionProducer, INotificationProducer, IMessageCommitter, ProcessResult } from '../messaging/types';
import CentralServicesShared, { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../shared/logger';
import * as Metrics from '@mojaloop/central-services-metrics';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import assert from 'assert';
import { CreateTransferDto } from './types';
import { ApplicationConfig } from '../shared/config';
import { PrepareResult, PrepareResultFailLiquidity, PrepareResultFailValidation, PrepareResultType } from '../domain/ledger-v2/types';
import { Ledger } from '../domain/ledger-v2/Ledger';


const { decodePayload } = Util.StreamingProtocol
const rethrow = Util.rethrow;
const { createFSPIOPError } = ErrorHandler.Factory;
const { FSPIOPErrorCodes } = ErrorHandler.Enums;

export interface FusedPrepareHandlerDependencies {
  positionProducer: IPositionProducer
  notificationProducer: INotificationProducer
  committer: IMessageCommitter
  config: ApplicationConfig
  ledger: Ledger
}

export interface FusedPrepareHandlerInput {
  message: any;
  payload: CreateTransferDto;
  headers: any;
  transferId: string;
  action: any;
  metric: string;
  functionality: CentralServicesShared.EventTypeEnum.TRANSFER;
  actionEnum: string;
}

/**
 * @description Combined business logic of the Prepare and Position handler
 */
export class FusedPrepareHandler {

  constructor(private deps: FusedPrepareHandlerDependencies) {

  }

  async handle(error: any, messages: any): Promise<void> {
    if (error) {
      rethrow.rethrowAndCountFspiopError(error, { operation: 'PrepareHandler.handle' });
    }

    assert(Array.isArray(messages))

    if (messages.length === 0) {
      logger.debug('FusedPrepareHandler.handle() - received empty batch, nothing to process');
      return;
    }

    logger.debug(`FusedPrepareHandler.handle() - processing batch of ${messages.length} messages`)

    // Extract message data for all messages
    const inputs = messages.map(message => ({
      message,
      input: this.extractMessageData(message)
    }));

    // Process all ledger operations in parallel
    const results = await Promise.allSettled(
      inputs.map(async ({ input }) => this.deps.ledger.prepare(input))
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

  private extractMessageData(message: any): FusedPrepareHandlerInput {
    assert(message)
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.headers)
    assert(message.value.metadata)
    assert(message.value.metadata.event)
    assert(message.value.metadata.event.action)
    const payloadEncoded = message.value.content.payload
    const payload = decodePayload(payloadEncoded, {}) as unknown as CreateTransferDto
    const headers = message.value.content.headers

    const transferId = payload.transferId

    const action = message.value.metadata.event.action
    // Note: we currently only support prepare messages
    assert.equal(action, 'prepare')

    return {
      message,
      payload,
      headers,
      transferId,
      action,
      metric: `handler_transfers_${action.toLowerCase()}`,
      functionality: Enum.Events.Event.Type.TRANSFER,
      actionEnum: this.getActionEnum(action)
    };
  }

  private getActionEnum(action: string): string {
    const actionUpper = action.toUpperCase();
    return Enum.Events.Event.Action[actionUpper] || actionUpper;
  }

  private async handleResult(result: PrepareResult, input: FusedPrepareHandlerInput): Promise<void> {
    switch (result.type) {
      case PrepareResultType.PASS: {
        // TODO(LD): better typing elsewhere
        assert(input.message)
        assert(input.message.value)
        assert(input.message.value.to)
        assert(input.message.value.from)
        assert(input.message.value.content)
        assert(input.message.value.content.payload)
        assert(input.message.value.metadata)

        await this.deps.notificationProducer.sendSuccess({
          transferId: input.transferId,
          action: input.action,
          to: input.message.value.to,
          from: input.message.value.from,
          payload: input.message.value.content.payload,
          headers: input.headers,
          metadata: input.message.value.metadata
        })
        break;
      }
      case PrepareResultType.DUPLICATE_FINAL: {
        await this.deps.notificationProducer.sendDuplicate({
          transferId: input.transferId,
          action: Enum.Events.Event.Action.PREPARE_DUPLICATE,
          to: input.message.value.from,
          from: this.deps.config.HUB_NAME,
          payload: result.finalisedTransfer,
          headers: input.headers,
          metadata: input.message.value.metadata
        });
        break;
      }
      case PrepareResultType.DUPLICATE_NON_FINAL: {
        // ignore this case - DFSPs are allowed to send multiple requests
        break;
      }
      case PrepareResultType.FAIL_VALIDATION: {
        const typedResult = result as PrepareResultFailValidation
        assert(typedResult.failureReasons)
        assert(input.message)
        assert(input.message.value)
        assert(input.message.value.from)
        assert(input.message.value.metadata)
        const fspiopError = createFSPIOPError(FSPIOPErrorCodes.VALIDATION_ERROR, typedResult.failureReasons.join(', '));

        await this.deps.notificationProducer.sendError({
          transferId: input.transferId,
          fspiopError: fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
          action: input.action,
          to: input.message.value.from,
          from: this.deps.config.HUB_NAME,
          headers: input.headers,
          metadata: input.message.value.metadata
        });
        break;
      }
      case PrepareResultType.FAIL_LIQUIDITY:
      case PrepareResultType.FAIL_OTHER: {
        const typedResult = result as PrepareResultFailLiquidity
        assert(typedResult.fspiopError)
        assert(input.message)
        assert(input.message.value)
        assert(input.message.value.from)
        assert(input.message.value.metadata)

        await this.deps.notificationProducer.sendError({
          transferId: input.transferId,
          fspiopError: typedResult.fspiopError.toApiErrorObject(this.deps.config.ERROR_HANDLING),
          action: input.action,
          to: input.message.value.from,
          from: this.deps.config.HUB_NAME,
          headers: input.headers,
          metadata: input.message.value.metadata
        });
        break;
      }
      default: {
        throw new Error(`handleResult() unhandled result.type: ${(result as any).type}`)
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