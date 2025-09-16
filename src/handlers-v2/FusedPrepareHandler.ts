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
    const startTime = process.hrtime.bigint();
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
    const extractStart = process.hrtime.bigint();
    const inputs = messages.map(message => ({
      message,
      input: this.extractMessageData(message)
    }));
    const extractEnd = process.hrtime.bigint();

    // Process all ledger operations in parallel
    const ledgerStart = process.hrtime.bigint();
    const results = await Promise.allSettled(
      inputs.map(async ({ input }) => this.deps.ledger.prepare(input))
    );
    const ledgerEnd = process.hrtime.bigint();

    // Combine inputs with their results
    const processedResults = inputs.map(({ message, input }, index) => ({
      message,
      input,
      result: results[index].status === 'fulfilled' ? results[index].value : undefined,
      error: results[index].status === 'rejected' ? results[index].reason : undefined
    }));

    // Auto-commit is enabled - no manual commits needed
    const commitStart = process.hrtime.bigint();
    const commitEnd = commitStart; // No actual commit time since auto-commit is enabled

    // Send responses in parallel after successful commits
    const responseStart = process.hrtime.bigint();
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
    const responseEnd = process.hrtime.bigint();

    // Sample performance metrics (log every 10th batch)
    if (Math.random() < 0.1) {
      const totalTime = Number(responseEnd - startTime) / 1_000_000;
      const extractTime = Number(extractEnd - extractStart) / 1_000_000;
      const ledgerTime = Number(ledgerEnd - ledgerStart) / 1_000_000;
      const commitTime = Number(commitEnd - commitStart) / 1_000_000;
      const responseTime = Number(responseEnd - responseStart) / 1_000_000;

      logger.info('FusedPrepareHandler performance sample', {
        batchSize: messages.length,
        totalTime_ms: totalTime.toFixed(2),
        extractTime_ms: extractTime.toFixed(2),
        ledgerTime_ms: ledgerTime.toFixed(2),
        commitTime_ms: commitTime.toFixed(2),
        responseTime_ms: responseTime.toFixed(2),
        avgPerMessage_ms: (totalTime / messages.length).toFixed(2)
      });
    }
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