import { INotificationProducer } from '../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../shared/logger';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as EventSdk from '@mojaloop/event-sdk';
import { ApplicationConfig } from '../shared/config';
import { Ledger } from 'src/domain/ledger-v2/Ledger';

const { resourceVersions } = Util;

export interface TimeoutHandlerDependencies {
  notificationProducer: INotificationProducer;
  config: ApplicationConfig;
  ledger: Ledger;
  distLock?: any;
}

import { TimedOutTransfer } from '../domain/ledger-v2/types';

export class TimeoutHandler {
  private running = false;

  constructor(private deps: TimeoutHandlerDependencies) {}

  async processTimeouts(): Promise<any> {
    let isAcquired = false;
    try {
      isAcquired = await this.acquireLock();
      if (!isAcquired) return;

      // Call ledger to sweep timed out transfers (handles position reversals internally)
      const sweepResult = await this.deps.ledger.sweepTimedOut();

      if (sweepResult.type === 'FAILURE') {
        throw sweepResult.error;
      }

      const { transfers } = sweepResult;

      if (transfers && transfers.length > 0) {
        await this.processTimedOutTransfers(transfers);
      }

      return {
        transfers
      };
    } catch (err) {
      logger.error('error in processTimeouts:', err);
      throw ErrorHandler.Factory.reformatFSPIOPError(err);
    } finally {
      if (isAcquired) await this.releaseLock();
    }
  }

  private async processTimedOutTransfers(transfers: TimedOutTransfer[]): Promise<void> {
    logger.verbose(`processing ${transfers.length} timed out transfers...`);

    for (const transfer of transfers) {
      const span = EventSdk.Tracer.createSpan('cl_transfer_timeout');
      try {
        const fspiopError = this.createFSPIOPTimeoutError();
        const state = Util.StreamingProtocol.createEventState(
          'failure',
          fspiopError.errorInformation.errorCode,
          fspiopError.errorInformation.errorDescription
        );

        const destination = transfer.payerId;
        const source = transfer.payeeId;
        const headers = this.createTimeoutHeaders(destination);

        await span.audit({
          state,
          headers,
          transferId: transfer.id
        }, EventSdk.AuditEventAction.start);

        // Send timeout error notifications to both participants
        const timeoutError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED);
        const timeoutErrorObject = timeoutError.toApiErrorObject(this.deps.config.ERROR_HANDLING);
        const timeoutMetadata = this.createTimeoutMetadata(
          transfer.id,
          Enum.Events.Event.Action.TIMEOUT_RESERVED,
          Util.StreamingProtocol.createEventState('failure', timeoutErrorObject.errorInformation.errorCode, timeoutErrorObject.errorInformation.errorDescription)
        );

        await this.sendTimeoutErrorNotificationsToBothParticipants(
          transfer.id,
          transfer.payerId,
          transfer.payeeId,
          timeoutErrorObject,
          Enum.Events.Event.Action.TIMEOUT_RESERVED,
          headers,
          timeoutMetadata
        );
      } catch (err) {
        logger.error('error in processTimedOutTransfers:', err);
        const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(err);
        const state = new EventSdk.EventStateMetadata(EventSdk.EventStatusType.failed, fspiopError.apiErrorCode.code, fspiopError.apiErrorCode.message);
        await span.error(fspiopError, state);
        await span.finish(fspiopError.message, state);
        throw fspiopError;
      } finally {
        if (!span.isFinished) {
          await span.finish();
        }
      }
    }
  }

  private createTimeoutHeaders(destination: string): Record<string, any> {
    return Util.Http.SwitchDefaultHeaders(
      destination,
      Enum.Http.HeaderResources.TRANSFERS,
      this.deps.config.HUB_NAME,
      resourceVersions[Enum.Http.HeaderResources.TRANSFERS].contentVersion
    );
  }

  private createTimeoutMetadata(transferId: string, action: string, state: any): any {
    return Util.StreamingProtocol.createMetadataWithCorrelatedEvent(
      transferId,
      Enum.Kafka.Topics.NOTIFICATION,
      action,
      state
    );
  }

  private createFSPIOPTimeoutError(): any {
    return ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED)
      .toApiErrorObject(this.deps.config.ERROR_HANDLING);
  }

  private async acquireLock(): Promise<boolean> {
    if (this.deps.distLock) {
      try {
        const distLockKey = this.deps.config.HANDLERS_TIMEOUT.DIST_LOCK.distLockKey || 'mutex:cl-timeout-handler';
        const distLockTtl = this.deps.config.HANDLERS_TIMEOUT?.DIST_LOCK?.lockTimeout || 10000;
        const distLockAcquireTimeout = this.deps.config.HANDLERS_TIMEOUT?.DIST_LOCK?.acquireTimeout || 5000;
        
        return !!(await this.deps.distLock.acquire(distLockKey, distLockTtl, distLockAcquireTimeout));
      } catch (err) {
        logger.error('Error acquiring distributed lock:', err);
        return false;
      }
    }
    logger.debug('Distributed lock not configured or disabled, running without distributed lock');
    return this.running ? false : (this.running = true);
  }

  private async releaseLock(): Promise<void> {
    if (this.deps.distLock) {
      try {
        await this.deps.distLock.release();
        logger.verbose('Distributed lock released');
      } catch (error) {
        logger.error('Error releasing distributed lock:', error);
      }
    }
    this.running = false;
  }

  /**
   * Send timeout error notifications to both payer and payee FSPs
   * This replicates the behavior from PositionHandler.sendTimeoutErrorNotifications
   */
  private async sendTimeoutErrorNotificationsToBothParticipants(
    transferId: string,
    payerFsp: string,
    payeeFsp: string,
    timeoutErrorObject: ErrorHandler.FSPIOPApiErrorObject,
    action: string,
    headers: any,
    metadata: any
  ): Promise<void> {
    try {
      // Send timeout error to payer FSP
      await this.deps.notificationProducer.sendError({
        transferId,
        fspiopError: timeoutErrorObject,
        action,
        to: payerFsp,
        from: this.deps.config.HUB_NAME,
        headers,
        metadata,
        payload: JSON.stringify(timeoutErrorObject)
      });

      // Send timeout error to payee FSP
      await this.deps.notificationProducer.sendError({
        transferId,
        fspiopError: timeoutErrorObject,
        action,
        to: payeeFsp,
        from: this.deps.config.HUB_NAME,
        headers,
        metadata,
        payload: JSON.stringify(timeoutErrorObject)
      });

      logger.debug(`Timeout error notifications sent to both participants for transfer: ${transferId}`, {
        payerFsp,
        payeeFsp,
        action
      });

    } catch (err) {
      logger.error(`Failed to send timeout error notifications for transfer: ${transferId}`, err);
      throw ErrorHandler.Factory.reformatFSPIOPError(err);
    }
  }
}