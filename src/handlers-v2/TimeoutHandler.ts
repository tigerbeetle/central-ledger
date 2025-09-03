import { INotificationProducer, IPositionProducer } from '../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../shared/logger';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as EventSdk from '@mojaloop/event-sdk';

const { resourceVersions } = Util;

export interface TimeoutHandlerDependencies {
  notificationProducer: INotificationProducer;
  positionProducer: IPositionProducer;
  config: any;
  timeoutService: any;
  distLock?: any;
}

export interface TimedOutTransfer {
  transferId: string;
  payerFsp: string;
  payeeFsp: string;
  externalPayerName?: string;
  externalPayeeName?: string;
  bulkTransferId: string | null;
  transferStateId: string;
  effectedParticipantCurrencyId?: number;
  payerParticipantCurrencyId?: number;
}

export interface TimedOutFxTransfer {
  commitRequestId: string;
  initiatingFsp: string;
  counterPartyFsp: string;
  externalInitiatingFspName?: string;
  externalCounterPartyFspName?: string;
  transferStateId: string;
  effectedParticipantCurrencyId?: number;
}

export class TimeoutHandler {
  private running = false;

  constructor(private deps: TimeoutHandlerDependencies) {}

  async processTimeouts(): Promise<any> {
    let isAcquired = false;
    try {
      isAcquired = await this.acquireLock();
      if (!isAcquired) return;

      const timeoutSegment = await this.deps.timeoutService.getTimeoutSegment();
      const intervalMin = timeoutSegment ? timeoutSegment.value : 0;
      const segmentId = timeoutSegment ? timeoutSegment.segmentId : 0;
      const cleanup = await this.deps.timeoutService.cleanupTransferTimeout();
      const latestTransferStateChange = await this.deps.timeoutService.getLatestTransferStateChange();

      const fxTimeoutSegment = await this.deps.timeoutService.getFxTimeoutSegment();
      const intervalMax = (latestTransferStateChange && parseInt(latestTransferStateChange.transferStateChangeId)) || 0;
      const fxIntervalMin = fxTimeoutSegment ? fxTimeoutSegment.value : 0;
      const fxSegmentId = fxTimeoutSegment ? fxTimeoutSegment.segmentId : 0;
      const fxCleanup = await this.deps.timeoutService.cleanupFxTransferTimeout();
      const latestFxTransferStateChange = await this.deps.timeoutService.getLatestFxTransferStateChange();
      const fxIntervalMax = (latestFxTransferStateChange && parseInt(latestFxTransferStateChange.fxTransferStateChangeId)) || 0;

      const { transferTimeoutList, fxTransferTimeoutList } = await this.deps.timeoutService.timeoutExpireReserved(
        segmentId, intervalMin, intervalMax, fxSegmentId, fxIntervalMin, fxIntervalMax
      );

      if (transferTimeoutList) {
        await this.processTimedOutTransfers(transferTimeoutList);
      }
      if (fxTransferTimeoutList) {
        await this.processFxTimedOutTransfers(fxTransferTimeoutList);
      }

      return {
        intervalMin,
        cleanup,
        intervalMax,
        fxIntervalMin,
        fxCleanup,
        fxIntervalMax,
        transferTimeoutList,
        fxTransferTimeoutList
      };
    } catch (err) {
      logger.error('error in processTimeouts:', err);
      throw ErrorHandler.Factory.reformatFSPIOPError(err);
    } finally {
      if (isAcquired) await this.releaseLock();
    }
  }

  private async processTimedOutTransfers(transferTimeoutList: TimedOutTransfer[]): Promise<void> {
    const fspiopError = this.createFSPIOPTimeoutError();
    if (!Array.isArray(transferTimeoutList)) {
      transferTimeoutList = [transferTimeoutList as TimedOutTransfer];
    }
    logger.verbose(`processing ${transferTimeoutList.length} timed out transfers...`);

    for (const TT of transferTimeoutList) {
      const span = EventSdk.Tracer.createSpan('cl_transfer_timeout');
      try {
        const state = Util.StreamingProtocol.createEventState(
          'failure',
          fspiopError.errorInformation.errorCode,
          fspiopError.errorInformation.errorDescription
        );

        const destination = TT.externalPayerName || TT.payerFsp;
        const source = TT.externalPayeeName || TT.payeeFsp;
        const headers = this.createTimeoutHeaders(destination);

        await span.audit({
          state,
          headers,
          transferId: TT.transferId
        }, EventSdk.AuditEventAction.start);

        // Create the notification message using the same structure as legacy handler
        const metadata = this.createTimeoutMetadata(TT.transferId, Enum.Events.Event.Action.TIMEOUT_RECEIVED, state);
        const message = Util.StreamingProtocol.createMessage(
          TT.transferId,
          destination,
          source,
          metadata,
          headers,
          fspiopError,
          { id: TT.transferId },
          `application/vnd.interoperability.${Enum.Http.HeaderResources.TRANSFERS}+json;version=${resourceVersions[Enum.Http.HeaderResources.TRANSFERS].contentVersion}`
        );

        // Add context for notification functionality (same as legacy)
        message.content.context = {
          payer: TT.externalPayerName || TT.payerFsp,
          payee: TT.externalPayeeName || TT.payeeFsp
        };

        if (TT.bulkTransferId === null) { // regular transfer
          // Transfer expired before funds were reserved
          if (TT.transferStateId === Enum.Transfers.TransferInternalState.EXPIRED_PREPARED) {
            message.from = this.deps.config.HUB_NAME;
            await this.deps.notificationProducer.sendError({
              transferId: TT.transferId,
              fspiopError,
              action: Enum.Events.Event.Action.TIMEOUT_RECEIVED,
              to: destination,
              from: this.deps.config.HUB_NAME,
              headers,
              metadata,
              payload: message.content.payload
            });
          } else if (TT.transferStateId === Enum.Transfers.TransferInternalState.RESERVED_TIMEOUT) {
            // Transfer expired after funds were reserved - need to rollback

            // Create position message for reserved timeouts
            message.from = this.deps.config.HUB_NAME;
            message.metadata.event.type = Enum.Events.Event.Type.POSITION;
            message.metadata.event.action = Enum.Events.Event.Action.TIMEOUT_RESERVED;
            const error = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED);

            await this.deps.positionProducer.sendAbort({
              transferId: TT.transferId,
              participantCurrencyId: TT.effectedParticipantCurrencyId?.toString() || '',
              amount: '0',
              currency: '',
              action: 'ABORT',
              from: this.deps.config.HUB_NAME,
              to: destination,
              headers,
              payload: error,
              metadata: message.metadata
            });
          }
        } else { // individual transfer from a bulk
          if (TT.transferStateId === Enum.Transfers.TransferInternalState.EXPIRED_PREPARED) {
            // Handle bulk timeout - would need bulk producer
            logger.info(`Bulk timeout for transfer ${TT.transferId} - bulk handling not yet implemented`);
          } else if (TT.transferStateId === Enum.Transfers.TransferInternalState.RESERVED_TIMEOUT) {
            // Handle bulk reserved timeout
            message.from = this.deps.config.HUB_NAME;
            message.metadata.event.type = Enum.Events.Event.Type.POSITION;
            message.metadata.event.action = Enum.Events.Event.Action.BULK_TIMEOUT_RESERVED;

            const error = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED);
            await this.deps.positionProducer.sendAbort({
              transferId: TT.transferId,
              participantCurrencyId: TT.payerParticipantCurrencyId?.toString() || '',
              amount: '0',
              currency: '',
              action: 'TIMEOUT_RESERVED',
              from: this.deps.config.HUB_NAME,
              to: destination,
              headers,
              payload: error,
              metadata: message.metadata
            });
          }
        }
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

  private async processFxTimedOutTransfers(fxTransferTimeoutList: TimedOutFxTransfer[]): Promise<void> {
    const fspiopError = this.createFSPIOPTimeoutError();
    if (!Array.isArray(fxTransferTimeoutList)) {
      fxTransferTimeoutList = [fxTransferTimeoutList as TimedOutFxTransfer];
    }
    logger.verbose(`processing ${fxTransferTimeoutList.length} timed out fxTransfers...`);

    for (const fTT of fxTransferTimeoutList) {
      const span = EventSdk.Tracer.createSpan('cl_fx_transfer_timeout');
      try {
        const state = Util.StreamingProtocol.createEventState(
          'failure',
          fspiopError.errorInformation.errorCode,
          fspiopError.errorInformation.errorDescription
        );

        const destination = fTT.externalInitiatingFspName || fTT.initiatingFsp;
        const source = fTT.externalCounterPartyFspName || fTT.counterPartyFsp;
        const headers = this.createFxTimeoutHeaders(destination);

        await span.audit({
          state,
          headers,
          transferId: fTT.commitRequestId
        }, EventSdk.AuditEventAction.start);

        // Create the FX notification message using the same structure as legacy handler
        const fxMetadata = this.createTimeoutMetadata(fTT.commitRequestId, Enum.Events.Event.Action.FX_TIMEOUT_RECEIVED, state);
        const fxMessage = Util.StreamingProtocol.createMessage(
          fTT.commitRequestId,
          destination,
          source,
          fxMetadata,
          headers,
          fspiopError,
          { id: fTT.commitRequestId },
          `application/vnd.interoperability.${Enum.Http.HeaderResources.FX_TRANSFERS}+json;version=${resourceVersions[Enum.Http.HeaderResources.FX_TRANSFERS].contentVersion}`
        );

        // Add context for FX notification functionality (same as legacy)
        fxMessage.content.context = {
          payer: fTT.externalInitiatingFspName || fTT.initiatingFsp,
          payee: fTT.externalCounterPartyFspName || fTT.counterPartyFsp
        };

        if (fTT.transferStateId === Enum.Transfers.TransferInternalState.EXPIRED_PREPARED) {
          fxMessage.from = this.deps.config.HUB_NAME;
          await this.deps.notificationProducer.sendError({
            transferId: fTT.commitRequestId,
            fspiopError,
            action: Enum.Events.Event.Action.FX_TIMEOUT_RECEIVED,
            to: destination,
            from: this.deps.config.HUB_NAME,
            headers,
            metadata: fxMetadata,
            payload: fxMessage.content.payload
          });
        } else if (fTT.transferStateId === Enum.Transfers.TransferInternalState.RESERVED_TIMEOUT) {
          // Create FX position message for reserved timeouts
          fxMessage.from = this.deps.config.HUB_NAME;
          fxMessage.metadata.event.type = Enum.Events.Event.Type.POSITION;
          fxMessage.metadata.event.action = Enum.Events.Event.Action.FX_TIMEOUT_RESERVED;

          await this.deps.positionProducer.sendAbort({
            transferId: fTT.commitRequestId,
            participantCurrencyId: fTT.effectedParticipantCurrencyId?.toString() || '',
            amount: '0',
            currency: '',
            action: 'ABORT',
            from: this.deps.config.HUB_NAME,
            to: destination,
            headers,
            payload: '',
            metadata: fxMessage.metadata
          });
        }
      } catch (err) {
        logger.error('error in processFxTimedOutTransfers:', err);
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

  private createFxTimeoutHeaders(destination: string): Record<string, any> {
    return Util.Http.SwitchDefaultHeaders(
      destination,
      Enum.Http.HeaderResources.FX_TRANSFERS,
      this.deps.config.HUB_NAME,
      resourceVersions[Enum.Http.HeaderResources.FX_TRANSFERS].contentVersion
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
        const distLockKey = this.deps.config.TIMEOUT_HANDLER_DIST_LOCK_KEY;
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
}