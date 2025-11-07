import { INotificationProducer } from '../messaging/types';
import { Enum, Util } from '@mojaloop/central-services-shared';
import { logger } from '../shared/logger';
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import * as EventSdk from '@mojaloop/event-sdk';
import { ApplicationConfig } from '../shared/config';
import { Ledger } from 'src/domain/ledger-v2/Ledger';

const { resourceVersions } = Util;

export interface TimeoutSegment {
  value: number;
  segmentId: number;
}

export interface LatestTransferStateChange {
  transferStateChangeId: string;
}

export interface LatestFxTransferStateChange {
  fxTransferStateChangeId: string;
}

export interface TimeoutExpireReservedResult {
  transferTimeoutList: TimedOutTransfer[] | null;
  fxTransferTimeoutList: TimedOutFxTransfer[] | null;
}

export interface ITimeoutService {
  getTimeoutSegment: () => Promise<TimeoutSegment | null>;
  cleanupTransferTimeout: () => Promise<any>;
  getLatestTransferStateChange: () => Promise<LatestTransferStateChange | null>;
  getFxTimeoutSegment: () => Promise<TimeoutSegment | null>;
  cleanupFxTransferTimeout: () => Promise<any>;
  getLatestFxTransferStateChange: () => Promise<LatestFxTransferStateChange | null>;
  timeoutExpireReserved: (
    segmentId: number,
    intervalMin: number,
    intervalMax: number,
    fxSegmentId: number,
    fxIntervalMin: number,
    fxIntervalMax: number
  ) => Promise<TimeoutExpireReservedResult>;
}

export interface TimeoutHandlerDependencies {
  notificationProducer: INotificationProducer;
  config: ApplicationConfig;
  timeoutService: ITimeoutService;
  ledger: Ledger;
  constants: {

  },
  distLock?: any;
  transferService: any;
  participantFacade: any;
  positionService: any;
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
            // Reverse the position
            await this.handleReservedTimeoutPositionReversal(TT);

            // Send error notifications
            const timeoutError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED);
            const timeoutErrorObject = timeoutError.toApiErrorObject(this.deps.config.ERROR_HANDLING);
            const timeoutMetadata = this.createTimeoutMetadata(TT.transferId, Enum.Events.Event.Action.TIMEOUT_RESERVED,
              Util.StreamingProtocol.createEventState('failure', timeoutErrorObject.errorInformation.errorCode, timeoutErrorObject.errorInformation.errorDescription)
            );

            await this.sendTimeoutErrorNotificationsToBothParticipants(
              TT.transferId,
              TT.payerFsp,
              TT.payeeFsp,
              timeoutErrorObject,
              Enum.Events.Event.Action.TIMEOUT_RESERVED,
              headers,
              timeoutMetadata
            );
          }
        } else { // individual transfer from a bulk
          if (TT.transferStateId === Enum.Transfers.TransferInternalState.EXPIRED_PREPARED) {
            // Handle bulk timeout - would need bulk producer
            logger.info(`Bulk timeout for transfer ${TT.transferId} - bulk handling not yet implemented`);
          } else if (TT.transferStateId === Enum.Transfers.TransferInternalState.RESERVED_TIMEOUT) {
            // Reverse the position
            await this.handleReservedTimeoutPositionReversal(TT);
            const timeoutError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED);
            const timeoutErrorObject = timeoutError.toApiErrorObject(this.deps.config.ERROR_HANDLING);
            const timeoutMetadata = this.createTimeoutMetadata(TT.transferId, Enum.Events.Event.Action.BULK_TIMEOUT_RESERVED,
              Util.StreamingProtocol.createEventState('failure', timeoutErrorObject.errorInformation.errorCode, timeoutErrorObject.errorInformation.errorDescription)
            );

            // Send error notifications
            await this.sendTimeoutErrorNotificationsToBothParticipants(
              TT.transferId,
              TT.payerFsp,
              TT.payeeFsp,
              timeoutErrorObject,
              Enum.Events.Event.Action.BULK_TIMEOUT_RESERVED,
              headers,
              timeoutMetadata
            );
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

          // Reverse the position
          await this.handleFxReservedTimeoutPositionReversal(fTT);

          // Send FX error notifications to participants
          const timeoutError = ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED);
          const timeoutErrorObject = timeoutError.toApiErrorObject(this.deps.config.ERROR_HANDLING);
          const timeoutMetadata = this.createTimeoutMetadata(fTT.commitRequestId, Enum.Events.Event.Action.FX_TIMEOUT_RESERVED,
            Util.StreamingProtocol.createEventState('failure', timeoutErrorObject.errorInformation.errorCode, timeoutErrorObject.errorInformation.errorDescription)
          );

          await this.sendTimeoutErrorNotificationsToBothParticipants(
            fTT.commitRequestId,
            fTT.initiatingFsp,
            fTT.counterPartyFsp,
            timeoutErrorObject,
            Enum.Events.Event.Action.FX_TIMEOUT_RESERVED,
            headers,
            timeoutMetadata
          );
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
   * FUSED: Handle position reversal for reserved timeout transfers directly
   * This replaces the need to send a message to PositionHandler via positionProducer
   */
  private async handleReservedTimeoutPositionReversal(TT: TimedOutTransfer): Promise<void> {
    try {
      // 1. Get transfer participants
      const transfer = await this.deps.transferService.getById(TT.transferId);
      if (!transfer) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR, 'Transfer not found');
      }

      // 2. Get transfer info for PAYER (who had funds reserved)
      const transferInfo = await this.deps.transferService.getTransferInfoToChangePosition(
        TT.transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYER_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      if (!transferInfo) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR, 'Transfer info not found');
      }

      // 3. Get participant currency info
      const participantCurrency = await this.deps.participantFacade.getByIDAndCurrency(
        transferInfo.participantId,
        transferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      // 4. Reverse the position (add back reserved funds)
      const isReversal = true;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferInternalState.EXPIRED_RESERVED,
        reason: ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED).message
      };

      await this.deps.positionService.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      logger.debug(`Successfully reversed position for timed out transfer: ${TT.transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      });

    } catch (err) {
      logger.error(`Failed to reverse position for timed out transfer: ${TT.transferId}`, err);
      throw ErrorHandler.Factory.reformatFSPIOPError(err);
    }
  }

  /**
   * FUSED: Handle position reversal for FX reserved timeout transfers directly
   */
  private async handleFxReservedTimeoutPositionReversal(fTT: TimedOutFxTransfer): Promise<void> {
    try {
      // 1. Get FX transfer participants
      const fxTransfer = await this.deps.transferService.getById(fTT.commitRequestId);
      if (!fxTransfer) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR, 'FX Transfer not found');
      }

      // 2. Get FX transfer info for INITIATING FSP (who had funds reserved)
      const fxTransferInfo = await this.deps.transferService.getTransferInfoToChangePosition(
        fTT.commitRequestId,
        Enum.Accounts.TransferParticipantRoleType.PAYER_DFSP, // Initiating FSP acts as payer in FX
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      if (!fxTransferInfo) {
        throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR, 'FX Transfer info not found');
      }

      // 3. Get participant currency info
      const participantCurrency = await this.deps.participantFacade.getByIDAndCurrency(
        fxTransferInfo.participantId,
        fxTransferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      // 4. Reverse the FX position (add back reserved funds)
      const isReversal = true;
      const transferStateChange = {
        transferId: fxTransferInfo.transferId,
        transferStateId: Enum.Transfers.TransferInternalState.EXPIRED_RESERVED,
        reason: ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED).message
      };

      await this.deps.positionService.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        fxTransferInfo.amount,
        transferStateChange
      );

      logger.debug(`Successfully reversed FX position for timed out transfer: ${fTT.commitRequestId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: fxTransferInfo.amount
      });

    } catch (err) {
      logger.error(`Failed to reverse FX position for timed out transfer: ${fTT.commitRequestId}`, err);
      throw ErrorHandler.Factory.reformatFSPIOPError(err);
    }
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