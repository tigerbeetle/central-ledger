import * as ErrorHandler from '@mojaloop/central-services-error-handling';
const { Enum, Util: { Time } } = require('@mojaloop/central-services-shared');
import assert from "assert";
import { Knex } from "knex";
import { AdminHandler } from '../../handlers-v2/AdminHandler';
import { FusedFulfilHandlerInput } from '../../handlers-v2/FusedFulfilHandler';
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { MessageContext, PositionKafkaMessage, PreparedMessage, PreparePositionsBatchResult } from "src/handlers-v2/PositionHandler";
import { DuplicationCheckResult, Location, TransferCheckResult, ValidationResult } from "src/handlers-v2/PrepareHandler";
import { CommitTransferDto, CreateTransferDto } from "src/handlers-v2/types";
import { ProxyObligation } from "src/handlers/transfers/prepare";
import { ApplicationConfig } from "../../shared/config";
import { logger } from '../../shared/logger';
import {
  AnyQuery,
  CommandResult,
  CreateDfspCommand,
  CreateDfspResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCommand,
  DepositResponse,
  DfspAccountResponse,
  FulfilResult,
  FulfilResultType,
  GetAllDfspsResponse,
  GetAllDfspAccountsQuery,
  GetDfspAccountsQuery,
  GetHubAccountsQuery,
  GetNetDebitCapQuery,
  HubAccountResponse,
  LegacyLedgerDfsp,
  LegacyLedgerAccount,
  LegacyLimit,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  LookupTransferResultType,
  PrepareResult,
  PrepareResultType,
  SetNetDebitCapCommand,
  SweepResult,
  TimedOutTransfer,
  WithdrawCommitCommand,
  WithdrawCommitResponse,
  WithdrawPrepareCommand,
  WithdrawPrepareResponse,
  WithdrawAbortCommand,
  WithdrawAbortResponse,
  SettlementPrepareCommand,
  SettlementCloseWindowCommand,
  SettlementAbortCommand,
  SettlementCommitCommand,
  GetSettlementQuery,
  GetSettlementQueryResponse,
  SettlementUpdateCommand,
} from './types';
import { Ledger } from './Ledger';
import { safeStringToNumber } from '../../shared/config/util';
import Helper from './LegacyLedgerHelper';
import { QueryResult } from 'src/shared/results';


/**
 * Legacy Types 
 */
export interface ParticipantWithCurrency {
  // From participant table
  participantId: number;
  name: string;
  description: string | null;
  isActive: boolean;
  createdDate: string;
  createdBy: string;

  // From participantCurrency table (added when currency is found)
  participantCurrencyId: number;
  currencyId: string;
  currencyIsActive: boolean;
}

export interface ParticipantServiceParticipant {
  participantId: number,
  name: string,
  isActive: number,
  createdDate: string,
  createdBy: string,
  isProxy: number,
  currencyList: Array<ParticipantServiceCurrency>
}

export interface ParticipantServiceCurrency {
  participantCurrencyId: number,
  participantId: number,
  currencyId: string,
  ledgerAccountTypeId: number,
  isActive: number,
  createdDate: string,
  createdBy: string,
}

export interface ParticipantServiceAccount {
  id: number,
  ledgerAccountType: string,
  currency: string,
  isActive: number,
  value: string,
  reservedValue: string,
  changedDate: string
}

export interface TransferStateChange {
  transferId: string
  transferStateId: string | number
  reason?: string
  createdDate?: string
}

export interface TransferStateChangeRow {
  transferStateChangeId: number
  transferId: string
  transferStateId: string
  reason?: string
  createdDate: Date
  enumeration: string
}

export interface Extension {
  key: string;
  value: string;
  isError?: boolean;
  isFulfilment?: boolean;
}

export interface ExtensionList {
  extension: Extension[];
}

export interface Amount {
  amount: string;
  currency: string;
}

export interface FulfilmentPayload {
  fulfilment?: string;
  completedTimestamp?: string;
  extensionList?: ExtensionList;
}

export interface ErrorPayload {
  errorInformation: {
    errorCode: string;
    errorDescription: string;
    extensionList?: ExtensionList;
  };
}

export type PayeeResponsePayload = FulfilmentPayload | ErrorPayload;

export interface TransformedTransfer {
  transferId: string;
  transferState: string;
  completedTimestamp: string;
  fulfilment?: string;
  extensionList?: Extension[];
}

export interface TransferReadModel {
  transferId: string;
  amount: string;
  currency: string;
  payerParticipantCurrencyId?: number;
  payerAmount: string;
  payerParticipantId: number;
  payerFsp: string;
  payerIsProxy: boolean;
  payeeParticipantCurrencyId?: number;
  payeeAmount: string;
  payeeParticipantId: number;
  payeeFsp: string;
  payeeIsProxy: boolean;
  transferStateChangeId: number;
  transferState: string;
  reason?: string;
  completedTimestamp: string;
  transferStateEnumeration: string;
  transferStateDescription: string;
  ilpPacket: string;
  condition: string;
  fulfilment?: string;
  errorCode?: string;
  errorDescription?: string;
  externalPayerName?: string;
  externalPayeeName?: string;
  extensionList?: Extension[];
  isTransferReadModel: true;
}

export interface TransferParticipantInfo {
  transferId: string;
  participantId: number;
  participantCurrencyId?: number;
  transferParticipantRoleTypeId: number;
  ledgerEntryTypeId: number;
  amount: string;
  externalParticipantId?: number;
  currencyId: string;
  transferStateId: string;
  reason?: string;
}

enum PrepareDuplicateResult {
  /**
   * Transfer id is unique
   */
  UNIQUE = 'UNIQUE',

  /**
   * Transfer Id is the same, body is different
   */
  MODIFIED = 'MODIFIED',

  /**
   * Transfer Id is the same, body is the same
   */
  DUPLICATED = 'DUPLICATED'
}

enum FulfilDuplicateResult {
  /**
   * Message is unique
   */
  UNIQUE = 'UNIQUE',

  /**
   * Transfer Id is the same, body is different
   */
  MODIFIED = 'MODIFIED',

  /**
   * Transfer Id is the same, body is the same
   */
  DUPLICATED = 'DUPLICATED'
}

export interface LegacyLedgerDependencies {
  config: ApplicationConfig
  knex: Knex

  /**
   * Legacy functions used for Onboarding the Hub, DFSP, Setting up the switch and Settlement
   * models.
   */
  lifecycle: {
    participantService: {
      create: (payload: { name: string, isProxy?: boolean }) => Promise<number>
      createParticipantCurrency: (participantId: number, currency: string, ledgerAccountTypeId: number, isActive?: boolean) => Promise<number>
      createHubAccount: (participantId: number, currency: string, ledgerAccountTypeId: number) => Promise<{ participantCurrency: any }>
      getAccounts: (name: string, query: { currency?: string }) => Promise<Array<ParticipantServiceAccount>>
      getAll: () => Promise<Array<ParticipantServiceParticipant>>,
      getByName: (name: string) => Promise<{ currencyList: ParticipantServiceCurrency[], participantId: number, name: string, isActive: number, createdDate: string }>
      getById: (id: number) => Promise<{ currencyList: ParticipantServiceCurrency[], participantId: number, name: string, isActive: number, createdDate: string }>
      getLimits: (name: string, query: { currency: string, type: string }) => Promise<Array<unknown>>
      getParticipantCurrencyById: (participantCurrencyId: number) => Promise<any>
      update: (name: string, payload: { isActive: boolean }) => Promise<unknown>
      updateAccount: (payload: { isActive: boolean }, params: { name: string, id: number }, enums: any) => Promise<void>
      validateHubAccounts: (currency: string) => Promise<void>
      recordFundsInOut: (
        payload: {
          action: string
          reason: string
          externalReference: string
          amount: {
            amount: string
            currency: string
          }
        },
        params: {
          name: string
          id: number | null
          transferId: string
        },
        enums: any
      ) => Promise<{
        accountMatched: {
          participantCurrencyId: number
          ledgerAccountTypeId: number
          accountIsActive: boolean
        }
        payload: any
      }>
      adjustLimits: (name: string, payload: {
        currency: string
        limit: {
          type: string
          value: number
          thresholdAlarmPercentage: number
        }
      }) => Promise<number>
    },
    settlementModelDomain: {
      createSettlementModel: (model: { name: string, settlementGranularity: string, settlementInterchange: string, settlementDelay: string, currency: string, requireLiquidityCheck: boolean, ledgerAccountType: string, settlementAccountType: string, autoPositionReset: boolean }) => Promise<void>
      getAll: () => Promise<Array<{ currencyId: string | null, ledgerAccountTypeId: number, settlementAccountTypeId: number }>>
    },
    participantFacade: {
      addLimitAndInitialPosition: (positionParticipantCurrencyId: number, settlementParticipantCurrencyId: number, payload: any, processLimitsOnly: boolean) => Promise<void>
      getByNameAndCurrency: (name: string, currency: string, accountType: any) => Promise<{ participantCurrencyId: number }>
    }
    transferService: {
      recordFundsIn: (payload: any, transactionTimestamp: string, enums: any) => Promise<void>
      saveTransferDuplicateCheck: (transferId: string, payload: any) => Promise<void>
    }
    transferFacade: {
      reconciliationTransferPrepare: (payload: any, transactionTimestamp: string, enums: any, trx?: any) => Promise<number>
      reconciliationTransferReserve: (payload: any, transactionTimestamp: string, enums: any, trx?: any) => Promise<number>
      reconciliationTransferCommit: (payload: any, transactionTimestamp: string, enums: any, trx?: any) => Promise<any>
      getTransferStateByTransferId: (transferId: string) => Promise<TransferStateChangeRow>
      getById: (transferId: string) => Promise<any>
    }
    adminHandler: AdminHandler
    enums: any
  },

  /**
   * Legacy functions used for clearing payments.
   */
  clearing: {
    validatePrepare: (
      payload: CreateTransferDto,
      headers: any,
      isFx: boolean,
      determiningTransferCheckResult: TransferCheckResult,
      proxyObligation: ProxyObligation
    ) => Promise<ValidationResult>;
    validateParticipantByName: (participantName: string) => Promise<boolean>;
    validatePositionAccountByNameAndCurrency: (
      participantName: string,
      currency: string
    ) => Promise<boolean>;
    validateParticipantTransferId: (participantName: string, transferId: string) => Promise<boolean>;
    validateFulfilCondition: (fulfilment: string, condition: string) => boolean;
    validationReasons: string[];
    handlePayeeResponse: (transferId: string, payload: PayeeResponsePayload, action: any, fspiopError?: any) => Promise<TransformedTransfer>;
    getTransferById: (transferId: string) => Promise<TransferReadModel | null>;
    getTransferInfoToChangePosition: (transferId: string, roleType: any, entryType: any) => Promise<TransferParticipantInfo | null>;
    getTransferFulfilmentDuplicateCheck: any;
    saveTransferFulfilmentDuplicateCheck: any;
    getTransferErrorDuplicateCheck: any;
    saveTransferErrorDuplicateCheck: any;
    transformTransferToFulfil: (transfer: any, isFx: boolean) => any;
    duplicateCheckComparator: (transferId: string, payload: any, getCheck: any, saveCheck: any) => Promise<any>;
    checkDuplication: (args: {
      payload: CreateTransferDto,
      isFx: boolean,
      ID: string,
      location: Location
    }) => Promise<DuplicationCheckResult>;
    savePreparedRequest: (args: {
      validationPassed: boolean,
      reasons: string[],
      payload: CreateTransferDto,
      isFx: boolean,
      functionality: any,
      params: any,
      location: Location,
      determiningTransferCheckResult: TransferCheckResult,
      proxyObligation: ProxyObligation
    }) => Promise<void>;
    getByIDAndCurrency: (
      participantId: number,
      currencyId: string,
      ledgerAccountTypeId: number,
      isCurrencyActive?: boolean
    ) => Promise<ParticipantWithCurrency | null>;
    calculatePreparePositionsBatch: (
      transferList: PositionKafkaMessage[]
    ) => Promise<PreparePositionsBatchResult>;
    changeParticipantPosition: (
      participantCurrencyId: number,
      isReversal: boolean,
      amount: string,
      transferStateChange: TransferStateChange
    ) => Promise<void>;
    getAccountByNameAndCurrency: (participantName: string, currency: string) => Promise<{ currencyIsActive: boolean }>;
    timeoutService: {
      getTimeoutSegment: () => Promise<{ value: number; segmentId: number } | null>;
      cleanupTransferTimeout: () => Promise<any>;
      getLatestTransferStateChange: () => Promise<{ transferStateChangeId: string } | null>;
      timeoutExpireReserved: (
        segmentId: number,
        intervalMin: number,
        intervalMax: number,
        fxSegmentId: number,
        fxIntervalMin: number,
        fxIntervalMax: number
      ) => Promise<{ transferTimeoutList: any[] | null; fxTransferTimeoutList: any[] | null }>;
    };
  }

  /**
   * Legacy functions used for settlement.
   */
  settlement: {
    settlementWindowModel: {
      process: (payload: { settlementWindowId: number, reason: string }, enums: any) => Promise<number>,
      close: (settlementWindowId: number, reason: string) => Promise<unknown>,
    },
    settlementDomain: {
      settlementEventTrigger: (
        params: {
          settlementModel: string,
          reason: string,
          settlementWindows: Array<Record<string, number>>
        },
        enums: any
      ) => Promise<unknown>
      getById: (params: { settlementId: number }, enums: any) => Promise<any>
    }
    settlementModel: {
      putById: (
        settlementId: number,
        payload: {
          participants: Array<{
            id: number
            accounts: Array<{
              id: number
              state: string
              reason: string
              externalReference?: string
            }>
          }>
        },
        enums: any
      ) => Promise<any>
    }
    enums: {
      ledgerAccountTypes: Record<string, number>
      ledgerEntryTypes: Record<string, number>
      participantLimitTypes: Record<string, number>
      settlementDelay: Record<string, number>
      settlementDelayEnums: Record<string, number>
      settlementGranularity: Record<string, number>
      settlementGranularityEnums: Record<string, number>
      settlementInterchangeEnums: Record<string, number>
      settlementStates: Record<string, number>
      settlementWindowStates: Record<string, number>
      transferParticipantRoleTypes: Record<string, number>
      transferStateEnums: Record<string, number>
      transferStates: Record<string, number>
      settlementInterchange: Record<string, number>
    }
  }
}

/**
 * @class LegacyLedger
 * @description Collects the business logic from all ledger-related activites into a common
 *   interface which can be abstracted out and reimplemented with TigerBeetle
 */
export default class LegacyLedger implements Ledger {
  constructor(private deps: LegacyLedgerDependencies) { }


  // ============================================================================
  // Lifecycle Methods
  // ============================================================================

  public async createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    assert(cmd.currency)
    assert(cmd.settlementModel)
    assert(cmd.settlementModel.name)
    assert(cmd.settlementModel.settlementGranularity)
    assert(cmd.settlementModel.settlementInterchange)
    assert(cmd.settlementModel.settlementDelay)
    assert.equal(cmd.settlementModel.currency, cmd.currency)
    assert(cmd.settlementModel.requireLiquidityCheck === true, 'createHubAccount - currently only allows settlements with liquidity checks enabled')
    assert(cmd.settlementModel.ledgerAccountType)
    assert(cmd.settlementModel.settlementAccountType)
    assert(cmd.settlementModel.autoPositionReset === true || cmd.settlementModel.autoPositionReset === false)

    try {
      try {
        await this._createHubAccount('HUB_MULTILATERAL_SETTLEMENT', cmd.currency)
        await this._createHubAccount('HUB_RECONCILIATION', cmd.currency)
      } catch (err) {
        // catch this early, since we can't know if the settlementModel has also already been created
        if ((err as ErrorHandler.FSPIOPError).message === 'Hub account has already been registered.') {
          logger.warn('createHubAccount', { error: err })
        } else {
          throw err
        }
      }

      await this.deps.lifecycle.settlementModelDomain.createSettlementModel(cmd.settlementModel)
      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      if (err.message === 'Settlement Model already exists') {
        return {
          type: 'ALREADY_EXISTS'
        }
      }

      return Helper.commandResultFailure(err)
    }
  }

  private async _createHubAccount(accountType: string, currency: string): Promise<void> {
    const participant = await this.deps.lifecycle.participantService.getByName('Hub')
    if (!participant) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ADD_PARTY_INFO_ERROR,
        'Participant was not found.'
      )
    }

    const ledgerAccountTypes = this.deps.lifecycle.enums.ledgerAccountType
    const ledgerAccountTypeId = ledgerAccountTypes[accountType]
    if (!ledgerAccountTypeId) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ADD_PARTY_INFO_ERROR,
        'Ledger account type was not found.'
      )
    }

    // Check if account already exists by looking through participant's currency list
    const accountExists = participant.currencyList.some(
      curr => curr.currencyId === currency && curr.ledgerAccountTypeId === ledgerAccountTypeId
    )
    if (accountExists) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ADD_PARTY_INFO_ERROR,
        'Hub account has already been registered.'
      )
    }

    if (participant.participantId !== this.deps.config.HUB_ID) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ADD_PARTY_INFO_ERROR,
        'Endpoint is reserved for creation of Hub account types only.'
      )
    }

    const isPermittedHubAccountType = this.deps.config.HUB_ACCOUNTS.indexOf(accountType) >= 0
    if (!isPermittedHubAccountType) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ADD_PARTY_INFO_ERROR,
        'The requested hub operator account type is not allowed.'
      )
    }

    const newCurrencyAccount = await this.deps.lifecycle.participantService.createHubAccount(
      participant.participantId,
      currency,
      ledgerAccountTypeId
    )
    if (!newCurrencyAccount) {
      throw ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.ADD_PARTY_INFO_ERROR,
        'Participant account and Position create have failed.'
      )
    }
  }


  /**
   * @description Create the dfsp accounts. Returns a duplicate response if any of the dfsp + 
   * currency combinations already exist.
   */
  public async createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse> {
    assert(cmd.dfspId)
    assert(cmd.currencies)
    assert(cmd.currencies.length > 0)
    assert(cmd.currencies.length < 16, 'Cannot register more than 16 currencies for a DFSP')

    try {
      const participant = await this.deps.lifecycle.participantService.getByName(cmd.dfspId);

      if (participant) {
        // If any of the new currencies to be registered are already created, then return 'ALREADY_EXISTS'
        const existingCurrencies = participant.currencyList.map(c => c.currencyId)
        const currencyAlreadyRegistered = existingCurrencies.some(curr => cmd.currencies.includes(curr))

        if (currencyAlreadyRegistered) {
          return {
            type: 'ALREADY_EXISTS'
          }
        }
      }

      // Create participant and currency accounts directly.
      for (const currency of cmd.currencies) {
        await this.createParticipantWithCurrency(cmd.dfspId, currency);
      }

      // Set the initial limits
      for (let i = 0; i < cmd.currencies.length; i++) {
        const currency = cmd.currencies[i];
        assert(currency)

        // Get participant accounts to get the participantCurrencyIds needed by the facade
        const positionAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
          cmd.dfspId,
          currency,
          Enum.Accounts.LedgerAccountType.POSITION
        );
        assert(positionAccount)
        const settlementAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
          cmd.dfspId,
          currency,
          Enum.Accounts.LedgerAccountType.SETTLEMENT
        );
        assert(settlementAccount)
      }

      return {
        type: 'SUCCESS'
      }


    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async disableDfsp(cmd: { dfspId: string }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)

    try {
      const dfspId = cmd.dfspId
      await this.deps.lifecycle.participantService.update(
        dfspId, { isActive: false }
      )

      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async enableDfsp(cmd: { dfspId: string }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)

    try {
      const dfspId = cmd.dfspId
      await this.deps.lifecycle.participantService.update(
        dfspId, { isActive: true }
      )
      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async enableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    assert(cmd.accountId)

    try {
      await this.deps.lifecycle.participantService.updateAccount(
        { isActive: true },
        { name: cmd.dfspId, id: cmd.accountId },
        this.deps.lifecycle.enums
      )

      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async disableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    assert(cmd.accountId)

    try {
      await this.deps.lifecycle.participantService.updateAccount(
        { isActive: false },
        { name: cmd.dfspId, id: cmd.accountId },
        this.deps.lifecycle.enums
      )

      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async deposit(cmd: DepositCommand): Promise<DepositResponse> {
    assert(cmd.amount)
    assert(cmd.amount > 0, 'depositCollateral amount must be greater than 0')
    assert(cmd.dfspId)
    assert(cmd.currency)
    assert(cmd.transferId)
    assert(cmd.reason)

    try {
      const enums = this.deps.lifecycle.enums

      // Get both settlement and position accounts
      const settlementAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
        cmd.dfspId,
        cmd.currency,
        Enum.Accounts.LedgerAccountType.SETTLEMENT
      );
      assert(settlementAccount, 'Settlement account not found');

      const positionAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
        cmd.dfspId,
        cmd.currency,
        Enum.Accounts.LedgerAccountType.POSITION
      );
      assert(positionAccount, 'Position account not found');

      // Create participantPosition and activate SETTLEMENT account if needed (BEFORE validation)
      const existingSettlementPosition = await this.deps.knex('participantPosition')
        .where('participantCurrencyId', settlementAccount.participantCurrencyId)
        .first();

      if (!existingSettlementPosition) {
        await this.deps.knex('participantPosition').insert({
          participantCurrencyId: settlementAccount.participantCurrencyId,
          value: 0,
          reservedValue: 0
        });

        // Activate the settlement account so validation passes
        await this.deps.knex('participantCurrency')
          .update({ isActive: 1 })
          .where('participantCurrencyId', settlementAccount.participantCurrencyId);
      }

      // Create participantPosition and activate POSITION account if needed
      const existingPositionPosition = await this.deps.knex('participantPosition')
        .where('participantCurrencyId', positionAccount.participantCurrencyId)
        .first();

      if (!existingPositionPosition) {
        await this.deps.knex('participantPosition').insert({
          participantCurrencyId: positionAccount.participantCurrencyId,
          value: 0,
          reservedValue: 0
        });

        // Activate the position account
        await this.deps.knex('participantCurrency')
          .update({ isActive: 1 })
          .where('participantCurrencyId', positionAccount.participantCurrencyId);
      }

      // Prepare payload for validation
      const payload = {
        action: Enum.Events.Event.Action.RECORD_FUNDS_IN,
        reason: cmd.reason,
        externalReference: `deposit-${cmd.dfspId}`,
        amount: {
          amount: cmd.amount.toString(),
          currency: cmd.currency
        }
      };

      // Call recordFundsInOut directly
      const validationResult = await this.deps.lifecycle.participantService.recordFundsInOut(
        payload,
        { name: cmd.dfspId, id: settlementAccount.participantCurrencyId, transferId: cmd.transferId },
        enums
      );

      // Use the validated account and payload
      const { accountMatched, payload: validatedPayload } = validationResult;
      validatedPayload.participantCurrencyId = accountMatched.participantCurrencyId;

      const now = new Date()
      const transactionTimestamp = Time.getUTCString(now)

      // Save duplicate check record first (required by foreign key constraint)
      await this.deps.lifecycle.transferService.saveTransferDuplicateCheck(cmd.transferId, validatedPayload);

      // Call admin handler directly (bypassing Kafka)
      await this.deps.lifecycle.adminHandler.createRecordFundsInOut(
        validatedPayload,
        transactionTimestamp,
        enums
      );

      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      // Check for duplicate transferId error
      if (err.code === 'ER_DUP_ENTRY' && err.message?.includes('transferDuplicateCheck.PRIMARY')) {
        return {
          type: 'ALREADY_EXISTS'
        }
      }
      return Helper.commandResultFailure(err)
    }
  }

  public async withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse> {
    assert(cmd.amount)
    assert(cmd.amount > 0, 'withdraw amount must be greater than 0')
    assert(cmd.dfspId)
    assert(cmd.currency)
    assert(cmd.transferId)
    assert(cmd.reason)

    try {
      const enums = this.deps.lifecycle.enums

      // Get both settlement and position accounts
      const settlementAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
        cmd.dfspId,
        cmd.currency,
        Enum.Accounts.LedgerAccountType.SETTLEMENT
      );
      assert(settlementAccount, 'Settlement account not found');

      const positionAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
        cmd.dfspId,
        cmd.currency,
        Enum.Accounts.LedgerAccountType.POSITION
      );
      assert(positionAccount, 'Position account not found');

      // Create participantPosition and activate SETTLEMENT account if needed (BEFORE validation)
      const existingSettlementPosition = await this.deps.knex('participantPosition')
        .where('participantCurrencyId', settlementAccount.participantCurrencyId)
        .first();

      if (!existingSettlementPosition) {
        await this.deps.knex('participantPosition').insert({
          participantCurrencyId: settlementAccount.participantCurrencyId,
          value: 0,
          reservedValue: 0
        });

        // Activate the settlement account so validation passes
        await this.deps.knex('participantCurrency')
          .update({ isActive: 1 })
          .where('participantCurrencyId', settlementAccount.participantCurrencyId);
      }

      // Create participantPosition and activate POSITION account if needed
      const existingPositionPosition = await this.deps.knex('participantPosition')
        .where('participantCurrencyId', positionAccount.participantCurrencyId)
        .first();

      if (!existingPositionPosition) {
        await this.deps.knex('participantPosition').insert({
          participantCurrencyId: positionAccount.participantCurrencyId,
          value: 0,
          reservedValue: 0
        });

        // Activate the position account
        await this.deps.knex('participantCurrency')
          .update({ isActive: 1 })
          .where('participantCurrencyId', positionAccount.participantCurrencyId);
      }

      // Prepare payload for validation
      const payload = {
        action: Enum.Events.Event.Action.RECORD_FUNDS_OUT_PREPARE_RESERVE,
        reason: cmd.reason,
        externalReference: `withdrawal-${cmd.dfspId}`,
        amount: {
          amount: cmd.amount.toString(),
          currency: cmd.currency
        }
      };

      // Call recordFundsInOut for validation (Kafka disabled)
      const validationResult = await this.deps.lifecycle.participantService.recordFundsInOut(
        payload,
        { name: cmd.dfspId, id: settlementAccount.participantCurrencyId, transferId: cmd.transferId },
        enums
      );

      // Use the validated account and payload
      const { accountMatched, payload: validatedPayload } = validationResult;
      validatedPayload.participantCurrencyId = accountMatched.participantCurrencyId;

      const now = new Date()
      const transactionTimestamp = Time.getUTCString(now)

      // Save duplicate check record first (required by foreign key constraint)
      await this.deps.lifecycle.transferService.saveTransferDuplicateCheck(cmd.transferId, validatedPayload);

      // Call admin handler directly (bypassing Kafka)
      await this.deps.lifecycle.adminHandler.createRecordFundsInOut(
        validatedPayload,
        transactionTimestamp,
        enums
      );

      // Check if the withdrawal was aborted due to insufficient funds
      const transferState = await this.deps.lifecycle.transferFacade.getTransferStateByTransferId(cmd.transferId)
      if (transferState.transferStateId === 'ABORTED_REJECTED') {
        return {
          type: 'INSUFFICIENT_FUNDS'
        }
      }

      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    assert(cmd.transferId)

    try {
      const enums = this.deps.lifecycle.enums

      const payload = {
        transferId: cmd.transferId,
        action: Enum.Events.Event.Action.RECORD_FUNDS_OUT_COMMIT
      } as any;

      const now = new Date()
      const transactionTimestamp = Time.getUTCString(now)

      // Call admin handler directly (bypassing Kafka)
      await this.deps.lifecycle.adminHandler.changeStatusOfRecordFundsOut(
        payload,
        cmd.transferId,
        transactionTimestamp,
        enums
      );
      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async withdrawAbort(cmd: WithdrawAbortCommand): Promise<WithdrawAbortResponse> {
    assert(cmd.transferId)

    try {
      const enums = this.deps.lifecycle.enums

      const payload = {
        transferId: cmd.transferId,
        action: Enum.Events.Event.Action.RECORD_FUNDS_OUT_ABORT
      } as any;

      const now = new Date()
      const transactionTimestamp = Time.getUTCString(now)

      // Call admin handler directly (bypassing Kafka)
      await this.deps.lifecycle.adminHandler.changeStatusOfRecordFundsOut(
        payload,
        cmd.transferId,
        transactionTimestamp,
        enums
      );
      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      return Helper.commandResultFailure(err)
    }
  }

  public async setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    assert(cmd.netDebitCapType === 'LIMITED', 'LegacyLedger does not support setting an unlimited net debit cap. Set to a very large number instead.')
    assert(cmd.dfspId)
    assert(cmd.currency)
    assert(cmd.amount)

    try {
      const payload = {
        currency: cmd.currency,
        limit: {
          type: 'NET_DEBIT_CAP',
          value: cmd.amount,
          // TODO(LD): is this ever used? going to hardcode to 10 for now
          thresholdAlarmPercentage: 10
        }
      }
      await this.deps.lifecycle.participantService.adjustLimits(cmd.dfspId, payload)
      return Helper.emptyCommandResultSuccess()
    } catch (err) {
      // Create the initial limit if it doesn't exist
      if ((err as ErrorHandler.FSPIOPError).message === 'Participant Limit does not exist') {
        try {
          await this.createParticipantLimit(cmd.dfspId, cmd.currency, cmd.amount)
          return Helper.emptyCommandResultSuccess()
        } catch (createErr) {
          return Helper.commandResultFailure(createErr)
        }
      }

      return Helper.commandResultFailure(err)
    }
  }

  public async getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse> {
    const legacyQuery = { currency: query.currency }
    try {
      let accounts = await this.deps.lifecycle.participantService.getAccounts(query.dfspId, legacyQuery)
      // ensure they are always ordered by id
      accounts = accounts.toSorted((a, b) => a.id - b.id)

      const formattedAccounts: Array<LegacyLedgerAccount> = []
      accounts.forEach(account => {
        // Map from the internal legacy participantService representation to
        // a compatible Ledger Interface
        const formattedAccount: LegacyLedgerAccount = {
          id: BigInt(account.id),
          ledgerAccountType: account.ledgerAccountType,
          currency: account.currency,
          isActive: Boolean(account.isActive),
          // TODO(LD): map the numbers!
          value: safeStringToNumber(account.value),
          reservedValue: safeStringToNumber(account.reservedValue),
          changedDate: new Date(account.changedDate)
        }
        formattedAccounts.push(formattedAccount)
      })

      assert(formattedAccounts.length === accounts.length)

      return {
        type: 'SUCCESS',
        accounts: formattedAccounts,
      }

    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse> {
    const legacyQuery = {}
    try {
      let accounts = await this.deps.lifecycle.participantService.getAccounts(query.dfspId, legacyQuery)
      // ensure they are always ordered by id
      accounts = accounts.toSorted((a, b) => a.id - b.id)

      const formattedAccounts: Array<LegacyLedgerAccount> = []
      accounts.forEach(account => {
        // Map from the internal legacy participantService representation to
        // a compatible Ledger Interface
        const formattedAccount: LegacyLedgerAccount = {
          id: BigInt(account.id),
          ledgerAccountType: account.ledgerAccountType,
          currency: account.currency,
          isActive: Boolean(account.isActive),
          // TODO(LD): map the numbers!
          value: safeStringToNumber(account.value),
          reservedValue: safeStringToNumber(account.reservedValue),
          changedDate: new Date(account.changedDate)
        }
        formattedAccounts.push(formattedAccount)
      })

      assert(formattedAccounts.length === accounts.length)

      return {
        type: 'SUCCESS',
        accounts: formattedAccounts,
      }

    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async getHubAccounts(query: GetHubAccountsQuery): Promise<HubAccountResponse> {
    try {
      const participants = await this.deps.lifecycle.participantService.getByName('Hub')
      const ledgerAccountTypes: Record<string, number> = this.deps.lifecycle.enums.ledgerAccountType
      const ledgerAccountIdMap = Object.keys(ledgerAccountTypes).reduce((acc, ledgerAccountType) => {
        const ledgerAccountId = ledgerAccountTypes[ledgerAccountType]
        acc[ledgerAccountId] = ledgerAccountType
        return acc
      }, {})

      const formattedAccounts: Array<LegacyLedgerAccount> = []
      participants.currencyList.forEach(currency => {
        const ledgerAccountType = ledgerAccountIdMap[currency.ledgerAccountTypeId]
        assert(ledgerAccountType)
        const formattedAccount: LegacyLedgerAccount = {
          id: BigInt(currency.participantCurrencyId),
          ledgerAccountType,
          currency: currency.currencyId,
          isActive: Boolean(currency.isActive),
          changedDate: new Date(currency.createdDate),
          // These feel wrong to me - we should just return the value anyway
          // but the getByName query doesn't look up account values.
          value: 0,
          reservedValue: 0,
        }
        formattedAccounts.push(formattedAccount)
      })

      return {
        type: 'SUCCESS',
        accounts: formattedAccounts,
      }

    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async getAllDfsps(_query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>> {
    try {
      const participants = await this.deps.lifecycle.participantService.getAll()
      const ledgerAccountTypes: Record<string, number> = this.deps.lifecycle.enums.ledgerAccountType
      const ledgerAccountIdMap = Object.keys(ledgerAccountTypes).reduce((acc, ledgerAccountType) => {
        const ledgerAccountId = ledgerAccountTypes[ledgerAccountType]
        acc[ledgerAccountId] = ledgerAccountType
        return acc
      }, {})

      const dfsps: Array<LegacyLedgerDfsp> = []
      participants.forEach(participant => {
        // Filter out the Hub accounts
        if (participant.name === 'Hub') {
          return
        }

        const formattedAccounts: Array<LegacyLedgerAccount> = []
        participant.currencyList.forEach(currency => {
          const ledgerAccountType = ledgerAccountIdMap[currency.ledgerAccountTypeId]
          assert(ledgerAccountType)
          const formattedAccount: LegacyLedgerAccount = {
            id: BigInt(currency.participantCurrencyId),
            ledgerAccountType,
            currency: currency.currencyId,
            isActive: Boolean(currency.isActive),
            changedDate: new Date(currency.createdDate),
            // These feel wrong to me - we should just return the value anyway
            // but the getByName query doesn't look up account values.
            value: 0,
            reservedValue: 0,
          }
          formattedAccounts.push(formattedAccount)
        })

        dfsps.push({
          name: participant.name,
          isActive: participant.isActive === 1,
          created: undefined,
          accounts: formattedAccounts
        })
      })

      return Helper.queryResultSuccess({ dfsps })
    } catch (err) {
      return Helper.queryResultFailure(err)
    }
  }

  // TODO: can we refactor all the mapping stuff to combine it with above?
  public async getDfsp(query: { dfspId: string; }): Promise<QueryResult<LegacyLedgerDfsp>> {
    try {
      const participant = await this.deps.lifecycle.participantService.getByName(query.dfspId)
      assert(participant, 'expected participant to be defined')
      const ledgerAccountTypes: Record<string, number> = this.deps.lifecycle.enums.ledgerAccountType
      const ledgerAccountIdMap = Object.keys(ledgerAccountTypes).reduce((acc, ledgerAccountType) => {
        const ledgerAccountId = ledgerAccountTypes[ledgerAccountType]
        acc[ledgerAccountId] = ledgerAccountType
        return acc
      }, {})

      const formattedAccounts: Array<LegacyLedgerAccount> = []
      participant.currencyList.forEach(currency => {
        const ledgerAccountType = ledgerAccountIdMap[currency.ledgerAccountTypeId]
        assert(ledgerAccountType)
        const formattedAccount: LegacyLedgerAccount = {
          id: BigInt(currency.participantCurrencyId),
          ledgerAccountType,
          currency: currency.currencyId,
          isActive: Boolean(currency.isActive),
          changedDate: new Date(currency.createdDate),
          // These feel wrong to me - we should just return the value anyway
          // but the getByName query doesn't look up account values.
          value: 0,
          reservedValue: 0,
        }
        formattedAccounts.push(formattedAccount)
      })

      const dfsp: LegacyLedgerDfsp = {
        name: participant.name,
        isActive: participant.isActive === 1,
        created: new Date(participant.createdDate),
        accounts: formattedAccounts
      }

      return Helper.queryResultSuccess(dfsp)
    } catch (err) {
      return Helper.queryResultFailure(err)
    }
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResult<LegacyLimit>> {
    const legacyQuery = { currency: query.currency, type: 'NET_DEBIT_CAP' }
    try {
      const result = await this.deps.lifecycle.participantService.getLimits(query.dfspId, legacyQuery)
      if (result.length === 0) {
        return {
          type: 'FAILURE',
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
            `getNetDebitCap() - no limits found for dfspId: ${query.dfspId}, currency: ${query.currency}, type: 'NET_DEBIT_CAP`
          )
        }
      }

      assert(result.length === 1)
      const legacyLimit = result[0] as { value: string, thresholdAlarmPercentage: string }
      assert(legacyLimit.value)
      assert(legacyLimit.thresholdAlarmPercentage)
      return {
        type: 'SUCCESS',
        result: {
          type: 'NET_DEBIT_CAP',
          value: safeStringToNumber(legacyLimit.value),
          alarmPercentage: safeStringToNumber(legacyLimit.thresholdAlarmPercentage)
        }

      }

    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  /**
   * Create participant and currency accounts directly (bypassing handler to avoid circular dependency)
   * This extracts the core logic from the participants handler create method
   */
  private async createParticipantWithCurrency(dfspId: string, currency: string): Promise<void> {
    await this.deps.lifecycle.participantService.validateHubAccounts(currency)

    let participant = await this.deps.lifecycle.participantService.getByName(dfspId)
    if (participant) {
      const currencyExists = participant.currencyList.find((curr: any) => {
        return curr.currencyId === currency
      })
      if (currencyExists) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR,
          'Participant currency has already been registered'
        )
      }
    } else {
      const participantId = await this.deps.lifecycle.participantService.create({ name: dfspId })
      participant = await this.deps.lifecycle.participantService.getById(participantId)
    }

    const allSettlementModels = await this.deps.lifecycle.settlementModelDomain.getAll()
    let settlementModels = allSettlementModels.filter(model => model.currencyId === currency)
    if (settlementModels.length === 0) {
      settlementModels = allSettlementModels.filter(model => model.currencyId === null) // Default settlement model
      if (settlementModels.length === 0) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.GENERIC_SETTLEMENT_ERROR,
          'Unable to find a matching or default, Settlement Model'
        )
      }
    }

    for (const settlementModel of settlementModels) {
      // TODO(LD): Ideally these would be created in a transaction - as it stands right now, these are non
      // atomically created.
      const participantCurrencyPosition = await this.deps.lifecycle.participantService.createParticipantCurrency(participant.participantId, currency, settlementModel.ledgerAccountTypeId, false)
      const participantCurrencySettlement = await this.deps.lifecycle.participantService.createParticipantCurrency(participant.participantId, currency, settlementModel.settlementAccountTypeId, false)

      if (Array.isArray(participant.currencyList)) {
        participant.currencyList = participant.currencyList.concat([
          await this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencyPosition),
          await this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencySettlement)
        ])
      } else {
        participant.currencyList = await Promise.all([
          this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencyPosition),
          this.deps.lifecycle.participantService.getParticipantCurrencyById(participantCurrencySettlement)
        ])
      }
    }
  }

  /**
   * Creates a participant limit when it doesn't exist
   * This directly inserts a limit into the database when positions already exist
   * but no limit has been set yet.
   *
   * @param dfspId - The participant name
   * @param currency - The currency for the limit
   * @param limitValue - The limit value to set
   */
  private async createParticipantLimit(dfspId: string, currency: string, limitValue: number): Promise<void> {
    try {
      // Get the position account to get the participantCurrencyId
      const positionAccount = await this.deps.lifecycle.participantFacade.getByNameAndCurrency(
        dfspId,
        currency,
        Enum.Accounts.LedgerAccountType.POSITION
      )
      assert(positionAccount, `Position account not found for ${dfspId} ${currency}`)

      // Get the limit type ID for NET_DEBIT_CAP
      const limitType = await this.deps.knex('participantLimitType')
        .where({ name: 'NET_DEBIT_CAP', isActive: 1 })
        .select('participantLimitTypeId')
        .first()

      assert(limitType, 'NET_DEBIT_CAP limit type not found')

      // Insert the new limit
      const participantLimit = {
        participantCurrencyId: positionAccount.participantCurrencyId,
        participantLimitTypeId: limitType.participantLimitTypeId,
        value: limitValue,
        thresholdAlarmPercentage: 10,
        isActive: 1,
        createdBy: 'unknown'
      }

      await this.deps.knex('participantLimit').insert(participantLimit)

      logger.info(`Successfully created participant limit for ${dfspId} ${currency}`, {
        limitValue,
        participantCurrencyId: positionAccount.participantCurrencyId
      })
    } catch (err) {
      logger.error(`Failed to create participant limit for ${dfspId} ${currency}`, err)
      throw ErrorHandler.Factory.reformatFSPIOPError(err)
    }
  }

  // ============================================================================
  // Clearing Methods
  // ============================================================================

  public async prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult> {
    const { payload, transferId, headers } = input;
    logger.debug(`prepare() - transferId: ${transferId}`)

    const duplicateResult = await this.checkPrepareDuplicate(payload, transferId)
    switch (duplicateResult) {
      case PrepareDuplicateResult.DUPLICATED: {
        const transfer = await this.deps.clearing.getTransferById(transferId)
        assert(transfer.transferStateEnumeration)
        const finalizedStates = [
          Enum.Transfers.TransferState.COMMITTED,
          Enum.Transfers.TransferState.ABORTED,
          Enum.Transfers.TransferState.RESERVED
        ].map(e => e.toString())

        if (finalizedStates.includes(transfer.transferStateEnumeration)) {
          const payload = this.deps.clearing.transformTransferToFulfil(transfer, false)
          return {
            type: PrepareResultType.DUPLICATE_FINAL,
            finalizedTransfer: payload,
          }
        }

        return {
          type: PrepareResultType.DUPLICATE_NON_FINAL
        }
      }
      case PrepareDuplicateResult.MODIFIED: {
        return {
          type: PrepareResultType.MODIFIED
        }
      }
      case PrepareDuplicateResult.UNIQUE:
      default: { }
    }

    // Validate participants and their currency accounts
    const participantValidation = await this.validateParticipants(payload)
    if (!participantValidation.validationPassed) {
      return {
        type: PrepareResultType.FAIL_VALIDATION,
        failureReasons: participantValidation.reasons
      };
    }

    // Save the transfer, even if it's invalid
    const transferValidationResult = await this.validateTransfer(payload, headers)
    await this.saveTransfer(payload, transferValidationResult)

    if (!transferValidationResult.validationPassed) {
      return {
        type: PrepareResultType.FAIL_VALIDATION,
        failureReasons: transferValidationResult.reasons,
      }
    }

    // TODO(LD): this is really ugly, but the original method needs a lot of kafka context,
    // so for compatibility we are going to keep it this way for now.
    //
    // Ideally we would refactor the positions to not require all of this Kafka context
    const messageContext = LegacyLedger.extractMessageContext(input);
    const { preparedMessagesList } = await this.calculatePreparePositions(payload, messageContext)
    assert(Array.isArray(preparedMessagesList))
    assert(preparedMessagesList.length === 1)

    // Process the prepared messages results
    const prepareMessage: PreparedMessage = preparedMessagesList[0];
    const { transferState, fspiopError } = prepareMessage;

    if (transferState.transferStateId !== Enum.Transfers.TransferState.RESERVED) {
      logger.info(`prepare() - Position prepare failed - insufficient liquidity for transfer: ${transferId}`);

      return {
        type: PrepareResultType.FAIL_LIQUIDITY,
        error: fspiopError
      }
    }

    logger.debug(`prepare() - Position prepare successful - funds reserved for transfer: ${transferId}`);
    return {
      type: PrepareResultType.PASS
    }
  }

  public async fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    const { payload, transferId, headers } = input;
    logger.debug(`fulfil() - transferId: ${transferId}`)

    // Handle ABORT action separately
    if (input.action === Enum.Events.Event.Action.ABORT) {
      return await this.handleAbort(input);
    }

    try {
      // TODO(LD): we changed the order of processing here to include the condition
      // which might change some of the error messages
      await this.validateFulfilMessage(input)
    } catch (err) {
      return {
        type: FulfilResultType.FAIL_VALIDATION,
        error: err
      }
    }

    const duplicateResult = await this.checkFulfilDuplicate(payload, transferId)
    switch (duplicateResult) {
      case FulfilDuplicateResult.DUPLICATED: {
        return {
          type: FulfilResultType.DUPLICATE_FINAL
        }
      }
      case FulfilDuplicateResult.MODIFIED: {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.MODIFIED_REQUEST,
            'Transfer fulfil has been modified'
          ),
        }
      }
      case FulfilDuplicateResult.UNIQUE:
      default: { }
    }

    // save the fulfil response
    await this.deps.clearing.handlePayeeResponse(transferId, payload, input.action);

    // Update the positions
    logger.info(`Processing position commit for transfer: ${transferId}`);
    try {
      // Get transfer info to change position for PAYEE
      const transferInfo = await this.deps.clearing.getTransferInfoToChangePosition(
        transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYEE_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      // Get participant currency info
      const participantCurrency = await this.deps.clearing.getByIDAndCurrency(
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

        return {
          type: FulfilResultType.FAIL_OTHER,
          error: fspiopError
        }
      }

      logger.info(`Position commit validation passed for transfer: ${transferId}`);

      // Change participant position (not a reversal for commit)
      const isReversal = false;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferState.COMMITTED
      };

      await this.deps.clearing.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      logger.info(`Position commit processed successfully for transfer: ${transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      });

      return {
        type: FulfilResultType.PASS
      }

    } catch (error) {
      logger.error(`Position commit failed for transfer: ${transferId}`, { error: error.message });
      return {
        type: FulfilResultType.FAIL_OTHER,
        error: error
      }
    }
  }

  /**
   * Handle abort/error message for a transfer
   * This reverses the position changes made during prepare
   */
  private async handleAbort(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    const { payload, transferId } = input;
    logger.debug(`handleAbort() - transferId: ${transferId}`)

    // Basic validation - ensure sender exists
    try {
      assert(input)
      assert(input.message)
      assert(input.message.value)
      assert(input.message.value.from)
      assert(payload)

      const { message: { value: { from } } } = input;
      if (!await this.deps.clearing.validateParticipantByName(from)) {
        return {
          type: FulfilResultType.FAIL_VALIDATION,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
            'Participant not found'
          )
        }
      }

      // Check for duplicate abort messages
      const duplicateResult = await this.checkAbortDuplicate(payload, transferId);
      switch (duplicateResult) {
        case FulfilDuplicateResult.DUPLICATED: {
          return {
            type: FulfilResultType.DUPLICATE_FINAL
          }
        }
        case FulfilDuplicateResult.MODIFIED: {
          return {
            type: FulfilResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.MODIFIED_REQUEST,
              'Transfer abort has been modified'
            ),
          }
        }
        case FulfilDuplicateResult.UNIQUE:
        default: { }
      }

      // Extract and validate error information from payload
      let error: ErrorHandler.FSPIOPError;
      const errorInfo = (payload as any).errorInformation;
      if (!errorInfo) {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            'missing error information in callback'
          ),
        }
      }
      error = ErrorHandler.Factory.createFSPIOPErrorFromErrorInformation(errorInfo);

      // Save the abort response
      await this.deps.clearing.handlePayeeResponse(
        transferId,
        payload,
        input.action,
        error.toApiErrorObject(this.deps.config.ERROR_HANDLING)
      );

      // Process position abort (reversal)
      logger.info(`Processing position reversal for transfer: ${transferId}`);
      // Get transfer info to change position for PAYER (we're reversing the prepare)
      const transferInfo = await this.deps.clearing.getTransferInfoToChangePosition(
        transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYER_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      // Get participant currency info for payer
      const participantCurrency = await this.deps.clearing.getByIDAndCurrency(
        transferInfo.participantId,
        transferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      // Validate transfer state - must be in a reserved state to abort
      const validAbortStates = [
        Enum.Transfers.TransferInternalState.RESERVED,
        Enum.Transfers.TransferInternalState.RESERVED_FORWARDED,
        Enum.Transfers.TransferInternalState.RECEIVED_ERROR
      ];

      if (!validAbortStates.includes(transferInfo.transferStateId)) {
        const error = ErrorHandler.Factory.createInternalServerFSPIOPError(
          `Invalid State for abort: ${transferInfo.transferStateId}`
        );

        logger.error(`Position abort validation failed - invalid state for transfer: ${transferId}`, {
          currentState: transferInfo.transferStateId,
          validStates: validAbortStates
        });

        return {
          type: FulfilResultType.FAIL_OTHER,
          error
        }
      }

      logger.info(`Position abort validation passed for transfer: ${transferId}`);

      // Change participant position (IS a reversal for abort - releases reserved funds)
      const isReversal = true;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferInternalState.ABORTED_ERROR
      };

      await this.deps.clearing.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      logger.info(`Position abort processed successfully for transfer: ${transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      });

      return {
        type: FulfilResultType.PASS
      }

    } catch (err) {
      return {
        type: FulfilResultType.FAIL_OTHER,
        error: err
      }
    }
  }

  public async sweepTimedOut(): Promise<SweepResult> {
    try {
      // Get timeout segments
      const timeoutSegment = await this.deps.clearing.timeoutService.getTimeoutSegment();
      const intervalMin = timeoutSegment ? timeoutSegment.value : 0;
      const segmentId = timeoutSegment ? timeoutSegment.segmentId : 0;
      await this.deps.clearing.timeoutService.cleanupTransferTimeout();
      const latestTransferStateChange = await this.deps.clearing.timeoutService.getLatestTransferStateChange();
      const intervalMax = (latestTransferStateChange && parseInt(latestTransferStateChange.transferStateChangeId)) || 0;

      // For now, we pass 0 for fx segments (ignoring fx case)
      const fxSegmentId = 0;
      const fxIntervalMin = 0;
      const fxIntervalMax = 0;

      // Get timed out transfers
      const { transferTimeoutList, fxTransferTimeoutList } = await this.deps.clearing.timeoutService.timeoutExpireReserved(
        segmentId, intervalMin, intervalMax, fxSegmentId, fxIntervalMin, fxIntervalMax
      );

      // Process RESERVED_TIMEOUT transfers - reverse their positions
      if (transferTimeoutList && Array.isArray(transferTimeoutList)) {
        for (const tt of transferTimeoutList) {
          if (tt.transferStateId === Enum.Transfers.TransferInternalState.RESERVED_TIMEOUT) {
            await this.reverseTimedOutTransferPosition(tt);
          }
        }
      }

      const simplifiedTransfers: TimedOutTransfer[] = transferTimeoutList && Array.isArray(transferTimeoutList)
        ? transferTimeoutList.map(tt => ({
          id: tt.transferId,
          payerId: tt.payerFsp,
          payeeId: tt.payeeFsp,
        }))
        : [];

      return {
        type: 'SUCCESS',
        transfers: simplifiedTransfers
      };
    } catch (err) {
      logger.error('sweepTimedOut() failed:', err);
      return {
        type: 'FAILURE',
        error: err instanceof Error ? err : new Error(String(err))
      };
    }
  }

  /**
   * Reverses the position for a timed out transfer that was in RESERVED state
   */
  private async reverseTimedOutTransferPosition(tt: any): Promise<void> {
    try {
      // Get transfer info for PAYER (who had funds reserved)
      const transferInfo = await this.deps.clearing.getTransferInfoToChangePosition(
        tt.transferId,
        Enum.Accounts.TransferParticipantRoleType.PAYER_DFSP,
        Enum.Accounts.LedgerEntryType.PRINCIPLE_VALUE
      );

      if (!transferInfo) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
          'Transfer info not found'
        );
      }

      // Get participant currency info
      const participantCurrency = await this.deps.clearing.getByIDAndCurrency(
        transferInfo.participantId,
        transferInfo.currencyId,
        Enum.Accounts.LedgerAccountType.POSITION
      );

      if (!participantCurrency) {
        throw ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
          'Participant currency not found'
        );
      }

      // Reverse the position (add back reserved funds)
      const isReversal = true;
      const transferStateChange = {
        transferId: transferInfo.transferId,
        transferStateId: Enum.Transfers.TransferInternalState.EXPIRED_RESERVED,
        reason: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_EXPIRED
        ).message
      };

      await this.deps.clearing.changeParticipantPosition(
        participantCurrency.participantCurrencyId,
        isReversal,
        transferInfo.amount,
        transferStateChange
      );

      logger.debug(`Successfully reversed position for timed out transfer: ${tt.transferId}`, {
        participantCurrencyId: participantCurrency.participantCurrencyId,
        amount: transferInfo.amount
      });
    } catch (err) {
      logger.error(`Failed to reverse position for timed out transfer: ${tt.transferId}`, err);
      throw ErrorHandler.Factory.reformatFSPIOPError(err);
    }
  }

  public async lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse> {
    assert(query.transferId)
    try {
      const transfer = await this.deps.clearing.getTransferById(query.transferId)
      if (!transfer) {
        return {
          type: LookupTransferResultType.NOT_FOUND
        }
      }

      if (transfer.transferState === 'RECEIVED') {
        return {
          type: LookupTransferResultType.FOUND_NON_FINAL,
          amountClearingCredit: 0n,
          amountUnrestricted: 0n,

        }
      }

      switch (transfer.transferState) {
        case 'ABORTED': {
          assert(transfer.completedTimestamp, 'Expected transfer.completedTimestamp when transfer.transferState === `COMMITTED`')
          return {
            type: LookupTransferResultType.FOUND_FINAL,
            finalizedTransfer: {
              completedTimestamp: transfer.completedTimestamp,
              transferState: transfer.transferState
            }
          }
        }
        case 'COMMITTED': {
          assert(transfer.completedTimestamp, 'Expected transfer.completedTimestamp when transfer.transferState === `COMMITTED`')
          assert(transfer.fulfilment, 'Expected transfer.fulfilment when transfer.transferState === `COMMITTED`')
          return {
            type: LookupTransferResultType.FOUND_FINAL,
            finalizedTransfer: {
              completedTimestamp: transfer.completedTimestamp,
              transferState: transfer.transferState,
              fulfilment: transfer.fulfilment
            }
          }
        }
        default: {
          throw ErrorHandler.Factory.createInternalServerFSPIOPError(
            `lookupTransfer() failed - getTransferById() found unexpected transferState: ${transfer.transferState}`
          )
        }
      }

    } catch (err) {
      return {
        type: LookupTransferResultType.FAILED,
        error: err
      }
    }
  }

  public async getTransfer(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  // ============================================================================
  // Settlement Methods
  // ============================================================================

  public async closeSettlementWindow(cmd: SettlementCloseWindowCommand): Promise<CommandResult<void>> {
    assert(cmd.id)
    assert(cmd.reason)

    try {
      await this.deps.settlement.settlementWindowModel.process(
        { settlementWindowId: cmd.id, reason: cmd.reason },
        this.deps.settlement.enums.settlementWindowStates
      )
      await this.deps.settlement.settlementWindowModel.close(cmd.id, cmd.reason)

      return {
        type: 'SUCCESS'
      }
    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async settlementPrepare(cmd: SettlementPrepareCommand): Promise<CommandResult<{ id: number }>> {
    assert(cmd.model)
    assert(cmd.reason)
    assert(cmd.windowIds)

    try {
      const triggerResult = await this.deps.settlement.settlementDomain.settlementEventTrigger(
        {
          settlementModel: cmd.model,
          reason: cmd.reason,
          // rewrap in a format the function expects
          settlementWindows: cmd.windowIds.map(id => {
            return { id }
          })
        },
        this.deps.settlement.enums
      ) as { id: number }
      assert(triggerResult.id, 'expected settlementEventTrigger() to return an id')

      return {
        type: 'SUCCESS',
        result: {
          id: triggerResult.id
        }
      }
    }
    catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async settlementAbort(cmd: SettlementAbortCommand): Promise<CommandResult<void>> {
    return {
      type: 'FAILURE',
      error: new Error('not implemented')
    }
  }

  /**
   * Noop for LegacyLedger - the Settlement is considered committed when each participant has been
   * updated
   */
  public async settlementCommit(cmd: SettlementCommitCommand): Promise<CommandResult<void>> {
    return { type: 'SUCCESS' }
  }

  public async settlementUpdate(cmd: SettlementUpdateCommand): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.id)
    assert(cmd.accountId)
    assert(cmd.externalReference)
    assert(cmd.participantId)
    assert(cmd.participantState)
    assert(cmd.reason)

    logger.info(`settlementUpdate() with cmd: ${JSON.stringify(cmd)}`)

    // Map states back to Legacy representation
    let state: 'PS_TRANSFERS_RECORDED' | 'PS_TRANSFERS_RESERVED' | 'PS_TRANSFERS_COMMITTED' | 'SETTLED'
    switch (cmd.participantState) {
      case 'RECORDED': state = 'PS_TRANSFERS_RECORDED'
        break
      case 'RESERVED': state = 'PS_TRANSFERS_RESERVED'
        break
      case 'COMMITTED': state = 'PS_TRANSFERS_COMMITTED'
        break
      case 'SETTLED': state = 'SETTLED'
        break
      default: {
        throw new Error(`Unexpected cmd.participantState: ${cmd.participantState}. Expected it to be \
          [RECORDED | RESERVED | COMMITTED | SETTLED]`)
      }
    }
    try {
      const payload = {
        participants: [
          {
            id: cmd.participantId,
            accounts: [
              {
                id: cmd.accountId,
                state,
                reason: cmd.reason,
                externalReference: cmd.externalReference
              }
            ]
          }
        ]
      }
      await this.deps.settlement.settlementModel.putById(
        cmd.id, payload, this.deps.settlement.enums
      )

      return {
        type: 'SUCCESS'
      }
    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async getSettlement(query: GetSettlementQuery): Promise<GetSettlementQueryResponse> {
    assert(query)
    assert(query.id)
    try {
      const settlement = await this.deps.settlement.settlementDomain.getById(
        { settlementId: query.id }, this.deps.settlement.enums
      )

      return {
        type: 'FOUND',
        ...settlement
      }
    } catch (err) {
      // getById throws if not found
      // TODO(LD): catch the specific not found error!

      return {
        type: 'FAILED',
        error: err
      }
    }
  }


  // ============================================================================
  // Private Methods
  // ============================================================================

  private async validateFulfilMessage(input: FusedFulfilHandlerInput): Promise<void> {
    const { transferId, payload, message: { value: { from } }, headers } = input;

    // make sure the sender exists
    if (!await this.deps.clearing.validateParticipantByName(from)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND, 'Participant not found');
    }

    // Get transfer details
    const transfer = await this.deps.clearing.getTransferById(transferId);
    if (!transfer) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.TRANSFER_ID_NOT_FOUND, 'Transfer ID not found');
    }

    if (!await this.deps.clearing.validateParticipantTransferId(from, transferId)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.CLIENT_ERROR, 'Participant not associated with transfer');
    }

    if (headers[Enum.Http.Headers.FSPIOP.SOURCE].toLowerCase() !== transfer.payeeFsp.toLowerCase()) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Source header does not match transfer payee');
    }

    if (headers[Enum.Http.Headers.FSPIOP.DESTINATION].toLowerCase() !== transfer.payerFsp.toLowerCase()) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'FSPIOP-Destination header does not match transfer payer');
    }

    assert(payload.fulfilment, 'payload.fulfilment not found')
    if (!this.deps.clearing.validateFulfilCondition(payload.fulfilment, transfer.condition)) {
      throw ErrorHandler.Factory.createFSPIOPError(ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR, 'Invalid fulfilment');
    }
  }

  /**
   * Shim Methods to improve usability before refactoring
   */
  private async checkPrepareDuplicate(payload: CreateTransferDto, transferId: string): Promise<PrepareDuplicateResult> {
    const checkDuplicateResult = await this.deps.clearing.checkDuplication({
      payload,
      isFx: false,
      ID: transferId,
      location: { module: 'PrepareHandler', method: 'checkDuplication', path: '' }
    });

    if (checkDuplicateResult.hasDuplicateHash && checkDuplicateResult.hasDuplicateId) {
      return PrepareDuplicateResult.DUPLICATED
    }

    if (checkDuplicateResult.hasDuplicateId) {
      return PrepareDuplicateResult.MODIFIED
    }

    // transfers should be unique
    assert(checkDuplicateResult.hasDuplicateHash === false)
    return PrepareDuplicateResult.UNIQUE
  }

  private async checkFulfilDuplicate(payload: CommitTransferDto, transferId: string): Promise<FulfilDuplicateResult> {
    const checkDuplicateResult = await this.deps.clearing.duplicateCheckComparator(
      transferId,
      payload,
      this.deps.clearing.getTransferFulfilmentDuplicateCheck,
      this.deps.clearing.saveTransferFulfilmentDuplicateCheck
    )

    if (checkDuplicateResult.hasDuplicateHash && checkDuplicateResult.hasDuplicateId) {
      return FulfilDuplicateResult.DUPLICATED
    }

    if (checkDuplicateResult.hasDuplicateId && !checkDuplicateResult.hasDuplicateHash) {
      return FulfilDuplicateResult.MODIFIED
    }

    return FulfilDuplicateResult.UNIQUE
  }

  private async checkAbortDuplicate(payload: any, transferId: string): Promise<FulfilDuplicateResult> {
    const checkDuplicateResult = await this.deps.clearing.duplicateCheckComparator(
      transferId,
      payload,
      this.deps.clearing.getTransferErrorDuplicateCheck,
      this.deps.clearing.saveTransferErrorDuplicateCheck
    )

    if (checkDuplicateResult.hasDuplicateHash && checkDuplicateResult.hasDuplicateId) {
      return FulfilDuplicateResult.DUPLICATED
    }

    if (checkDuplicateResult.hasDuplicateId && !checkDuplicateResult.hasDuplicateHash) {
      return FulfilDuplicateResult.MODIFIED
    }

    return FulfilDuplicateResult.UNIQUE
  }

  private async validateParticipants(payload: CreateTransferDto): Promise<ValidationResult> {
    assert(payload)
    assert(payload.payerFsp)
    assert(payload.payeeFsp)
    assert(payload.amount)
    assert(payload.amount.currency)

    // shortcuts
    const payerId = payload.payerFsp
    const payeeId = payload.payeeFsp
    const currency = payload.amount.currency

    // First check if participants exist and are active
    const payerValid = await this.deps.clearing.validateParticipantByName(payerId);
    const payeeValid = await this.deps.clearing.validateParticipantByName(payeeId);

    if (!payerValid || !payeeValid) {
      return {
        validationPassed: false,
        reasons: ['payer or payee invalid']
      };
    }

    const payerAccountValid = await this.deps.clearing.validatePositionAccountByNameAndCurrency(payerId, currency)
    const payeeAccountValid = await this.deps.clearing.validatePositionAccountByNameAndCurrency(payeeId, currency)

    if (!payerAccountValid || !payeeAccountValid) {
      return {
        validationPassed: false,
        // TODO(LD): nasty globals here
        reasons: [this.deps.clearing.validationReasons[0]]
      }
    }

    return {
      validationPassed: true,
      reasons: []
    }
  }

  private async validateTransfer(payload: CreateTransferDto, headers: any): Promise<ValidationResult> {
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    return await this.deps.clearing.validatePrepare(
      payload,
      headers,
      isFx,
      determiningTransferCheckResult,
      proxyObligation
    );
  }

  private async saveTransfer(payload: CreateTransferDto, validation: ValidationResult): Promise<void> {
    // hardcoded for our use case
    const isFx = false
    const determiningTransferCheckResult = this.createMinimalTransferCheckResult()
    const proxyObligation = this.createMinimalProxyObligation(payload)

    return await this.deps.clearing.savePreparedRequest({
      validationPassed: validation.validationPassed,
      reasons: validation.reasons,
      payload,
      isFx,
      functionality: Enum.Events.Event.Type.TRANSFER,
      params: { message: null }, // Not used in current implementation
      location: { module: 'PrepareHandler', method: 'saveTransfer', path: '' },
      determiningTransferCheckResult,
      proxyObligation
    });
  }

  private async calculatePreparePositions(
    payload: CreateTransferDto,
    messageContext: MessageContext
  ): Promise<PreparePositionsBatchResult> {
    // this.deps.calculatePreparePositionsBatch expects a whole kafka message
    // so transform the payload to one:
    const message = LegacyLedger.createMinimalPositionKafkaMessage(payload, messageContext)
    return this.deps.clearing.calculatePreparePositionsBatch([message])
  }

  // Helper methods to create minimal objects for validation compatibility
  private createMinimalProxyObligation(payload: CreateTransferDto): ProxyObligation {
    return {
      isFx: false,
      payloadClone: { ...payload },
      isInitiatingFspProxy: false,
      isCounterPartyFspProxy: false,
      initiatingFspProxyOrParticipantId: null,
      counterPartyFspProxyOrParticipantId: null
    };
  }

  private createMinimalTransferCheckResult(): TransferCheckResult {
    return {
      watchListRecords: [],
      participantCurrencyValidationList: []
    };
  }

  /**
   * Extracts MessageContext from a PrepareMessageInput for position processing
   * @param input - The prepare message input containing the original Kafka message
   * @returns MessageContext with fields needed for position processing
   */
  static extractMessageContext(input: FusedPrepareHandlerInput): MessageContext {
    const message = input.message;

    return {
      from: message.value.from,
      to: message.value.to,
      headers: input.headers,
      action: input.action,
      eventId: message.value.metadata?.event?.id,
      eventType: message.value.metadata?.event?.type,
      messageId: message.value.id,
      messageType: message.value.type,
      trace: message.value.metadata?.trace
    };
  }

  /**
   * Creates a minimal Kafka message for position processing from a transfer DTO and context
   */
  static createMinimalPositionKafkaMessage(
    payload: CreateTransferDto,
    messageContext: MessageContext
  ): PositionKafkaMessage {
    const now = new Date().toISOString();

    return {
      topic: 'position-prepare',
      key: payload.transferId,
      value: {
        id: messageContext.messageId || payload.transferId,
        from: messageContext.from,
        to: messageContext.to,
        type: messageContext.messageType || 'application/json',
        content: {
          headers: messageContext.headers,
          payload: payload,
          uriParams: { id: payload.transferId },
          context: {}
        },
        metadata: {
          event: {
            id: messageContext.eventId || payload.transferId,
            type: messageContext.eventType || 'position',
            action: messageContext.action,
            createdAt: now,
            state: {
              status: 'success',
              code: 0,
              description: 'action successful'
            }
          },
          trace: messageContext.trace
        }
      }
    };
  }

}