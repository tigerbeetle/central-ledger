import { Enum, Util } from '@mojaloop/central-services-shared';
import assert from "node:assert";
import {
  AccountFilterFlags,
  Client,
  CreateAccountStatus,
  CreateTransferResult,
  CreateTransferStatus,
  id,
  Transfer,
  TransferFlags
} from 'tigerbeetle-node';
import {
  FulfilHandlerInput,
  PaymentFulfilResult
} from '../../../handlers/payment-fulfil';
import {
  PaymentPrepareResult,
  PrepareHandlerInput
} from "../../../handlers/payment-prepare";
import { ApplicationConfig } from "../../../lib/config";
import { assertBoolean } from '../../../lib/config/util';
import { logger } from "../../../shared/logger";
import * as Result from '../shared/results';
import {
  AccountCode,
  AnyQuery,
  CommandResult,
  CreateDfspCommand,
  CreateDfspResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DeactivateDfspResponse,
  DeactivateDfspResponseType,
  DepositCommand,
  DepositResponse,
  DfspAccountResponse,
  Enums,
  GetAllDfspAccountsQuery,
  GetAllDfspsResponse,
  GetDfspAccountsQuery,
  GetNetDebitCapQuery,
  GetNetDebitCapsQuery,
  GetSettlementQuery,
  GetSettlementsQuery,
  GetSettlementsQueryResponse,
  GetSettlementWindowQuery,
  GetSettlementWindowsQuery,
  GetSettlementWindowsQueryResponse,
  HubAccountResponse,
  Ledger,
  LegacyLedgerAccount,
  LegacyLedgerDfsp,
  LegacyLimit,
  LegacyLimitItem,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  QueryResult,
  QueryResultWithNotFound,
  SetNetDebitCapCommand,
  Settlement,
  SettlementAbortCommand,
  SettlementCloseWindowCommand,
  SettlementCommitCommand,
  SettlementPrepareCommand,
  SettlementUpdateCommand,
  SettlementUpdateResult,
  SettlementWindow,
  SweepResult,
  TransferCode,
  WithdrawAbortCommand,
  WithdrawAbortResponse,
  WithdrawCommitCommand,
  WithdrawCommitResponse,
  WithdrawPrepareCommand,
  WithdrawPrepareResponse
} from "../shared/types";
const { TransferState } = Enum.Transfers

import SettlementDomain from '../../settlement';
import { default as Helper, default as LedgerTigerBeetleHelper } from './helper';
import SpecStore, { CmdHubCurrencyEnable } from './spec-store';

const ErrorHandler = require('@mojaloop/central-services-error-handling')

interface Dependencies {
  config: ApplicationConfig
  client: Client,
  helper: Helper,
  enums: Enums,
  specStore: SpecStore,
}


type TransferFailureResult<T> = CreateTransferResult & {
  type: T
}

type PrepareFailureType = 'FAIL_LIQUIDITY' | 'PAYER_CLOSED' | 'PAYEE_CLOSED' | 'MODIFIED' |
  'EXISTS' | 'UNKNOWN'
type AbortFailureType = 'ALREADY_ABORTED' | 'ALREADY_FULFILLED' | 'NOT_FOUND' | 'UNKNOWN'
type FulfilFailureType = 'ALREADY_ABORTED' | 'PAYER_CLOSED' | 'PAYEE_CLOSED' | 'ALREADY_FULFILLED'
  | 'NOT_FOUND' | 'PAYER_ACCOUNT_CLOSED' | 'PAYEE_ACCOUNT_CLOSED' | 'METADATA_CORRUPTED' | 'UNKNOWN'
type WithdrawPrepareFailureType = 'ACCOUNT_CLOSED' | 'TRANSFER_ID_REUSED' | 'INSUFFICIENT_FUNDS' |
  'UNKNOWN'
type WithdrawCommitFailureType = 'NOT_FOUND' | 'UNKNOWN'
type WithdrawAbortFailureType = 'NOT_FOUND' | 'UNKNOWN'
type DepositFailureType = 'EXISTS' | 'MODIFIED' | 'UNKNOWN'
type SetNetDebitCapFailureType = 'UNKNOWN'
type CloseDfspMasterAccountFailureType = 'DEBIT_ACCOUNT_NOT_FOUND' | 'ALREADY_CLOSED' | 'UNKNOWN'
type EnableDfspAccountFailureType = 'ALREADY_ENABLED' | 'UNKNOWN'
type DisableDfspAccountFailureType = 'ALREADY_CLOSED' | 'UNKNOWN'
type SettlementPrepareCreateAccountsFailureType = 'UNKNOWN'

export class LedgerTigerBeetle implements Ledger {
  private readonly config: ApplicationConfig
  private readonly client: Client
  private readonly helper: Helper
  private specStore: SpecStore

  constructor(private deps: Dependencies) {
    this.config = deps.config
    this.client = deps.client
    this.helper = deps.helper
    this.specStore = deps.specStore
  }

  /**
   * In the TigerBeetle Representation of the Ledger, there are no 'Hub' Accounts, since the Hub is
   * implied. Previous handled the Hub as another participant. We _do_ however need to keep track of
   * the settlement models that have been created, and the currencies enabled by the switch.
   */
  public async createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    assert(cmd.currency)
    assert(cmd.settlementModel)
    assert(cmd.settlementModel.name)
    assert(cmd.settlementModel.settlementGranularity)
    assert(cmd.settlementModel.settlementInterchange)
    assert(cmd.settlementModel.settlementDelay)
    assert.equal(cmd.settlementModel.currency, cmd.currency)
    assert(
      cmd.settlementModel.requireLiquidityCheck === true,
      'createHubAccount - currently only allows settlements with liquidity checks enabled'
    )
    assert(cmd.settlementModel.ledgerAccountType)
    assert(cmd.settlementModel.settlementAccountType)
    assertBoolean(cmd.settlementModel.autoPositionReset)

    try {
      // Validate the currency is valid.
      await this.specStore.validateCurrency(cmd.currency)

      const cmdEnableHubCurrency: CmdHubCurrencyEnable = {
        currency: cmd.currency,
        accounts: cmd.accountType ? [cmd.accountType] : []
      }
      const enableHubCurrencyResponse = await this.specStore.enableHubCurrency(cmdEnableHubCurrency)

      if (enableHubCurrencyResponse.type === 'FAILURE') {
        throw enableHubCurrencyResponse.error
      }

      if (enableHubCurrencyResponse.type === 'EXISTS') {
        return {
          type: 'ALREADY_EXISTS_HUB_ACCOUNT'
        }
      }

      await SettlementDomain.createSettlementModel(cmd.settlementModel)
      return Result.emptyCommandResultSuccess()
    } catch (err: any) {
      if (err.message === 'Settlement Model already exists') {
        return {
          type: 'ALREADY_EXISTS_SETTLEMENT_MODEL'
        }
      }

      return Result.commandResultFailure(err)
    }
  }

  /**
   * @method createDfsp
   * @description Create the accounts for the (Dfsp, Currency). If the Dfsp hasn't been created before
   *   sets up the SpecDfsp
   */
  public async createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse> {
    assert(cmd.dfspId)
    assert(cmd.currencies)
    assert(cmd.currencies.length > 0)
    assert.equal(cmd.currencies.length, 1, 'Currently only 1 currency is supported')
    const currency = cmd.currencies[0]

    try {
      await this.specStore.assertCurrenciesEnabled(cmd.currencies)

      // Get or create the specDfsp.
      const masterAccountId = await this.specStore.getOrCreateDfspMasterAccount(cmd.dfspId)
      const accountSpecResult = await this.specStore.getAccountSpec(cmd.dfspId, currency)

      if (accountSpecResult.type === 'FAILURE') {
        return accountSpecResult
      }

      // Lookup accounts in TigerBeetle, ensure they exist.
      if (accountSpecResult.type === 'SUCCESS') {
        const spec = accountSpecResult.result
        const accounts = await this.deps.client.lookupAccounts([
          spec.deposit,
          spec.unrestricted,
          spec.unrestrictedLock,
          spec.restricted,
          spec.reserved,
          spec.commitedOutgoing,
        ])
        if (accounts.length !== 6) {
          throw new Error(`Found existing dfsp: ${cmd.dfspId} for currency: ${currency}. `
            + `But found only ${accounts.length} in TigerBeetle.`)
        }

        // Already exists.
        return {
          type: 'ALREADY_EXISTS'
        }
      }

      return this.createDfspAccounts(cmd.dfspId, currency)
    } catch (err: any) {
      logger.error(`createDfsp() failed with error: ${err.message}`)
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  private async createDfspAccounts(id: string, currency: string): Promise<CreateDfspResponse> {
    try {
      // Backwards compatibility, check that the correct legacy accounts have been created.
      const currencyAccounts = await this.specStore.getCurrencyAccounts(currency)
      const hubReconcilation = currencyAccounts.find(acc => acc.account === 'HUB_RECONCILIATION')
      const hubMultilateralSettlement = currencyAccounts.find(acc => acc.account === 'HUB_MULTILATERAL_SETTLEMENT')
      if (!hubReconcilation) {
        throw new Error(`Hub reconciliation account for the specified currency does not exist.`)
      }
      if (!hubMultilateralSettlement) {
        throw new Error(`Hub multilateral net settlement account for the specified currency does not exist.`)
      }

      // First we create the spec.
      const spec = await this.specStore.newAccountSpec(id, currency)
      const masterAccountId = await this.specStore.getOrCreateDfspMasterAccount(id)
      const ledger = await this.specStore.getCurrencyLedger(currency)
      const accountIdSettlementBalance = await this.specStore.getAccountIdSettlementBalance(currency)

      const accounts = this.helper.buildAccountsDsfp(
        spec, ledger, accountIdSettlementBalance, masterAccountId
      )
      const createAccountResults = await this.client.createAccounts(accounts)
      let fatal = false
      const readableErrors: Array<any> = []
      createAccountResults.forEach((result, idx) => {
        if (result.status === CreateAccountStatus.created) return
        if (result.status === CreateAccountStatus.exists) return
        // This is fine, the 'different flags' could be closed.
        if (result.status === CreateAccountStatus.exists_with_different_flags) return

        readableErrors.push(CreateAccountStatus[result.status])
        const failedAccount = accounts[idx]
        console.error(`Batch account at ${idx} failed to create: ${CreateAccountStatus[result.status]}.\n`
          + `Failed account: ${LedgerTigerBeetleHelper.stringify(failedAccount)}.`
        )
        fatal = true
      })

      if (fatal) {
        return {
          type: 'FAILURE',
          error: new Error(`LedgerError: ${readableErrors.join(',')}`)
        }
      }

      return {
        type: 'SUCCESS'
      }
    } catch (error: any) {
      return {
        type: 'FAILURE',
        error
      }
    }
  }

  public async disableDfsp(cmd: { dfspId: string; }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)

    try {
      const masterAccountId = await this.specStore.getDfspMasterAccount(cmd.dfspId)
      let closeAccountResult = await this.closeDfspMasterAccount(masterAccountId)

      if (closeAccountResult.type === DeactivateDfspResponseType.CREATE_ACCOUNT) {
        await this.createAccountDevNull()
        closeAccountResult = await this.closeDfspMasterAccount(masterAccountId)

        if (closeAccountResult.type === DeactivateDfspResponseType.CREATE_ACCOUNT) {
          throw new Error(`Failed to closeDfspMasterAccount again with no DevNull account!`)
        }
      }

      if (closeAccountResult.type === DeactivateDfspResponseType.FAILED) {
        return Result.commandResultFailure(closeAccountResult.error)
      }

      return Result.emptyCommandResultSuccess()
    } catch (err) {
      return Result.commandResultFailure(err)
    }
  }

  private async closeDfspMasterAccount(masterAccountId: bigint): Promise<DeactivateDfspResponse> {
    // Create a closing transfer to mark this Dfsp as deactivated
    const closingTransfer: Transfer = {
      ...Helper.createTransferTemplate,
      id: id(),
      debit_account_id: Helper.accountIds.devNull,
      credit_account_id: masterAccountId,
      amount: 0n,
      ledger: Helper.ledgerIds.globalControl,
      code: 100,
      flags: TransferFlags.closing_credit | TransferFlags.pending,
    }
    const transferResults = await this.deps.client.createTransfers([closingTransfer])
    const fatalErrors: Array<TransferFailureResult<CloseDfspMasterAccountFailureType>> = []
    transferResults.forEach((result, idx) => {
      if (idx === 0) {
        switch (result.status) {
          case CreateTransferStatus.created: return
          case CreateTransferStatus.debit_account_not_found:
            // In this case, the devNull account hasn't been created yet.
            fatalErrors.push({ type: 'DEBIT_ACCOUNT_NOT_FOUND', ...result })
            return
          case CreateTransferStatus.credit_account_already_closed:
            fatalErrors.push({ type: 'ALREADY_CLOSED', ...result })
            return
          default:
            fatalErrors.push({ type: 'UNKNOWN', ...result })
            return
        }
      }

      throw new Error(`Unhandled createTransfers result at ${idx}, ${CreateTransferStatus[result.status]}`)
    })

    if (fatalErrors.length === 0) {
      return {
        type: DeactivateDfspResponseType.SUCCESS
      }
    }

    const firstError = fatalErrors[0]
    switch (firstError.type) {
      case 'DEBIT_ACCOUNT_NOT_FOUND':
        return {
          type: DeactivateDfspResponseType.CREATE_ACCOUNT
        }
      case 'ALREADY_CLOSED':
        return {
          type: DeactivateDfspResponseType.ALREADY_CLOSED
        }
      case 'UNKNOWN':
        return {
          type: DeactivateDfspResponseType.FAILED,
          error: new Error(`closeDfspMasterAccount failed with unexpected error: `
            + `${CreateTransferStatus[firstError.status]}`)
        }
    }
  }

  /**
   * Lazy creation of the devNull account. Used as a counterparty for things such as closing
   * dfsp master accounts.
   */
  private async createAccountDevNull(): Promise<void> {
    const result = await this.deps.client.createAccounts([
      {
        ...LedgerTigerBeetleHelper.createAccountTemplate,
        id: LedgerTigerBeetleHelper.accountIds.devNull,
        ledger: LedgerTigerBeetleHelper.ledgerIds.globalControl,
        code: AccountCode.Dev_Null,
        flags: 0,
      }
    ])
    const fatalErrors = result.map((result, idx) => {
      if (result.status === CreateAccountStatus.exists) return
      if (result.status === CreateAccountStatus.created) return

      return `createAccounts at idx: ${idx} failed with error: ${CreateAccountStatus[result.status]}.`
    }).filter(status => status !== undefined)

    if (fatalErrors.length > 0) {
      throw new Error(`createAccountDevNull - failed to create counterparty account with error: ` +
        `[${fatalErrors.join(', ')}]`)
    }
  }

  public async enableDfsp(cmd: { dfspId: string; }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)

    try {
      const masterAccountId = await this.specStore.getDfspMasterAccount(cmd.dfspId)
      const transfers = await this.deps.client.getAccountTransfers({
        account_id: masterAccountId,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        code: 0,
        timestamp_min: 0n,
        timestamp_max: 0n,
        limit: 10,
        flags: AccountFilterFlags.credits |
          AccountFilterFlags.reversed,
      })

      if (transfers.length === 0) {
        // Account isn't closed, return success.
        return {
          type: 'SUCCESS'
        }
      }

      // Get the the closing transfer and void it.
      const lastClosingTransferId = transfers[0].id
      const createTransferResults = await this.deps.client.createTransfers([{
        ...LedgerTigerBeetleHelper.createTransferTemplate,
        id: id(),
        debit_account_id: 0n,
        credit_account_id: 0n,
        pending_id: lastClosingTransferId,
        amount: 0n,
        ledger: LedgerTigerBeetleHelper.ledgerIds.globalControl,
        code: 100,
        flags: TransferFlags.void_pending_transfer
      }])

      assert.equal(createTransferResults.length, 1, 'expected just 1 transferError result')
      const result = createTransferResults[0]
      switch (result.status) {
        case CreateTransferStatus.created:
        // Pending closing transfer has already been voided, so the account must be open!
        case CreateTransferStatus.pending_transfer_not_pending:
        case CreateTransferStatus.pending_transfer_already_voided:
          return {
            type: 'SUCCESS'
          }
        default:
          return {
            type: 'FAILURE',
            error: new Error(`enableDfsp failed to void closing transfer with error: `
              + `${CreateTransferStatus[result.status]}`)
          }
      }
    } catch (err) {
      return Result.commandResultFailure(err)
    }
  }

  public async enableDfspAccount(cmd: { dfspId: string; accountId: number; }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    assert(typeof cmd.accountId === 'number')
    const accountId = BigInt(cmd.accountId)

    try {
      // Only the Deposit and Unrestricted Accounts can be enabled/disabled
      const specAccounts = await this.specStore.getAccountSpecs(cmd.dfspId)
      if (specAccounts.length === 0) {
        return {
          type: 'FAILURE',
          error: new Error(`enableDfspAccount() - dfsp: ${cmd.dfspId} not found.`)
        }
      }

      const { currency, code, spec } = await this.specStore.getCurrencyCodeAndSpec(cmd.dfspId, accountId)
      switch (code) {
        case AccountCode.Deposit:
        case AccountCode.Unrestricted:
          break;
        default:
          return {
            type: 'FAILURE',
            error: new Error(`enableDfspAccount() - account id not found, or is not Deposit or Unrestricted.`)
          }
      }

      const ledgers = await this.specStore.getCurrencyLedger(currency)
      // Look up the closing transfer to void it.
      const closingTransfers = (await this.deps.client.getAccountTransfers({
        account_id: accountId,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        code: TransferCode.Close_Account,
        timestamp_min: 0n,
        timestamp_max: 0n,
        limit: 10,
        flags: AccountFilterFlags.credits |
          AccountFilterFlags.reversed,
      })).filter(transfer => transfer.flags & TransferFlags.closing_credit)

      if (closingTransfers.length === 0) {
        // no transfers found, therefore this account must not be closed
        // treat is as successful
        return {
          type: 'SUCCESS'
        }
      }
      const lastClosingTransfer = closingTransfers[0]
      // Void the closing transfer to reopen this account.
      const voidClosingTransfer: Transfer = {
        ...LedgerTigerBeetleHelper.createTransferTemplate,
        id: id(),
        pending_id: lastClosingTransfer.id,
        debit_account_id: spec.unrestrictedLock,
        credit_account_id: code === AccountCode.Deposit ? spec.deposit : spec.unrestricted,
        amount: 0n,
        ledger: ledgers.ledgerOperation,
        code: TransferCode.Close_Account,
        flags: TransferFlags.void_pending_transfer
      }
      const transferResults = await this.deps.client.createTransfers([voidClosingTransfer])
      const fatalErrors: Array<TransferFailureResult<EnableDfspAccountFailureType>> = []

      transferResults.forEach((result, idx) => {
        if (idx === 0) {
          switch (result.status) {
            case CreateTransferStatus.created:
            case CreateTransferStatus.pending_transfer_already_voided:
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...result })
              return
          }
        }

        throw new Error(`Unhandled transfer error: ${idx}, ${CreateTransferStatus[result.status]}.`)
      })

      return {
        type: 'SUCCESS'
      }
    } catch (err: any) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async disableDfspAccount(cmd: { dfspId: string; accountId: number; }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    assert(cmd.accountId)
    const accountId = BigInt(cmd.accountId)

    try {
      // Only the Deposit and Unrestricted Accounts can be enabled/disabled
      const specAccounts = await this.specStore.getAccountSpecs(cmd.dfspId)
      if (specAccounts.length === 0) {
        return {
          type: 'FAILURE',
          error: new Error(`enableDfspAccount() - dfsp: ${cmd.dfspId} not found.`)
        }
      }

      const { currency, code, spec } = await this.specStore.getCurrencyCodeAndSpec(cmd.dfspId, accountId)
      switch (code) {
        case AccountCode.Deposit:
        case AccountCode.Unrestricted:
          break;
        default:
          return {
            type: 'FAILURE',
            error: new Error(`disableDfspAccount() - account is not Deposit or Unrestricted.`)
          }
      }

      const ledgers = await this.specStore.getCurrencyLedger(spec.currency)

      // Create a closing transfer to mark this Account as deactivated
      const closingTransfer: Transfer = {
        ...LedgerTigerBeetleHelper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.unrestrictedLock,
        credit_account_id: spec.unrestricted,
        amount: 0n,
        ledger: ledgers.ledgerOperation,
        code: TransferCode.Close_Account,
        flags: TransferFlags.closing_credit | TransferFlags.pending,
      }
      const transferResults = await this.deps.client.createTransfers([closingTransfer])
      const fatalErrors: Array<TransferFailureResult<DisableDfspAccountFailureType>> = []

      transferResults.forEach((result, idx) => {
        if (idx) {
          switch (result.status) {
            case CreateTransferStatus.created:
            case CreateTransferStatus.credit_account_already_closed:
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...result })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${idx}, ${CreateTransferStatus[result.status]}`)
      })

      return {
        type: 'SUCCESS'
      }

    } catch (err: any) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async deposit(cmd: DepositCommand): Promise<DepositResponse> {
    throw new Error('Method not implemented.');
  }

  public async withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse> {
    throw new Error('Method not implemented.');
  }

  public async withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    throw new Error('Method not implemented.');
  }

  public async withdrawAbort(cmd: WithdrawAbortCommand): Promise<WithdrawAbortResponse> {
    throw new Error('Method not implemented.');
  }

  public async setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  /**
   * @method getHubAccounts
   * 
   * @description There is no concept of a 'Hub Account' in the TigerBeetle implementation, but to 
   * keep backwards compatbility, we return mock Hub accounts.
   */
  public async getHubAccounts(query: AnyQuery): Promise<HubAccountResponse> {
    try {
      const createdDate = await this.specStore.getFirstOrImplyCreationDate()
      const currencyLedgers = await this.specStore.getCurrencyLedgers()
      const currencyAccounts = await this.specStore.getAllCurrencyAccounts()

      const accounts: Array<LegacyLedgerAccount> = []
      currencyAccounts.forEach(acc => {
        accounts.push({
          // id: BigInt(acc.id),
          id: BigInt(0),
          ledgerAccountType: acc.account,
          currency: acc.currency,
          isActive: true,
          changedDate: acc.changedDate,
          createdDate: acc.createdDate,
          value: 0,
          reservedValue: 0,
        })
      })
      // currencyLedgers.forEach(currencyLedger => {
      //   accounts.push({
      //     id: BigInt(currencyLedger.id),
      //     ledgerAccountType: 'HUB_MULTILATERAL_SETTLEMENT',
      //     currency: currencyLedger.currency,
      //     isActive: true,
      //     changedDate: currencyLedger.changedDate,
      //     createdDate: currencyLedger.createdDate,
      //     value: 0,
      //     reservedValue: 0,
      //   })
      //   accounts.push({
      //     id: BigInt(currencyLedger.id),
      //     ledgerAccountType: 'HUB_RECONCILIATION',
      //     currency: currencyLedger.currency,
      //     isActive: true,
      //     changedDate: currencyLedger.changedDate,
      //     createdDate: currencyLedger.createdDate,
      //     value: 0,
      //     reservedValue: 0,
      //   })
      // })

      return {
        type: 'SUCCESS',
        createdDate,
        accounts: accounts
      }
    } catch (error: any) {
      return {
        type: 'FAILURE',
        error
      }
    }
  }

  public async getDfsp(query: { dfspId: string; }): Promise<QueryResultWithNotFound<LegacyLedgerDfsp>> {
    throw new Error('Method not implemented.');
  }

  public async getAllDfsps(query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>> {
    throw new Error('Method not implemented.');
  }

  public async getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error('Method not implemented.');
  }

  public async getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error('Method not implemented.');
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResultWithNotFound<LegacyLimit>> {
    throw new Error('Method not implemented.');
  }

  public async getNetDebitCaps(query: GetNetDebitCapsQuery): Promise<QueryResultWithNotFound<Array<LegacyLimitItem>>> {
    throw new Error('Method not implemented.');
  }

  public async prepare(inputs: Array<PrepareHandlerInput>): Promise<Array<PaymentPrepareResult>> {
    throw new Error('Method not implemented.');
  }
  public async fulfil(inputs: Array<FulfilHandlerInput>): Promise<Array<PaymentFulfilResult>> {
    throw new Error('Method not implemented.');
  }
  public async sweepTimedOut(now: Date): Promise<SweepResult> {
    throw new Error('Method not implemented.');
  }
  public async lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse> {
    throw new Error('Method not implemented.');
  }
  public async closeSettlementWindow(cmd: SettlementCloseWindowCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }
  public async settlementPrepare(cmd: SettlementPrepareCommand): Promise<CommandResult<{ id: number; }>> {
    throw new Error('Method not implemented.');
  }
  public async settlementAbort(cmd: SettlementAbortCommand): Promise<CommandResult<SettlementUpdateResult>> {
    throw new Error('Method not implemented.');
  }
  public async settlementCommit(cmd: SettlementCommitCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }
  public async settlementUpdate(cmd: SettlementUpdateCommand): Promise<CommandResult<SettlementUpdateResult>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlementWindows(query: GetSettlementWindowsQuery): Promise<QueryResult<GetSettlementWindowsQueryResponse>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlementWindow(query: GetSettlementWindowQuery): Promise<QueryResultWithNotFound<SettlementWindow>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlement(query: GetSettlementQuery): Promise<QueryResultWithNotFound<Settlement>> {
    throw new Error('Method not implemented.');
  }
  public async getSettlements(query: GetSettlementsQuery): Promise<GetSettlementsQueryResponse> {
    throw new Error('Method not implemented.');
  }
}