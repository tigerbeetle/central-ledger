import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import { Enum } from '@mojaloop/central-services-shared';
import assert, { ok } from "assert";
import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler";
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { ApplicationConfig } from "src/shared/config";
import { QueryResult } from 'src/shared/results';
import { Account, AccountFilterFlags, AccountFlags, amount_max, Client, CreateAccountError, CreateTransferError, CreateTransfersError, id, QueryFilter, QueryFilterFlags, Transfer, TransferFlags } from 'tigerbeetle-node';
import { convertBigIntToNumber } from "../../shared/config/util";
import { logger } from '../../shared/logger';
import { CurrencyManager } from './CurrencyManager';
import { Ledger } from "./Ledger";
import { DfspAccountIds, SpecAccount, SpecDfsp, SpecNetDebitCap, SpecStore } from "./SpecStore";
import Helper from './TigerBeetleLedgerHelper';
import { TransferBatcher } from "./TransferBatcher";
import {
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
  FulfilResult,
  FulfilResultType,
  GetAllDfspAccountsQuery,
  GetAllDfspsResponse,
  GetDfspAccountsQuery,
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
  LedgerDfsp,
  LedgerAccount,
  WithdrawAbortCommand,
  WithdrawAbortResponse,
  AccountCode,
  TransferCode,
} from "./types";

const NS_PER_MS = 1_000_000n
const NS_PER_SECOND = NS_PER_MS * 1_000n

/**
 * Internal mapping of TigerBeetle errors 
 */
type FailureResult<T> = CreateTransfersError & {
  type: T
}
type PrepareFailureType = 'FAIL_LIQUIDITY' | 'PAYER_CLOSED' | 'PAYEE_CLOSED' | 'MODIFIED' |
  'EXISTS' | 'UNKNOWN'
type AbortFailureType = 'ALREADY_ABORTED' | 'ALREADY_FULFILLED' | 'NOT_FOUND' | 'UNKNOWN'
type FulfilFailureType = 'ALREADY_ABORTED' | 'PAYER_CLOSED' | 'PAYEE_CLOSED' | 'ALREADY_FULFILLED'
  | 'NOT_FOUND' | 'PAYER_ACCOUNT_CLOSED' | 'PAYEE_ACCOUNT_CLOSED' | 'UNKNOWN'
type WithdrawPrepareFailureType = 'ACCOUNT_CLOSED' | 'TRANSFER_ID_REUSED' | 'INSUFFICIENT_FUNDS' |
  'UNKNOWN'
type WithdrawCommitFailureType = 'NOT_FOUND' | 'UNKNOWN'
type WithdrawAbortFailureType = 'NOT_FOUND' | 'UNKNOWN'
type DepositFailureType = 'EXISTS' | 'MODIFIED' | 'UNKNOWN'
type SetNetDebitCapFailureType = 'UNKNOWN'
type CloseDfspMasterAccountFailureType = 'DEBIT_ACCOUNT_NOT_FOUND' | 'ALREADY_CLOSED' | 'UNKNOWN'
type EnableDfspAccountFailureType = 'ALREADY_ENABLED' | 'UNKNOWN'
type DisableDfspAccountFailureType = 'ALREADY_CLOSED' | 'UNKNOWN'

/**
 * An internal representation of an Account, combined with Spec
 */
interface InternalLedgerAccount extends Account {
  dfspId: string,
  currency: string,
  // Technically we don't need this since it lives on the account.code, but as a number,
  // but explicit typing here makes accessing this property easier.
  accountCode: AccountCode
}

/**
 * Internal representation of the Dfsp/Participant Master account
 */
interface InternalMasterAccount extends Account {
  dfspId: string,
}

export interface TigerBeetleLedgerDependencies {
  config: ApplicationConfig
  client: Client
  specStore: SpecStore
}

export default class TigerBeetleLedger implements Ledger {
  private currencyManager: CurrencyManager

  constructor(private deps: TigerBeetleLedgerDependencies) {
    this.currencyManager = new CurrencyManager(this.deps.config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS)
  }

  // ============================================================================
  // Lifecycle Methods
  // ============================================================================

  /**
   * @method createHubAccount
   * @description Creates an account in the Hub for the provided Currency and Settlement Model
   */
  public async createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    // TODO(LD): I don't know if we need this at the moment, since we won't have common accounts
    // but instead use separate collateral accounts for each Dfsp + Currency combination
    //
    // We will however need to set up:
    // 1. Settlement Models/Configuration
    // 2. Enable certain currencies
    logger.warn('createHubAccount() - noop')

    return {
      type: 'SUCCESS'
    }
  }

  /**
   * @method createDfsp
   * @description Create the accounts for the (Dfsp, Currency). If the Dfsp hasn't been created before
   *   sets up the SpecDfsp
   */
  public async createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse> {
    try {
      assert(cmd.dfspId)
      assert.equal(cmd.currencies.length, 1, 'Currently only 1 currency is supported')
      const currency = cmd.currencies[0]
      this.currencyManager.assertCurrenciesEnabled([currency])

      // Get or create the SpecDfsp
      const masterAccountId = await this._getOrCreateSpecDfsp(cmd.dfspId)

      // Lookup the dfsp first, ensure it's been created
      const accountSpecResult = await this.deps.specStore.getAccountSpec(cmd.dfspId, currency)
      if (accountSpecResult.type === "SpecAccount") {
        const accounts = await this.deps.client.lookupAccounts([
          accountSpecResult.deposit,
          accountSpecResult.unrestricted,
          accountSpecResult.unrestrictedLock,
          accountSpecResult.restricted,
          accountSpecResult.reserved,
          accountSpecResult.commitedOutgoing,
        ]);
        if (accounts.length === 6) {
          return {
            type: 'ALREADY_EXISTS'
          }
        }

        // We have a partial save of accounts, that means spec store and TigerBeetle are out of
        // sync. We simply continue here and allow new accounts to be created in TigerBeetle, and
        // the partial accounts to be ignored in the spec store
        logger.warn(`createDfsp() - found only ${accounts.length} of expected 7 for dfsp: \
          ${cmd.dfspId} and currency: ${currency}. Overwriting old accounts.`)
      }

      const ledgerOperation = this.currencyManager.getLedgerOperation(currency)
      const accountIdSettlementBalance = this.currencyManager.getAccountIdSettlementBalance(currency)
      const accountIds: DfspAccountIds = {
        deposit: Helper.idSmall(),
        unrestricted: Helper.idSmall(),
        unrestrictedLock: Helper.idSmall(),
        restricted: Helper.idSmall(),
        reserved: Helper.idSmall(),
        commitedOutgoing: Helper.idSmall(),
        clearingCredit: Helper.idSmall(),
        clearingSetup: Helper.idSmall(),
        clearingLimit: Helper.idSmall(),
      }

      const accounts: Array<Account> = [
        // Settlement_Balance
        {
          ...Helper.createAccountTemplate,
          id: accountIdSettlementBalance,
          ledger: ledgerOperation,
          code: AccountCode.Settlement_Balance,
          flags: 0,
        },
        // dev/null account
        {
          ...Helper.createAccountTemplate,
          id: Helper.accountIds.devNull,
          ledger: Helper.ledgerIds.globalControl,
          code: AccountCode.Dev_Null,
          flags: 0,
        },
        // Dfsp/Participant account. Keeps track of Dfsp active/not active and creation timestamp
        {
          ...Helper.createAccountTemplate,
          id: masterAccountId,
          ledger: Helper.ledgerIds.globalControl,
          code: AccountCode.Dfsp,
          flags: 0,
        },
        // Deposit
        {
          ...Helper.createAccountTemplate,
          id: accountIds.deposit,
          ledger: ledgerOperation,
          code: AccountCode.Deposit,
          flags: AccountFlags.linked | AccountFlags.credits_must_not_exceed_debits,
        },
        // Unrestricted
        {
          ...Helper.createAccountTemplate,
          id: accountIds.unrestricted,
          ledger: ledgerOperation,
          code: AccountCode.Unrestricted,
          flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
        },
        // Unrestricted_Lock
        {
          ...Helper.createAccountTemplate,
          id: accountIds.unrestrictedLock,
          ledger: ledgerOperation,
          code: AccountCode.Unrestricted_Lock,
          flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
        },
        // Restricted
        {
          ...Helper.createAccountTemplate,
          id: accountIds.restricted,
          ledger: ledgerOperation,
          code: AccountCode.Restricted,
          flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
        },
        // Reserved
        {
          ...Helper.createAccountTemplate,
          id: accountIds.reserved,
          ledger: ledgerOperation,
          code: AccountCode.Reserved,
          flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
        },
        // Committed_Outgoing
        {
          ...Helper.createAccountTemplate,
          id: accountIds.commitedOutgoing,
          ledger: ledgerOperation,
          code: AccountCode.Committed_Outgoing,
          flags: AccountFlags.debits_must_not_exceed_credits,
        },
        // Clearing_Setup
        {
          ...Helper.createAccountTemplate,
          id: accountIds.clearingSetup,
          ledger: ledgerOperation,
          code: AccountCode.Clearing_Setup,
          flags: 0,
        },
        // Clearing_Limit
        {
          ...Helper.createAccountTemplate,
          id: accountIds.clearingLimit,
          ledger: ledgerOperation,
          code: AccountCode.Clearing_Limit,
          flags: AccountFlags.debits_must_not_exceed_credits,
        },
        // Clearing_Credit
        {
          ...Helper.createAccountTemplate,
          id: accountIds.clearingCredit,
          ledger: ledgerOperation,
          code: AccountCode.Clearing_Credit,
          flags: AccountFlags.debits_must_not_exceed_credits,
        },
      ]

      await this.deps.specStore.associateAccounts(cmd.dfspId, currency, accountIds)
      const createAccountsErrors = await this.deps.client.createAccounts(accounts)

      let fatal = false
      const readableErrors = []
      createAccountsErrors.forEach((error, idx) => {
        // Allowable errors
        if (error.index <= 2 && error.result === CreateAccountError.exists) {
          return
        }

        readableErrors.push(CreateAccountError[error.result])
        console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`)
        fatal = true
      })

      if (fatal) {
        // if THIS fails, then we have dangling entries in the database
        await this.deps.specStore.tombstoneAccounts(cmd.dfspId, currency, accountIds)

        return {
          type: 'FAILURE',
          error: new Error(`LedgerError: ${readableErrors.join(',')}`)
        }
      }

      const setNetDebitCapResult = await this.setNetDebitCap({
        netDebitCapType: 'UNLIMITED',
        dfspId: cmd.dfspId,
        currency
      })
      if (setNetDebitCapResult.type === 'FAILURE') {
        logger.error(`Successfully created dfsp, but failed to set the net debit cap with error: ${setNetDebitCapResult.error}`)
        return {
          type: 'FAILURE',
          error: setNetDebitCapResult.error
        }
      }

      return {
        type: 'SUCCESS'
      }
    } catch (err) {
      logger.error(`createDfsp failed with error: ${err.message}`)
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async disableDfsp(cmd: { dfspId: string }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)

    logger.debug(`disableDfsp() - disabling dfsp: ${cmd.dfspId}`)

    const specDfsp = await this.deps.specStore.queryDfsp(cmd.dfspId)
    if (specDfsp.type === 'SpecDfspNone') {
      return {
        type: 'FAILURE',
        error: new Error(`Participant does not exist`)
      }
    }
    let closeAccountResult = await this._closeDfspMasterAccount(specDfsp.accountId)

    switch (closeAccountResult.type) {
      case DeactivateDfspResponseType.SUCCESS:
      case DeactivateDfspResponseType.ALREADY_CLOSED: {
        return {
          type: 'SUCCESS',
          result: undefined
        }
      }
      case DeactivateDfspResponseType.FAILED: {
        return {
          type: 'FAILURE',
          error: closeAccountResult.error
        }
      }
    }

    assert(closeAccountResult.type === DeactivateDfspResponseType.CREATE_ACCOUNT)
    const createAccountsErrors = await this.deps.client.createAccounts([
      {
        ...Helper.createAccountTemplate,
        id: Helper.accountIds.devNull,
        ledger: Helper.ledgerIds.globalControl,
        code: Helper.transferCodes.unknown,
        flags: 0,
      }
    ])
    const fatal = createAccountsErrors.reduce((acc, curr) => {
      if (acc) {
        return acc
      }

      if (curr.result === CreateAccountError.exists ||
        curr.result === CreateAccountError.ok
      ) {
        return false
      }
      return true
    }, false)
    if (fatal) {
      return {
        type: 'FAILURE',
        error: new Error('disableDfsp - failed to create counterparty account.')
      }
    }
    closeAccountResult = await this._closeDfspMasterAccount(specDfsp.accountId)
    switch (closeAccountResult.type) {
      case DeactivateDfspResponseType.SUCCESS:
      case DeactivateDfspResponseType.ALREADY_CLOSED: {
        return {
          type: 'SUCCESS',
          result: undefined
        }
      }
      // We shouldn't see the same CREATE_ACCOUNT error twice!
      case DeactivateDfspResponseType.CREATE_ACCOUNT:
      case DeactivateDfspResponseType.FAILED: {
        return {
          type: 'FAILURE',
          error: new Error('Failed to close the dfsp account after retry.')
        }
      }
    }
  }

  public async enableDfsp(cmd: { dfspId: string }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    logger.debug(`enableDfsp() - enabling dfsp: ${cmd.dfspId}`)

    const specDfsp = await this.deps.specStore.queryDfsp(cmd.dfspId)
    if (specDfsp.type === 'SpecDfspNone') {
      return {
        type: 'FAILURE',
        error: new Error(`Participant does not exist`)
      }
    }
    const transfers = await this.deps.client.getAccountTransfers({
      account_id: specDfsp.accountId,
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
      // This is a success case, as account isn't closed.
      return {
        type: 'SUCCESS',
        result: undefined
      }
    }

    // get the latest pending transfer
    const lastClosingTransferId = transfers[0].id
    const createTransferResults = await this.deps.client.createTransfers([{
      ...Helper.createTransferTemplate,
      id: id(),
      debit_account_id: 0n,
      credit_account_id: 0n,
      pending_id: lastClosingTransferId,
      amount: 0n,
      ledger: Helper.ledgerIds.globalControl,
      code: 100,
      flags: TransferFlags.void_pending_transfer
    }])

    if (createTransferResults.length === 0) {
      return {
        type: 'SUCCESS',
        result: undefined
      }
    }

    assert.equal(createTransferResults.length, 1, 'expected just 1 transferError result')
    const result = createTransferResults[0]
    switch (result.result) {
      case CreateTransferError.ok:
      // Pending closing transfer has already been voided, so the account must be open!
      case CreateTransferError.pending_transfer_not_pending:
      case CreateTransferError.pending_transfer_already_voided:
        return {
          type: 'SUCCESS',
          result: undefined
        }
      default:
        return {
          type: 'FAILURE',
          error: new Error(`enableDfsp failed to void closing transfer with error: ${CreateTransferError[result.result]}`)
        }
    }
  }

  private async _closeDfspMasterAccount(masterAccountId: bigint): Promise<DeactivateDfspResponse> {
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
    const transferErrors = await this.deps.client.createTransfers([closingTransfer])
    const fatalErrors: Array<FailureResult<CloseDfspMasterAccountFailureType>> = []

    transferErrors.forEach(error => {
      if (error.index === 0) {
        switch (error.result) {
          case CreateTransferError.ok:
            return
          case CreateTransferError.debit_account_not_found:
            fatalErrors.push({ type: 'DEBIT_ACCOUNT_NOT_FOUND', ...error })
            return
          case CreateTransferError.credit_account_already_closed:
            fatalErrors.push({ type: 'ALREADY_CLOSED', ...error })
            return
          default:
            fatalErrors.push({ type: 'UNKNOWN', ...error })
            return
        }
      }

      throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
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
          error: new Error(`_closeDfspMasterAccount failed with unexpected error: ${CreateTransferError[firstError.result]}`)
        }
    }
  }

  public async enableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    assert(cmd.accountId)
    const accountId = BigInt(cmd.accountId)

    try {
      logger.debug(`enableDfspAccount() - disabling dfsp: ${cmd.dfspId} accountId: ${cmd.accountId}`)

      // Only the Deposit and Unrestricted Accounts can be enabled/disabled
      const specAccounts = await this.deps.specStore.queryAccounts(cmd.dfspId)
      if (specAccounts.length === 0) {
        return {
          type: 'FAILURE',
          error: new Error(`enableDfspAccount() - dfsp: ${cmd.dfspId} not found.`)
        }
      }

      // Match the accountId to a specific spec, so we can pull out currency
      let spec: SpecAccount
      let accountToClose: AccountCode.Deposit | AccountCode.Unrestricted
      specAccounts.forEach(specAccount => {
        if (specAccount.deposit === accountId) {
          spec = specAccount
          accountToClose = AccountCode.Deposit
          return
        }
        if (specAccount.unrestricted === accountId) {
          spec = specAccount
          accountToClose = AccountCode.Unrestricted
          return
        }
      })
      if (!spec) {
        return {
          type: 'FAILURE',
          error: new Error(`enableDfspAccount() - account id not found, or is not Deposit or Unrestricted.`)
        }
      }

      const ledgerOperation = this.currencyManager.getLedgerOperation(spec.currency)
      // Look up the closing transfer to void it
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
          type: 'SUCCESS',
          result: undefined
        }
      }
      const lastClosingTransfer = closingTransfers[0]
      // Void the closing transfer to reopen this account.
      const voidClosingTransfer: Transfer = {
        ...Helper.createTransferTemplate,
        id: id(),
        pending_id: lastClosingTransfer.id,
        debit_account_id: spec.unrestrictedLock,
        credit_account_id: accountToClose === AccountCode.Deposit ? spec.deposit : spec.unrestricted,
        amount: 0n,
        ledger: ledgerOperation,
        code: TransferCode.Close_Account,
        flags: TransferFlags.void_pending_transfer
      }
      const transferErrors = await this.deps.client.createTransfers([voidClosingTransfer])
      const fatalErrors: Array<FailureResult<EnableDfspAccountFailureType>> = []

      transferErrors.forEach(error => {
        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.pending_transfer_already_voided:
              // Not a failure - the account is already open
              fatalErrors.push({ type: 'ALREADY_ENABLED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        switch (firstError.type) {
          case 'ALREADY_ENABLED':
            // Account already open - treat as success
            return {
              type: 'SUCCESS',
              result: undefined
            }
          case 'UNKNOWN':
            return {
              type: 'FAILURE',
              error: new Error(`enableDfspAccount failed with error: ${CreateTransferError[firstError.result]}`)
            }
        }
      }

      return {
        type: 'SUCCESS',
        result: undefined
      }
    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async disableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>> {
    assert(cmd)
    assert(cmd.dfspId)
    assert(cmd.accountId)
    const accountId = BigInt(cmd.accountId)

    try {
      logger.debug(`disableDfspAccount() - disabling dfsp: ${cmd.dfspId} accountId: ${cmd.accountId}`)

      // Only the Deposit and Unrestricted Accounts can be enabled/disabled
      const specAccounts = await this.deps.specStore.queryAccounts(cmd.dfspId)
      if (specAccounts.length === 0) {
        return {
          type: 'FAILURE',
          error: new Error(`disableDfspAccount() - dfsp: ${cmd.dfspId} not found.`)
        }
      }

      // Match the accountId to a specific spec, so we can pull out currency
      let spec: SpecAccount
      let accountToClose: AccountCode.Unrestricted
      let matchWrongAccount = false
      specAccounts.forEach(specAccount => {
        // Only allow closing the unrestricted account
        if (specAccount.unrestricted === accountId) {
          spec = specAccount
          accountToClose = AccountCode.Unrestricted
          return
        }

        if (specAccount.deposit === accountId) {
          matchWrongAccount = true
          return
        }
      })

      // Tried to close the deposit account (which is mapped to the Settlement account)
      if (matchWrongAccount) {
        return {
          type: 'FAILURE',
          error: new Error(`Only position account update is permitted`)
        }
      }

      if (!spec) {
        return {
          type: 'FAILURE',
          error: new Error(`disableDfspAccount() - account id not found, or is not Deposit or Unrestricted.`)
        }
      }

      const ledgerOperation = this.currencyManager.getLedgerOperation(spec.currency)

      // Create a closing transfer to mark this Account as deactivated
      const closingTransfer: Transfer = {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.unrestrictedLock,
        credit_account_id: spec.unrestricted,
        amount: 0n,
        ledger: ledgerOperation,
        code: TransferCode.Close_Account,
        flags: TransferFlags.closing_credit | TransferFlags.pending,
      }
      const transferErrors = await this.deps.client.createTransfers([closingTransfer])
      const fatalErrors: Array<FailureResult<DisableDfspAccountFailureType>> = []

      transferErrors.forEach(error => {
        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.credit_account_already_closed:
              // Account already closed - treat as success
              fatalErrors.push({ type: 'ALREADY_CLOSED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        switch (firstError.type) {
          case 'ALREADY_CLOSED':
            // Account already closed - treat as success
            return {
              type: 'SUCCESS',
              result: undefined
            }
          case 'UNKNOWN':
            return {
              type: 'FAILURE',
              error: new Error(`disableDfspAccount failed with error: ${CreateTransferError[firstError.result]}`)
            }
        }
      }

      return {
        type: 'SUCCESS',
        result: undefined
      }

    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  public async deposit(cmd: DepositCommand): Promise<DepositResponse> {
    assert(cmd.amount)
    assert(cmd.currency)
    assert(cmd.dfspId)
    assert(cmd.transferId)
    assert(cmd.reason)

    try {
      // Lookup the net debit cap
      const netDebitCap = await this._getNetDebitCapInternal(cmd.currency, cmd.dfspId)
      const spec = await this.deps.specStore.getAccountSpec(cmd.dfspId, cmd.currency)
      if (spec.type === 'SpecAccountNone') {
        throw new Error(`no dfspId found: ${cmd.dfspId}`)
      }
      const ledgerOperation = this.currencyManager.getLedgerOperation(cmd.currency)
      const assetScale = this.currencyManager.getAssetScale(cmd.currency)

      // Save the funding spec before writing to TigerBeetle (write last, read first)
      const saveFundingResult = await this.deps.specStore.saveFundingSpec([{
        transferId: cmd.transferId,
        dfspId: cmd.dfspId,
        currency: cmd.currency,
        action: 'DEPOSIT',
        reason: cmd.reason
      }])

      // Ensure we got exactly one result
      assert(saveFundingResult.length === 1, 'Expected exactly one result from saveFundingSpec')

      // Handle the result of saving funding spec
      if (saveFundingResult[0].type === 'FAILURE') {
        return {
          type: 'FAILURE',
          error: new Error('Failed to save funding specification')
        }
      }

      const idLockTransfer = id()
      let netDebitCapLockAmount = amount_max
      if (netDebitCap.type === 'LIMITED') {
        netDebitCapLockAmount = Helper.toTigerBeetleAmount(netDebitCap.amount, assetScale)
      }
      const transfers: Array<Transfer> = [
        // Deposit funds into Unrestricted
        {
          ...Helper.createTransferTemplate,
          id: Helper.fromMojaloopId(cmd.transferId),
          debit_account_id: spec.deposit,
          credit_account_id: spec.unrestricted,
          amount: Helper.toTigerBeetleAmount(cmd.amount, assetScale),
          ledger: ledgerOperation,
          code: TransferCode.Deposit,
          flags: TransferFlags.linked
        },
        // Sweep total balance from Restricted to Unrestricted
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: spec.restricted,
          credit_account_id: spec.unrestricted,
          amount: amount_max,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Sweep_To_Unrestricted,
          flags: TransferFlags.linked | TransferFlags.balancing_debit
        },
        // Temporarily lock up to the net debit cap.
        {
          ...Helper.createTransferTemplate,
          id: idLockTransfer,
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.unrestrictedLock,
          amount: netDebitCapLockAmount,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Lock,
          flags: TransferFlags.linked | TransferFlags.pending | TransferFlags.balancing_debit
        },
        // Sweep whatever remains in Unrestricted to Restricted.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.restricted,
          amount: amount_max,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Sweep_To_Restricted,
          flags: TransferFlags.linked | TransferFlags.balancing_debit
        },
        // Reset the pending limit transfer.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          pending_id: idLockTransfer,
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: 0n,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Lock,
          flags: TransferFlags.void_pending_transfer
        }
      ]

      const createTransfersResults = await this.deps.client.createTransfers(transfers)
      const fatalErrors: Array<FailureResult<DepositFailureType>> = []

      createTransfersResults.forEach(error => {
        // Ignore noisy errors
        if (error.result === CreateTransferError.linked_event_failed) {
          return
        }

        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.exists:
              fatalErrors.push({ type: 'EXISTS', ...error })
              return
            case CreateTransferError.exists_with_different_flags:
            case CreateTransferError.exists_with_different_pending_id:
            case CreateTransferError.exists_with_different_timeout:
            case CreateTransferError.exists_with_different_debit_account_id:
            case CreateTransferError.exists_with_different_credit_account_id:
            case CreateTransferError.exists_with_different_amount:
            case CreateTransferError.exists_with_different_user_data_128:
            case CreateTransferError.exists_with_different_user_data_64:
            case CreateTransferError.exists_with_different_user_data_32:
            case CreateTransferError.exists_with_different_ledger:
            case CreateTransferError.exists_with_different_code:
              fatalErrors.push({ type: 'MODIFIED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        switch (firstError.type) {
          case 'EXISTS':
            return {
              type: 'ALREADY_EXISTS'
            }
          case 'MODIFIED':
            return {
              type: 'FAILURE',
              error: new Error(`deposit failed - transfer already exists with different parameters`)
            }
          case 'UNKNOWN':
            return {
              type: 'FAILURE',
              error: new Error(`deposit failed with error: ${CreateTransferError[firstError.result]}`)
            }
        }
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

  public async withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse> {
    assert(cmd.transferId)
    assert(cmd.currency)
    assert(cmd.dfspId)
    assert(cmd.amount)
    assert(cmd.reason)

    try {
      const specAccountResult = await this.deps.specStore.getAccountSpec(cmd.dfspId, cmd.currency)
      if (specAccountResult.type === 'SpecAccountNone') {
        throw new Error(`no dfspId found: ${cmd.dfspId}`)
      }
      const spec = specAccountResult
      const ledgerOperation = this.currencyManager.getLedgerOperation(cmd.currency)
      const netDebitCap = await this._getNetDebitCapInternal(cmd.currency, cmd.dfspId)
      const assetScale = this.currencyManager.getAssetScale(cmd.currency)
      const withdrawAmountTigerBeetle = Helper.toTigerBeetleAmount(cmd.amount, assetScale)

      // Save the funding spec before writing to TigerBeetle (write last, read first)
      const saveFundingResult = await this.deps.specStore.saveFundingSpec([{
        transferId: cmd.transferId,
        dfspId: cmd.dfspId,
        currency: cmd.currency,
        action: 'WITHDRAWAL',
        reason: cmd.reason
      }])

      // Ensure we got exactly one result
      assert(saveFundingResult.length === 1, 'Expected exactly one result from saveFundingSpec')

      // Handle the result of saving funding spec
      if (saveFundingResult[0].type === 'FAILURE') {
        return {
          type: 'FAILURE',
          error: new Error('Failed to save funding specification')
        }
      }

      const idLockTransfer = id()
      const transfers: Array<Transfer> = [
        // Sweep total balance from Restricted to Unrestricted
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: spec.restricted,
          credit_account_id: spec.unrestricted,
          amount: amount_max,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Sweep_To_Unrestricted,
          flags: TransferFlags.linked | TransferFlags.balancing_debit
        },
        // Prepare the withdrawal
        {
          ...Helper.createTransferTemplate,
          id: Helper.fromMojaloopId(cmd.transferId),
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.deposit,
          amount: withdrawAmountTigerBeetle,
          ledger: ledgerOperation,
          code: TransferCode.Withdraw,
          flags: TransferFlags.linked | TransferFlags.pending
        },
        // Temporarily lock up to the net debit cap amount.
        {
          ...Helper.createTransferTemplate,
          id: idLockTransfer,
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.unrestrictedLock,
          amount: netDebitCap.type === 'LIMITED' ?
            Helper.toTigerBeetleAmount(netDebitCap.amount, assetScale) : amount_max,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Lock,
          flags: TransferFlags.linked | TransferFlags.balancing_debit | TransferFlags.pending
        },
        // Sweep whatever remains in Unrestricted to Restricted.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.restricted,
          amount: amount_max,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Sweep_To_Restricted,
          flags: TransferFlags.linked | TransferFlags.balancing_debit
        },
        // Void the pending limit lock transfer.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          pending_id: idLockTransfer,
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: 0n,
          ledger: ledgerOperation,
          code: TransferCode.Net_Debit_Cap_Lock,
          flags: TransferFlags.void_pending_transfer
        },
      ]

      const createTransfersResults = await this.deps.client.createTransfers(transfers)
      const fatalErrors: Array<FailureResult<WithdrawPrepareFailureType>> = []

      createTransfersResults.forEach(error => {
        // Ignore noisy errors
        if (error.result === CreateTransferError.linked_event_failed) {
          return
        }

        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.debit_account_already_closed:
            case CreateTransferError.credit_account_already_closed:
              fatalErrors.push({ type: 'ACCOUNT_CLOSED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        if (error.index === 1) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.debit_account_already_closed:
            case CreateTransferError.credit_account_already_closed:
              fatalErrors.push({ type: 'ACCOUNT_CLOSED', ...error })
              return
            case CreateTransferError.exists_with_different_flags:
            case CreateTransferError.exists_with_different_pending_id:
            case CreateTransferError.exists_with_different_timeout:
            case CreateTransferError.exists_with_different_debit_account_id:
            case CreateTransferError.exists_with_different_credit_account_id:
            case CreateTransferError.exists_with_different_amount:
            case CreateTransferError.exists_with_different_user_data_128:
            case CreateTransferError.exists_with_different_user_data_64:
            case CreateTransferError.exists_with_different_user_data_32:
            case CreateTransferError.exists_with_different_ledger:
            case CreateTransferError.exists_with_different_code:
            case CreateTransferError.exists:
            case CreateTransferError.id_already_failed:
              fatalErrors.push({ type: 'TRANSFER_ID_REUSED', ...error })
              return
            case CreateTransferError.exceeds_credits:
            case CreateTransferError.exceeds_debits:
              fatalErrors.push({ type: 'INSUFFICIENT_FUNDS', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        switch (firstError.type) {
          case 'INSUFFICIENT_FUNDS':
            return {
              type: 'INSUFFICIENT_FUNDS',
            }
          case 'ACCOUNT_CLOSED':
            return {
              type: 'FAILURE',
              error: new Error(`Withdrawal failed as one or more accounts is closed.`)
            }
          case 'TRANSFER_ID_REUSED':
            return {
              type: 'FAILURE',
              error: new Error(`Withdrawal failed - transferId has already been used.`)
            }
          case 'UNKNOWN':
            return {
              type: 'FAILURE',
              error: new Error(`Withdrawal failed with error: ${CreateTransferError[firstError.result]}`)
            }
        }
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

  public async withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    assert(cmd.transferId)

    try {
      const transfers: Array<Transfer> = [
        // Commit the withdrawal
        {
          ...Helper.createTransferTemplate,
          id: id(),
          pending_id: Helper.fromMojaloopId(cmd.transferId),
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: amount_max,
          ledger: 0,
          code: TransferCode.Withdraw,
          flags: TransferFlags.post_pending_transfer
        },
      ]

      const createTransfersResult = await this.deps.client.createTransfers(transfers)
      const fatalErrors: Array<FailureResult<WithdrawCommitFailureType>> = []

      createTransfersResult.forEach(error => {
        // Ignore noisy errors
        if (error.result === CreateTransferError.linked_event_failed) {
          return
        }

        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.pending_transfer_not_found:
              fatalErrors.push({ type: 'NOT_FOUND', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        switch (firstError.type) {
          case 'NOT_FOUND':
            return {
              type: 'FAILURE',
              error: new Error(`transferId: ${cmd.transferId} not found`)
            }
          case 'UNKNOWN':
            return {
              type: 'FAILURE',
              error: new Error(`withdrawCommit() failed with error: ${CreateTransferError[firstError.result]}`)
            }
        }
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

  public async withdrawAbort(cmd: WithdrawAbortCommand): Promise<WithdrawAbortResponse> {
    assert(cmd.transferId)

    try {
      const transfers: Array<Transfer> = [
        // Abort the withdrawal
        {
          ...Helper.createTransferTemplate,
          id: id(),
          pending_id: Helper.fromMojaloopId(cmd.transferId),
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: 0n,
          ledger: 0,
          code: TransferCode.Withdraw,
          flags: TransferFlags.void_pending_transfer
        },
      ]

      const createTransfersResult = await this.deps.client.createTransfers(transfers)
      const fatalErrors: Array<FailureResult<WithdrawAbortFailureType>> = []

      createTransfersResult.forEach(error => {
        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.pending_transfer_not_found:
              fatalErrors.push({ type: 'NOT_FOUND', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        switch (firstError.type) {
          case 'NOT_FOUND':
            return {
              type: 'FAILURE',
              error: new Error(`transferId: ${cmd.transferId} not found`)
            }
          case 'UNKNOWN':
            return {
              type: 'FAILURE',
              error: new Error(`withdrawAbort() failed with error: ${CreateTransferError[firstError.result]}`)
            }
        }
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

  public async setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    assert(cmd.currency)
    assert(cmd.dfspId)

    let amountNetDebitCap: bigint
    switch (cmd.netDebitCapType) {
      case 'LIMITED': {
        assert(cmd.amount)
        assert(cmd.amount >= 0, 'expected amount to 0 or a positive integer')
        const assetScale = this.currencyManager.getAssetScale(cmd.currency)
        amountNetDebitCap = Helper.toTigerBeetleAmount(cmd.amount, assetScale)
        break;
      }
      case 'UNLIMITED': {
        amountNetDebitCap = 0n
        break;
      }
    }
    const specAccountResult = await this.deps.specStore.getAccountSpec(cmd.dfspId, cmd.currency)
    if (specAccountResult.type === 'SpecAccountNone') {
      throw new Error(`no dfspId + currency found: ${cmd.dfspId} + ${cmd.currency}`)
    }
    const spec = specAccountResult
    const ledgerOperation = this.currencyManager.getLedgerOperation(cmd.currency)

    // Write Last, Read First
    const saveSpecNetDebitCapResults = await this.deps.specStore.saveSpecNetDebitCaps([{
      type: cmd.netDebitCapType,
      amount: cmd.netDebitCapType === 'UNLIMITED' ? undefined : cmd.amount,
      dfspId: cmd.dfspId,
      currency: cmd.currency
    }])
    assert(saveSpecNetDebitCapResults.length === 1)
    const saveSpecNetDebitCapResult = saveSpecNetDebitCapResults[0]
    if (saveSpecNetDebitCapResult.type === 'FAILURE') {
      logger.error(`setNetDebitCap() - saveSpecNetDebitCaps() failed with error: \
        ${saveSpecNetDebitCapResult.error.message}`
      )
      return {
        type: 'FAILURE',
        error: saveSpecNetDebitCapResult.error
      }
    }

    const idLockTransfer = id()
    const transfers: Array<Transfer> = [
      // Sweep total balance from Restricted to Unrestricted
      {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.restricted,
        credit_account_id: spec.unrestricted,
        amount: amount_max,
        ledger: ledgerOperation,
        code: TransferCode.Net_Debit_Cap_Sweep_To_Unrestricted,
        flags: TransferFlags.linked | TransferFlags.balancing_debit
      },
      // Move the new NDC amount out to a temporary account.
      // if the new NDC amount is unlimited, then move all into temporary account.
      {
        ...Helper.createTransferTemplate,
        id: idLockTransfer,
        debit_account_id: spec.unrestricted,
        credit_account_id: spec.unrestrictedLock,
        amount: cmd.netDebitCapType === 'LIMITED' ? amountNetDebitCap : amount_max,
        ledger: ledgerOperation,
        code: TransferCode.Net_Debit_Cap_Lock,
        flags: TransferFlags.linked | TransferFlags.balancing_debit | TransferFlags.pending
      },
      // Sweep whatever remains in Unrestricted to Restricted.
      {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.unrestricted,
        credit_account_id: spec.restricted,
        amount: amount_max,
        ledger: ledgerOperation,
        code: TransferCode.Net_Debit_Cap_Sweep_To_Restricted,
        flags: TransferFlags.linked | TransferFlags.balancing_debit
      },
      // Void the pending limit lock transfer.
      {
        ...Helper.createTransferTemplate,
        id: id(),
        pending_id: idLockTransfer,
        debit_account_id: 0n,
        credit_account_id: 0n,
        amount: 0n,
        ledger: ledgerOperation,
        code: TransferCode.Net_Debit_Cap_Lock,
        flags: TransferFlags.void_pending_transfer
      },
    ]
    const createTransfersResults = await this.deps.client.createTransfers(transfers)
    const fatalErrors: Array<FailureResult<SetNetDebitCapFailureType>> = []

    createTransfersResults.forEach(error => {
      // Ignore noisy errors
      if (error.result === CreateTransferError.linked_event_failed) {
        return
      }

      if (error.index === 0) {
        switch (error.result) {
          case CreateTransferError.ok:
            return
          default:
            fatalErrors.push({ type: 'UNKNOWN', ...error })
            return
        }
      }

      throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
    })

    if (fatalErrors.length > 0) {
      const firstError = fatalErrors[0]
      return {
        type: 'FAILURE',
        error: new Error(`setNetDebitCap() failed with error: ${CreateTransferError[firstError.result]}`)
      }
    }

    return {
      type: 'SUCCESS',
      result: undefined
    }
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResult<LegacyLimit>> {
    assert(query.currency)
    assert(query.dfspId)


    try {
      const getSpecNetDebitCapsResult = await this.deps.specStore.getSpecNetDebitCaps([{
        dfspId: query.dfspId,
        currency: query.currency
      }])
      assert(getSpecNetDebitCapsResult.length === 1, 'Expected exactly 1 SpecNetDebitCapResult')
      const specNetDebitCapResult = getSpecNetDebitCapsResult[0]
      if (specNetDebitCapResult.type === 'FAILURE') {
        logger.error(`getNetDebitCap() - failed with error: \
          ${specNetDebitCapResult.error}`)

        return {
          type: 'FAILURE',
          error: specNetDebitCapResult.error
        }
      }
      const netDebitCap = specNetDebitCapResult.result

      // TigerBeetleLedger sees this internally as an Unlimited limit, but to match the Admin API
      // we consider it a missing limit.
      if (netDebitCap.type === 'UNLIMITED') {
        return {
          type: 'FAILURE',
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
            `getNetDebitCap() - no limits found for dfspId: ${query.dfspId}, currency: ${query.currency}, type: 'NET_DEBIT_CAP`
          )
        }
      }
      const limit: LegacyLimit = {
        type: "NET_DEBIT_CAP",
        value: netDebitCap.amount,
        alarmPercentage: 10
      }

      return {
        type: 'SUCCESS',
        result: limit
      }
    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  /**
   * Helper function to get one and only one net debit cap
   */
  private async _getNetDebitCapInternal(currency: string, dfspId: string): Promise<SpecNetDebitCap> {
    const getSpecNetDebitCapsResult = await this.deps.specStore.getSpecNetDebitCaps([{
      dfspId,
      currency,
    }])
    assert(getSpecNetDebitCapsResult.length === 1, 'Expected exactly 1 SpecNetDebitCapResult')
    const specNetDebitCapResult = getSpecNetDebitCapsResult[0]
    if (specNetDebitCapResult.type === 'FAILURE') {
      logger.error(`_getNetDebitCapInternal2() - failed with error: ${specNetDebitCapResult.error}`)
      throw specNetDebitCapResult.error
    }

    return specNetDebitCapResult.result
  }

  public async getDfsp(query: { dfspId: string; }): Promise<QueryResult<LegacyLedgerDfsp>> {
    try {
      const specDfsp = await this.deps.specStore.queryDfsp(query.dfspId)
      const specAccounts = await this.deps.specStore.queryAccounts(query.dfspId)
      if (specAccounts.length === 0 || specDfsp.type === 'SpecDfspNone') {
        return {
          type: 'FAILURE',
          error: new Error(`Dfsp not found for dfspId: ${query.dfspId}`)
        }
      }

      const masterAccount = (await this._internalAccountsForSpecDfsps([specDfsp]))[0]
      const internalLedgerAccounts = await this._internalAccountsForSpecAccounts(specAccounts)

      // Group by currency and convert to legacy accounts
      const internalLedgerAccountsPerCurrency = internalLedgerAccounts.reduce((acc, ila) => {
        (acc[ila.currency] = acc[ila.currency] || []).push(ila);
        return acc;
      }, {} as Record<string, Array<InternalLedgerAccount>>);

      const legacyLedgerAccounts = Object.values(internalLedgerAccountsPerCurrency)
        .flatMap(accounts => this._fromInternalAccountsToLegacyLedgerAccounts(accounts))

      const ledgerDfsp: LegacyLedgerDfsp = {
        name: query.dfspId,
        isActive: !(masterAccount.flags & AccountFlags.closed),
        created: new Date(Number(masterAccount.timestamp / NS_PER_MS)),
        accounts: legacyLedgerAccounts
      }

      return {
        type: 'SUCCESS',
        result: ledgerDfsp
      }
    } catch (error) {
      return {
        type: 'FAILURE',
        error
      }
    }
  }

  /**
   * TigerBeetleLedger native impementation of getDfsp
   */
  public async getDfspV2(query: { dfspId: string }): Promise<QueryResult<LedgerDfsp>> {
    try {
      const specDfsp = await this.deps.specStore.queryDfsp(query.dfspId)
      const specAccounts = await this.deps.specStore.queryAccounts(query.dfspId)
      if (specAccounts.length === 0 || specDfsp.type === 'SpecDfspNone') {
        return {
          type: 'FAILURE',
          error: new Error(`Dfsp not found for dfspId: ${query.dfspId}`)
        }
      }

      const masterAccount = (await this._internalAccountsForSpecDfsps([specDfsp]))[0]
      const internalLedgerAccounts = await this._internalAccountsForSpecAccounts(specAccounts)

      // Group by currency and convert to legacy accounts
      const internalLedgerAccountsPerCurrency = internalLedgerAccounts.reduce((acc, ila) => {
        (acc[ila.currency] = acc[ila.currency] || []).push(ila);
        return acc;
      }, {} as Record<string, Array<InternalLedgerAccount>>);

      const ledgerAccounts = Object.values(internalLedgerAccountsPerCurrency)
        .flatMap(accounts => this._fromInternalAccountsToLedgerAccounts(accounts))

      const ledgerDfsp: LedgerDfsp = {
        name: query.dfspId,
        status: (masterAccount.flags & AccountFlags.closed) && 'DISABLED' || 'ENABLED',
        created: new Date(Number(masterAccount.timestamp / NS_PER_MS)),
        accounts: ledgerAccounts
      }

      return {
        type: 'SUCCESS',
        result: ledgerDfsp
      }
    } catch (error) {
      return {
        type: 'FAILURE',
        error
      }
    }

  }

  public async getAllDfsps(_query: AnyQuery): Promise<QueryResult<GetAllDfspsResponse>> {
    try {
      const specDfsps = await this.deps.specStore.queryDfspsAll()
      const masterAccounts = await this._internalAccountsForSpecDfsps(specDfsps)
      const masterAccountsPerDfsp = masterAccounts.reduce((acc, masterAccount) => {
        acc[masterAccount.dfspId] = masterAccount
        return acc
      }, {} as Record<string, InternalMasterAccount>)

      const specAccounts = await this.deps.specStore.queryAccountsAll()
      const internalLedgerAccounts = await this._internalAccountsForSpecAccounts(specAccounts)

      // Group by dfspId and currency
      const internalLedgerAccountsPerDfsp = internalLedgerAccounts.reduce((acc, ila) => {
        const dfspAccounts = acc[ila.dfspId] = acc[ila.dfspId] || {};
        (dfspAccounts[ila.currency] = dfspAccounts[ila.currency] || []).push(ila);
        return acc;
      }, {} as Record<string, Record<string, Array<InternalLedgerAccount>>>);

      // we should have exactly the same number of master accounts as internalLedgerAccountsPerDfsp
      assert.equal(
        Object.keys(masterAccountsPerDfsp).length,
        Object.keys(internalLedgerAccountsPerDfsp).length,
        'Expected the same number of dfsps in `masterAccountsPerDfsp` as `internalLedgerAccountsPerDfsp`'
      )

      const dfsps = Object.entries(internalLedgerAccountsPerDfsp).map(([dfspId, dfspAccountMap]) => {
        const masterAccount = masterAccountsPerDfsp[dfspId]
        assert(masterAccount)
        const dfspLegacyAccounts = Object.values(dfspAccountMap)
          .flatMap(currencyAccounts => this._fromInternalAccountsToLegacyLedgerAccounts(currencyAccounts));

        return {
          name: dfspId,
          // TODO(LD): verify!
          isActive: !(masterAccount.flags & AccountFlags.closed),
          created: new Date(Number(masterAccount.timestamp / NS_PER_MS)),
          accounts: dfspLegacyAccounts
        } as LegacyLedgerDfsp;
      })

      return {
        type: 'SUCCESS',
        result: {
          dfsps
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
   * @method getDfspAccounts
   * @description Lookup the accounts for a Dfsp + Currency
   */
  public async getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse> {
    try {
      // Get all the spec accounts
      const specDfsp = await this.deps.specStore.queryDfsp(query.dfspId)
      const allSpecAccounts = await this.deps.specStore.queryAccounts(query.dfspId)

      // Filter accounts by currency
      const specAccounts = allSpecAccounts.filter(acc => acc.currency === query.currency)

      if (specAccounts.length === 0 || specDfsp.type === 'SpecDfspNone') {
        return {
          type: 'FAILURE',
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
            `Accounts not found for dfspId: ${query.dfspId}, currency: ${query.currency}`
          )
        }
      }

      // Get the TigerBeetle Accounts
      const internalLedgerAccounts = await this._internalAccountsForSpecAccounts(specAccounts)

      // Map to legacy view
      const legacyLedgerAccounts = this._fromInternalAccountsToLegacyLedgerAccounts(internalLedgerAccounts)
      return {
        type: 'SUCCESS',
        accounts: legacyLedgerAccounts
      }
    } catch (error) {
      return {
        type: 'FAILURE',
        error: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
          `Failed to get accounts: ${error.message}`
        )
      }
    }
  }

  /**
   * @method getAllDfspAccounts
   * @description Lookup the accounts for a Dfsp, across all currencies
   */
  public async getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse> {
    try {
      // Get all the spec accounts for the DFSP (across all currencies)
      const specDfsp = await this.deps.specStore.queryDfsp(query.dfspId)
      const specAccounts = await this.deps.specStore.queryAccounts(query.dfspId)

      if (specAccounts.length === 0 || specDfsp.type === 'SpecDfspNone') {
        return {
          type: 'FAILURE',
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
            `Accounts not found for dfspId: ${query.dfspId}`
          )
        }
      }

      // Group spec accounts by currency
      const accountsByCurrency = specAccounts.reduce((acc, specAccount) => {
        if (!acc[specAccount.currency]) {
          acc[specAccount.currency] = []
        }
        acc[specAccount.currency].push(specAccount)
        return acc
      }, {} as Record<string, Array<SpecAccount>>)

      // Process each currency separately and flatten results
      const allLegacyLedgerAccounts: Array<LegacyLedgerAccount> = []

      for (const currency of Object.keys(accountsByCurrency)) {
        const currencySpecAccounts = accountsByCurrency[currency]

        // Get the TigerBeetle Accounts for this currency
        const internalLedgerAccounts = await this._internalAccountsForSpecAccounts(currencySpecAccounts)

        // Map to legacy view (only handles one currency at a time)
        const legacyLedgerAccounts = this._fromInternalAccountsToLegacyLedgerAccounts(internalLedgerAccounts)

        allLegacyLedgerAccounts.push(...legacyLedgerAccounts)
      }

      return {
        type: 'SUCCESS',
        accounts: allLegacyLedgerAccounts
      }
    } catch (error) {
      return {
        type: 'FAILURE',
        error: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
          `Failed to get all accounts: ${error.message}`
        )
      }
    }
  }

  /**
   * @method getHubAccounts
   * 
   * @description There is no concept of a 'Hub Account' in the TigerBeetle implementation, but to keep backwards
   *   compatbility, we return mock Hub accounts based on the currencies enabled in the 
   *   `TIGERBEETLE.CURRENCY_LEDGERS` config parameter.
   */
  public async getHubAccounts(query: AnyQuery): Promise<HubAccountResponse> {
    const currencyLedgers = this.deps.config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS

    assert(currencyLedgers.length > 0, 'Expected at least one currency to be defined')

    const accounts: Array<LegacyLedgerAccount> = []
    currencyLedgers.forEach(currencyLedger => {
      accounts.push({
        id: 0n,
        ledgerAccountType: 'HUB_MULTILATERAL_SETTLEMENT',
        currency: currencyLedger.currency,
        isActive: true,
        value: 0,
        reservedValue: 0,
        changedDate: new Date(0)
      })
      accounts.push({
        id: 0n,
        ledgerAccountType: 'HUB_RECONCILIATION',
        currency: currencyLedger.currency,
        isActive: true,
        value: 0,
        reservedValue: 0,
        changedDate: new Date(0)
      })
    })

    return {
      type: 'SUCCESS',
      accounts: accounts
    }
  }

  /**
   * Lookup the SpecDfsp for this dfspId or create a new one
   */
  private async _getOrCreateSpecDfsp(dfspId: string): Promise<bigint> {
    const result = await this.deps.specStore.queryDfsp(dfspId)
    if (result.type === 'SpecDfsp') {
      return result.accountId
    }

    const accountId = Helper.idSmall()
    await this.deps.specStore.associateDfsp(dfspId, accountId)
    return accountId
  }

  private async _internalAccountsForSpecDfsps(specDfsps: Array<SpecDfsp>): Promise<Array<InternalMasterAccount>> {
    const dfspIdMap: Record<string, null> = {}
    const accountKeys: Array<string> = []

    const accountIds = specDfsps.map(spec => spec.accountId)
    const accountResult = await Helper.safeLookupAccounts(this.deps.client, accountIds)
    if (accountResult.type === 'FAILURE') {
      logger.error(`_internalAccountsForSpecDfsps() - failed with error: ${accountResult.error.message}`)
      throw accountResult.error
    }

    return accountResult.result.map((account, idx) => {
      const spec = specDfsps[idx]
      assert(spec)

      return {
        ...account,
        dfspId: spec.dfspId
      }
    })
  }

  private async _internalAccountsForSpecAccounts(specAccounts: Array<SpecAccount>): Promise<Array<InternalLedgerAccount>> {
    // flat map
    const buildKey = (dfspId: string, currency: string, code: AccountCode) => `${dfspId};${currency};${code}`
    const dfspIdMap: Record<string, null> = {}
    const accountKeys: Array<string> = []
    const accountIds: Array<bigint> = []

    specAccounts.forEach(specAccount => {
      dfspIdMap[specAccount.dfspId] = null
      const keys = [
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Deposit),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Unrestricted),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Clearing_Credit),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Restricted),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Reserved),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Committed_Outgoing),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Unrestricted_Lock),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Clearing_Setup),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Clearing_Limit),
      ]
      const ids = [
        specAccount.deposit,
        specAccount.unrestricted,
        specAccount.clearingCredit,
        specAccount.restricted,
        specAccount.reserved,
        specAccount.commitedOutgoing,
        specAccount.unrestrictedLock,
        specAccount.clearingSetup,
        specAccount.clearingLimit,
      ]

      accountKeys.push(...keys)
      accountIds.push(...ids)
    })
    const dfspIds = Object.keys(dfspIdMap)
    logger.debug(`_internalAccountsForSpecAccounts() - found: ${dfspIds.length} unique dfsps.`)

    assert(accountIds.length < 8000, 'Exceeded maximum number of accounts.')

    // Look up TigerBeetle Accounts
    const accountResult = await Helper.safeLookupAccounts(this.deps.client, accountIds)
    if (accountResult.type === 'FAILURE') {
      logger.error(`_internalAccountsForSpecAccounts() - failed with error: ${accountResult.error.message}`)
      throw accountResult.error
    }

    const internalLedgerAccounts: Array<InternalLedgerAccount> = []
    for (let idx = 0; idx < accountResult.result.length; idx++) {
      const key = accountKeys[idx]
      const [dfspId, currency, accountCodeStr] = key.split(';')
      assert(dfspId)
      assert(currency)
      assert(accountCodeStr)
      const accountCode = parseInt(accountCodeStr) as AccountCode
      const tigerbeetleAccount = accountResult.result[idx]

      internalLedgerAccounts.push({
        dfspId,
        currency,
        accountCode,
        ...tigerbeetleAccount
      })
    }

    return internalLedgerAccounts
  }

  /**
   * @description Map from an internal TigerBeetle Ledger representation of a LedgerAccount to a
   *   backwards compatible representation
   */
  private _fromInternalAccountsToLegacyLedgerAccounts(input: Array<InternalLedgerAccount>):
    Array<LegacyLedgerAccount> {
    const accounts: Array<LegacyLedgerAccount> = []
    const currencies = [...new Set(input.map(item => item.currency))]
    input.map(internalAccount => internalAccount.currency)
    assert.equal(currencies.length, 1, '_fromInternalAccountsToLegacyLedgerAccounts expects accounts of only 1 currency at a time.')
    const currency = currencies[0]
    const assetScale = this.currencyManager.getAssetScale(currency)

    const accountUnrestricted: InternalLedgerAccount = input.find(acc => acc.accountCode === AccountCode.Unrestricted)
    assert(accountUnrestricted, 'could not find unrestricted account')

    const accountRestricted: InternalLedgerAccount = input.find(acc => acc.accountCode === AccountCode.Restricted)
    assert(accountRestricted, 'could not find restricted account')

    const accountDeposit: InternalLedgerAccount = input.find(acc => acc.accountCode === AccountCode.Deposit)
    assert(accountDeposit, 'could not find deposit account')

    // Legacy Settlement Balance: How much Dfsp has available to settle.
    // Was a negative number in the legacy API once the dfsp had deposited funds.
    const legacySettlementBalancePosted = (accountDeposit.debits_posted - accountDeposit.credits_posted) * -1n
    // TODO(LD): This doesn't make any more sense, since we won't use pending/posted
    const legacySettlementBalancePending = (accountDeposit.debits_pending - accountDeposit.credits_pending) * -1n

    // Legacy Position Balance: How much Dfsp is owed or how much this Dfsp owes.
    const clearingBalancePosted = accountUnrestricted.credits_posted - accountUnrestricted.debits_posted
      + accountRestricted.credits_posted - accountRestricted.debits_posted

    // instead this should be the net credit balance of the Reserved account
    const clearingBalancePending = accountUnrestricted.credits_pending - accountUnrestricted.debits_pending
    const legacyPositionBalancePosted = (legacySettlementBalancePosted + clearingBalancePosted) * BigInt(-1)
    const legacyPositionBalancePending = (legacySettlementBalancePending + clearingBalancePending) * BigInt(-1)


    // Funds withdrawal internally uses Pending/Posted, but doesn't expose this in the API
    const settlementValue = -1n * (accountDeposit.debits_posted - accountDeposit.credits_pending - accountDeposit.credits_posted)
    // I'm pretty sure this should always be 0
    const settlementReservedValue = 0

    accounts.push({
      id: accountUnrestricted.id,
      ledgerAccountType: 'POSITION',
      currency,
      isActive: !(accountUnrestricted.flags & AccountFlags.closed),
      value: Helper.toRealAmount(legacyPositionBalancePosted, assetScale),
      reservedValue: Helper.toRealAmount(legacyPositionBalancePending, assetScale),
      // value: convertBigIntToNumber(legacyPositionBalancePosted) / valueDivisor,
      // reservedValue: convertBigIntToNumber(legacyPositionBalancePending) / valueDivisor,
      // We don't have this in TigerBeetle, although we could use the created date
      changedDate: new Date(0)
    })

    accounts.push({
      id: accountDeposit.id,
      ledgerAccountType: 'SETTLEMENT',
      currency,
      isActive: !(accountDeposit.flags & AccountFlags.closed),
      value: Helper.toRealAmount(settlementValue, assetScale),
      reservedValue: settlementReservedValue,

      // value: convertBigIntToNumber(legacySettlementBalancePosted) / valueDivisor,
      // reservedValue: convertBigIntToNumber(legacySettlementBalancePending) / valueDivisor,
      // We don't have this in TigerBeetle, although we could use the created date
      changedDate: new Date(0)
    })

    return accounts;
  }

  /**
   * Maps from an internal TigerBeetle account to a LedgerAccount
   */
  private _fromInternalAccountsToLedgerAccounts(input: Array<InternalLedgerAccount>): Array<LedgerAccount> {
    return input.map(acc => {
      const assetScale = this.currencyManager.getAssetScale(acc.currency)
      const realCreditsPending = Helper.toRealAmount(acc.credits_pending, assetScale)
      const realDebitsPending = Helper.toRealAmount(acc.debits_pending, assetScale)
      const realCreditsPosted = Helper.toRealAmount(acc.credits_posted, assetScale)
      const realDebitsPosted = Helper.toRealAmount(acc.debits_posted, assetScale)

      const ledgerAccount: LedgerAccount = {
        id: acc.id,
        code: acc.code,
        currency: acc.currency,
        status: (acc.flags & AccountFlags.closed) && 'DISABLED' || 'ENABLED',
        realCreditsPending: realCreditsPending,
        realDebitsPending: realDebitsPending,
        realCreditsPosted: realCreditsPosted,
        realDebitsPosted: realDebitsPosted,
      }

      return ledgerAccount
    })

  }

  // ============================================================================
  // Clearing Methods
  // ============================================================================

  public async prepare(input: FusedPrepareHandlerInput): Promise<PrepareResult> {
    logger.debug('TigerBeetleLedger.prepare()')
    try {
      if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
        return {
          type: PrepareResultType.PASS
        }
      }
      // shortcuts
      const amountStr = input.payload.amount.amount
      const currency = input.payload.amount.currency
      const payer = input.payload.payerFsp
      const payee = input.payload.payeeFsp

      // Lookup the Dfsp and Account Specs

      // TODO(LD): switch the interface to array based!
      const dfspSpecPayer = await this.deps.specStore.queryDfsp(payer)
      const dfspSpecPayee = await this.deps.specStore.queryDfsp(payee)
      const accountSpecPayer = await this.deps.specStore.getAccountSpec(payer, currency)
      const accountSpecPayee = await this.deps.specStore.getAccountSpec(payee, currency)
      if (dfspSpecPayer.type === 'SpecDfspNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payer fsp: ${payer} not found`
          ),
        }
      }
      if (dfspSpecPayee.type === 'SpecDfspNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payee fsp: ${payee} not found`
          ),
        }
      }
      if (accountSpecPayer.type === 'SpecAccountNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payer fsp: ${payer} not found`
          ),
        }
      }
      if (accountSpecPayee.type === 'SpecAccountNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payee fsp: ${payee} not found`
          ),
        }
      }

      const prepareId = Helper.fromMojaloopId(input.payload.transferId)
      const ledgerOperation = this.currencyManager.getLedgerOperation(currency)
      const assetScale = this.currencyManager.getAssetScale(currency)
      const amountTigerBeetle = Helper.fromMojaloopAmount(amountStr, assetScale)

      const nowMs = (new Date()).getTime()
      /**
       * In future versions of the FSPIOP API, expiration will be defined in relative seconds,
       * instead of absolute timestamps. That will make the below timeout calculations less error
       * prone.
       */
      const expirationMs = Date.parse(input.payload.expiration)
      if (isNaN(expirationMs)) {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `invalid transfer expiration`
          ),
        }
      }

      if (nowMs > expirationMs) {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `expiration date already in the past`
          ),
        }
      }

      /**
       * Write Last, Read First Rule
       * Write data dependencies first, then write to TigerBeetle
       * Reference: https://tigerbeetle.com/blog/2025-11-06-the-write-last-read-first-rule/
       */
      await this.deps.specStore.saveTransferSpec([
        {
          id: input.payload.transferId,
          amount: amountStr,
          currency: currency,
          payerId: payer,
          payeeId: payee,
          condition: input.payload.condition,
          ilpPacket: input.payload.ilpPacket
        }
      ])

      // Hash key properties of the transfer for idempotency/modification detection
      const transferHash = Helper.hashTransferProperties({
        amount: amountStr,
        currency: currency,
        expiration: input.payload.expiration,
        payeeFsp: payee,
        payerFsp: payer,
        condition: input.payload.condition,
        ilpPacket: input.payload.ilpPacket
      })

      const transfers: Array<Transfer> = [
        // Ensure both Participants are active
        {
          ...Helper.createTransferTemplate,
          id: prepareId,
          debit_account_id: dfspSpecPayer.accountId,
          credit_account_id: dfspSpecPayee.accountId,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          user_data_64: transferHash,
          ledger: Helper.ledgerIds.globalControl,
          code: TransferCode.Clearing_Active_Check,
          flags: TransferFlags.linked | TransferFlags.pending,
        },
        // Setup the limit account for this payment.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: accountSpecPayer.clearingSetup,
          credit_account_id: accountSpecPayer.clearingLimit,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: 1,
          flags: TransferFlags.linked
        },
        // Reserve funds for Participant A from Clearing Credit.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: accountSpecPayer.clearingCredit,
          credit_account_id: accountSpecPayer.clearingSetup,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Reserve,
          flags: TransferFlags.linked | TransferFlags.balancing_debit
            | TransferFlags.balancing_credit
        },
        // Reserve funds for Participant A from Unrestricted
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: accountSpecPayer.unrestricted,
          credit_account_id: accountSpecPayer.clearingSetup,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Reserve,
          flags: TransferFlags.linked | TransferFlags.balancing_debit
            | TransferFlags.balancing_credit
        },
        // Reserve funds for Participant A from Clearing_Setup
        {
          ...Helper.createTransferTemplate,
          id: prepareId + 3n,
          debit_account_id: accountSpecPayer.clearingSetup,
          credit_account_id: accountSpecPayer.reserved,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Reserve,
          flags: TransferFlags.linked
        },
        // ??
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: accountSpecPayer.clearingLimit,
          credit_account_id: accountSpecPayer.clearingSetup,
          amount: amount_max,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: 1,
          flags: TransferFlags.balancing_credit
        },
      ]

      if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
        return {
          type: PrepareResultType.PASS
        }
      }

      const fatalErrors: Array<FailureResult<PrepareFailureType>> = []
      const createTransferErrors = await this.deps.client.createTransfers(transfers)
      createTransferErrors.forEach(error => {
        // Ignore noisy errors
        if (error.result === CreateTransferError.linked_event_failed) {
          return
        }

        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.exists: {
              fatalErrors.push({ type: 'EXISTS', ...error })
              return
            }
            case CreateTransferError.exists_with_different_amount:
            case CreateTransferError.exists_with_different_debit_account_id:
            case CreateTransferError.exists_with_different_credit_account_id:
            case CreateTransferError.exists_with_different_user_data_64: {
              fatalErrors.push({ type: 'MODIFIED', ...error })
              return
            }
            case CreateTransferError.debit_account_already_closed:
              fatalErrors.push({ type: 'PAYER_CLOSED', ...error })
              return
            case CreateTransferError.credit_account_already_closed:
              fatalErrors.push({ type: 'PAYEE_CLOSED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        // TODO(LD): need to figure out how the errors changed:
        // specifically idempotency check (need to reinstate derivation from Mojaloop id)
        if (error.index === 3) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            // Collapse the DFSP deactivated and DFSP account deactivated into the same error
            case CreateTransferError.debit_account_already_closed:
              fatalErrors.push({ type: 'PAYER_CLOSED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        if (error.index === 5) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.exceeds_credits: {
              fatalErrors.push({ type: 'FAIL_LIQUIDITY', ...error })
              return
            }
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }

        throw new Error(`unhandled transfer error: ${error.index}, ${CreateTransferError[error.result]}`)
      })

      if (fatalErrors.length === 0) {
        return {
          type: PrepareResultType.PASS
        }
      }

      // Handle just the first error
      const firstError = fatalErrors[0]
      switch (firstError.type) {
        case 'FAIL_LIQUIDITY': {
          return {
            type: PrepareResultType.FAIL_LIQUIDITY,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.PAYER_FSP_INSUFFICIENT_LIQUIDITY
            )
          }
        }
        case 'PAYER_CLOSED':
          return {
            type: PrepareResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
              `payer is not active`
            )
          }
        case 'PAYEE_CLOSED':
          return {
            type: PrepareResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
              `payee is not active`
            )
          }
        case 'EXISTS': {
          // handled below
          break;
        }
        case 'MODIFIED':
          return {
            type: PrepareResultType.MODIFIED
          }
        case 'UNKNOWN':
          return {
            type: PrepareResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
              `prepare failed with unknown error: ${CreateTransferError[firstError.result]}`
            )
          }
      }

      assert(firstError.type === 'EXISTS')
      const lookupTransferResult = await this.lookupTransfer({
        transferId: input.payload.transferId
      })

      switch (lookupTransferResult.type) {
        case LookupTransferResultType.FOUND_NON_FINAL: {
          return {
            type: PrepareResultType.DUPLICATE_NON_FINAL
          }
        }
        case LookupTransferResultType.FOUND_FINAL: {
          return {
            type: PrepareResultType.DUPLICATE_FINAL,
            finalizedTransfer: lookupTransferResult.finalizedTransfer,
          }
        }
        case LookupTransferResultType.NOT_FOUND: {
          return {
            type: PrepareResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createInternalServerFSPIOPError(
              `TigerBeetleLedger.prepare() - TigerBeetleLedger.lookupTransfer() got result \
                  ${lookupTransferResult.type} after encountering ${firstError}. This should not be \
                  possible`.replace(/\s+/g, ' ')
            )
          }
        }
        case LookupTransferResultType.FAILED: {
          return {
            type: PrepareResultType.FAIL_OTHER,
            error: lookupTransferResult.error
          }
        }
      }
    } catch (err) {
      return {
        type: PrepareResultType.FAIL_OTHER,
        error: err
      }
    }
  }

  private async abort(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    logger.debug('TigerBeetleLedger.abort()')
    assert(input.action === Enum.Events.Event.Action.ABORT)

    try {
      // Lookup transfer spec to verify authorization
      const transferSpecResults = await this.deps.specStore.lookupTransferSpec([input.transferId])
      assert(transferSpecResults.length === 1)
      const transferSpec = transferSpecResults[0]
      if (transferSpec.type !== 'SpecTransfer') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `payment metadata not found`
          )
        }
      }
      if (input.callerDfspId !== transferSpec.payeeId) {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `Only the payee (${transferSpec.payeeId}) can abort this transfer. Caller: ${input.callerDfspId}`
          )
        }
      }

      const payerAccountSpecResult = await this.deps.specStore.getAccountSpec(transferSpec.payerId, transferSpec.currency)
      if (payerAccountSpecResult.type === 'SpecAccountNone') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `Could not find AccountSpec for dfsp: ${transferSpec.payerId}`
          )
        }
      }
      const payerAccountSpec = payerAccountSpecResult
      const ledgerOperation = this.currencyManager.getLedgerOperation(transferSpec.currency)

      // Lookup the original transfers from Clearing_Credit -> Setup and Unrestricted -> Setup
      // and reverse them. 
      // 
      // This is serializing reads/writes to TigerBeetle, which could have an adverse effect on 
      // performance.
      const lookupTransferResult = await this.lookupTransfer({ transferId: input.transferId })
      switch (lookupTransferResult.type) {
        case LookupTransferResultType.FOUND_FINAL: {
          return {
            type: FulfilResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
              `failed to abort transfer: ${input.transferId} - already fulfilled or aborted`
            )
          }
        }
        case LookupTransferResultType.NOT_FOUND:
        case LookupTransferResultType.FAILED:
          return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `failed to abort transfer: ${input.transferId} - expected state: FOUND_NON_FINAL, found: ${lookupTransferResult.type} `
          )
        }
      }
      assert(lookupTransferResult.type === LookupTransferResultType.FOUND_NON_FINAL)
    
      const prepareId = Helper.fromMojaloopId(input.transferId)
      const transfers: Array<Transfer> = [
        // Void the pending transfer
        // This acts as an atomicity check.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: 0n,
          user_data_128: prepareId,
          pending_id: prepareId,
          ledger: 0,
          code: TransferCode.Clearing_Active_Check,
          flags: TransferFlags.linked | TransferFlags.void_pending_transfer,
        },
        // Reverse reservation
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: payerAccountSpec.reserved,
          credit_account_id: payerAccountSpec.clearingCredit,
          amount: lookupTransferResult.amountClearingCredit,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Reverse,
          flags: TransferFlags.linked,
        },
        // Reverse reservation
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: payerAccountSpec.reserved,
          credit_account_id: payerAccountSpec.unrestricted,
          amount: lookupTransferResult.amountUnrestricted,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Reverse,
          flags: 0
        }
      ]
      const fatalErrors: Array<FailureResult<AbortFailureType>> = []
      const createTransferErrors = await this.deps.client.createTransfers(transfers)
      createTransferErrors.forEach(error => {
        // Ignore noisy errors
        if (error.result === CreateTransferError.linked_event_failed) {
          return
        }

        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.pending_transfer_not_found:
              fatalErrors.push({ type: 'NOT_FOUND', ...error })
              return
            case CreateTransferError.pending_transfer_already_posted:
              fatalErrors.push({ type: 'ALREADY_FULFILLED', ...error })
              return
            case CreateTransferError.pending_transfer_already_voided:
              fatalErrors.push({ type: 'ALREADY_ABORTED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        let readableError: string
        switch (firstError.type) {
          case 'ALREADY_ABORTED':
            readableError = 'Payment was already aborted.'
            break
          case 'ALREADY_FULFILLED':
            readableError = 'Payment was already fulfilled.'
            break
          case 'NOT_FOUND':
            readableError = 'Payment could not be found.'
            break
          case 'UNKNOWN': {
            readableError = CreateTransferError[firstError.result]
            break;
          }
          default:
            throw new Error(`unhandled AbortFailureType: ${firstError.type}`)
        }
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `failed to abort transfer with error: ${readableError}`
          )
        }
      }

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

  public async fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    logger.debug('TigerBeetleLedger.fulfil()')

    if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
      return {
        type: FulfilResultType.PASS
      }
    }

    if (input.action === Enum.Events.Event.Action.ABORT) {
      return this.abort(input)
    }

    try {
      const transferSpecResults = await this.deps.specStore.lookupTransferSpec([input.transferId])
      assert(transferSpecResults.length === 1, `expected transfer spec for id: ${input.transferId}`)
      const transferSpec = transferSpecResults[0]
      if (transferSpec.type === 'SpecTransferNone') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payment metadata not found`
          ),
        }
      }
      assert(transferSpec.type === 'SpecTransfer')

      // Authorization check - only payee can fulfil
      if (input.callerDfspId !== transferSpec.payeeId) {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `Only the payee (${transferSpec.payeeId}) can fulfil this transfer. Caller: ${input.callerDfspId}`
          )
        }
      }

      const ledgerOperation = this.currencyManager.getLedgerOperation(transferSpec.currency)
      const assetScale = this.currencyManager.getAssetScale(transferSpec.currency)
      const amountTigerBeetle = Helper.fromMojaloopAmount(transferSpec.amount, assetScale)

      const dfspSpecPayer = await this.deps.specStore.queryDfsp(transferSpec.payerId)
      const dfspSpecPayee = await this.deps.specStore.queryDfsp(transferSpec.payeeId)
      const accountSpecPayer = await this.deps.specStore.getAccountSpec(transferSpec.payerId, transferSpec.currency)
      const accountSpecPayee = await this.deps.specStore.getAccountSpec(transferSpec.payeeId, transferSpec.currency)
      if (dfspSpecPayer.type === 'SpecDfspNone') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payer fsp: ${transferSpec.payerId} not found`
          ),
        }
      }
      if (dfspSpecPayee.type === 'SpecDfspNone') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payee fsp: ${transferSpec.payeeId} not found`
          ),
        }
      }
      if (accountSpecPayer.type === 'SpecAccountNone') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payer fsp: ${transferSpec.payerId} not found`
          ),
        }
      }
      if (accountSpecPayee.type === 'SpecAccountNone') {
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payee fsp: ${transferSpec.payeeId} not found`
          ),
        }
      }

      const prepareId = Helper.fromMojaloopId(input.transferId)

      // Validate that the fulfilment matches the condition
      const fulfilmentAndConditionResult = Helper.validateFulfilmentAndCondition(
        input.payload.fulfilment, transferSpec.condition
      )
      if (fulfilmentAndConditionResult.type === 'FAIL') {
        const abortResult = await this.abort({ ...input, action: Enum.Events.Event.Action.ABORT })
        if (abortResult.type !== FulfilResultType.PASS) {
          return {
            type: FulfilResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
              `encountered unexpected error when aborting payment after invalid fulfilment`
            )
          }
        }

        return {
          type: FulfilResultType.FAIL_VALIDATION,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `fulfilment failed validation with error: ${fulfilmentAndConditionResult.reason}`
          )
        }
      }
      // Attach the fulfilment to the TransferSpec
      // There's a latency performance hit we're going to take here sadly, not sure if there's any
      // way around it.
      await this.deps.specStore.attachTransferSpecFulfilment([
        {
          id: transferSpec.id,
          fulfilment: input.payload.fulfilment,
        }
      ])

      const idLockTransfer = id()
      const transfers: Array<Transfer> = [
        // Ensure both Participants are active
        {
          ...Helper.createTransferTemplate,
          id: id(),
          pending_id: prepareId,
          debit_account_id: 0n,
          credit_account_id: 0n,
          // Ensures that the amount didn't get modified between prepare() and fulfil()
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: 0,
          code: 0,
          flags: TransferFlags.linked | TransferFlags.post_pending_transfer,
        },
        // Fulfil payment for Participant A.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: accountSpecPayer.reserved,
          credit_account_id: accountSpecPayee.commitedOutgoing,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Fulfil,
          flags: TransferFlags.linked
        },
        // Note: we always assume Payee Instant Credit is enabled as that is the legacy behaviour
        // Future implementations where the Payee Instant Credit can be disabled should skip the
        // following

        // Make credit available for transfers
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: accountSpecPayee.commitedOutgoing,
          credit_account_id: accountSpecPayee.clearingCredit,
          amount: amountTigerBeetle,
          user_data_128: prepareId,
          ledger: ledgerOperation,
          code: TransferCode.Clearing_Credit,
          flags: 0
        },
      ]

      const fatalErrors: Array<FailureResult<FulfilFailureType>> = []
      const createTransferErrors = await this.deps.client.createTransfers(transfers)
      createTransferErrors.forEach(error => {
        // Ignore noisy errors
        if (error.result === CreateTransferError.linked_event_failed) {
          return
        }
        if (error.index === 0) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.debit_account_already_closed:
              fatalErrors.push({ type: 'PAYER_CLOSED', ...error })
              return
            case CreateTransferError.credit_account_already_closed:
              fatalErrors.push({ type: 'PAYEE_CLOSED', ...error })
              return
            case CreateTransferError.pending_transfer_not_found:
              fatalErrors.push({ type: 'NOT_FOUND', ...error })
              return
            case CreateTransferError.pending_transfer_already_posted:
              fatalErrors.push({ type: 'ALREADY_FULFILLED', ...error })
              return
            case CreateTransferError.pending_transfer_already_voided:
              fatalErrors.push({ type: 'ALREADY_ABORTED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }
        if (error.index === 1) {
          switch (error.result) {
            case CreateTransferError.ok:
              return
            case CreateTransferError.credit_account_already_closed:
              fatalErrors.push({ type: 'PAYEE_ACCOUNT_CLOSED', ...error })
              return
            default:
              fatalErrors.push({ type: 'UNKNOWN', ...error })
              return
          }
        }
        fatalErrors.push({ type: 'UNKNOWN', ...error })
      })

      if (fatalErrors.length > 0) {
        const firstError = fatalErrors[0]
        let readableError: string
        switch (firstError.type) {
          case 'PAYER_CLOSED': readableError = 'Payer is closed.'
            break;
          case 'PAYEE_CLOSED': readableError = 'Payee is closed.'
            break;
          case 'ALREADY_ABORTED': readableError = 'Payment was already aborted.'
            break;
          case 'ALREADY_FULFILLED': readableError = 'Payment was already fulfilled.'
            break;
          case 'NOT_FOUND': readableError = 'Payment could not be found.'
            break;
          case 'PAYER_ACCOUNT_CLOSED': readableError = 'Payer account is closed.'
            break
          case 'PAYEE_ACCOUNT_CLOSED': readableError = 'Payee account is closed.'
            break;
          case 'UNKNOWN': readableError = CreateTransferError[firstError.result]
            break;
          default:
            throw new Error(`unhandled FulfilFailureType: ${firstError.type}`)
        }
        return {
          type: FulfilResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `fulfil failed with error: ${readableError}`
          )
        }
      }

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

  /**
   * @method lookupTransfer
   */
  // TODO(LD): have an internal version that exposes the individual amounts for DUPLICATE_NON_FINAL
  public async lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse> {
    const prepareId = Helper.fromMojaloopId(query.transferId)

    // look up all TigerBeetle Transfers related to this MojaloopId
    const relatedTransfers = await this.deps.client.queryTransfers({
      user_data_128: prepareId,
      user_data_64: 0n,
      user_data_32: 0,
      ledger: 0,
      code: 0,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: 10,
      flags: 0
    })

    if (relatedTransfers.length === 0) {
      return {
        type: LookupTransferResultType.NOT_FOUND,
      }
    }

    // Found < 6   => Error, something was created incorrectly
    // Found == 6  => Only the prepare transfers have been created
    // Found > 6   => Prepare + Fulfil transfers or Prepare + Abort transfers have been created
    const idempotentTransfers = relatedTransfers.filter(t => t.code === TransferCode.Clearing_Active_Check)
    const reserveTransfers = relatedTransfers.filter(t => t.code === TransferCode.Clearing_Reserve)
    const fulfilTransfers = relatedTransfers.filter(t => t.code === TransferCode.Clearing_Fulfil)
    const abortTransfers = relatedTransfers.filter(t => t.code === TransferCode.Clearing_Reverse)
    if (reserveTransfers.length === 3 && fulfilTransfers.length === 0) {
      // we can deduce this based on the order
      const amountClearingCredit =reserveTransfers[0].amount
      const amountUnrestricted = reserveTransfers[1].amount
      const amountTotal = reserveTransfers[2].amount
      assert.equal(amountClearingCredit + amountUnrestricted, amountTotal, 'Invalid amounts derived from reserveTransfers')

      return {
        type: LookupTransferResultType.FOUND_NON_FINAL,
        amountClearingCredit,
        amountUnrestricted,
      }
    }



    // TODO(LD): need to refactor this for newer transfers

    // if (reserveTransfers.length > 9) {
    //   return {
    //     type: LookupTransferResultType.FAILED,
    //     error: ErrorHandler.Factory.createInternalServerFSPIOPError(
    //       `Found: ${relatedTransfers.length} transfers with code: ${TransferCode.Clearing_Reserve}. Expected at most 6.`
    //     )
    //   }
    // }

    assert.equal(idempotentTransfers.length, 2, `expected pending + post_pending or pending + void_pending for prepareId: ${prepareId} and code: ${TransferCode.Clearing_Active_Check}`)

    let pendingTransfer: Transfer;
    let finalTransfer: Transfer;
    if (idempotentTransfers[0].pending_id === 0n) {
      [pendingTransfer, finalTransfer] = idempotentTransfers
    } else if (idempotentTransfers[1].pending_id === 0n) {
      [finalTransfer, pendingTransfer] = idempotentTransfers
    } else {
      return {
        type: LookupTransferResultType.FAILED,
        error: ErrorHandler.Factory.createInternalServerFSPIOPError(
          `Found: ${relatedTransfers.length} related transfers. Expected at most 2.`
        )
      }
    }

    if (finalTransfer.flags & TransferFlags.post_pending_transfer) {
      const committedTime = convertBigIntToNumber(finalTransfer.timestamp / 1_000_000n)
      const transferSpec = await this.deps.specStore.lookupTransferSpec([query.transferId])
      assert(transferSpec.length === 1, 'expected exactly one transferSpec result')

      const foundSpec = transferSpec[0]
      if (foundSpec.type === 'SpecTransferNone') {
        return {
          type: LookupTransferResultType.FAILED,
          error: ErrorHandler.Factory.createInternalServerFSPIOPError(
            `missing transfer spec for finalized transferId: ${query.transferId}`
          )
        }
      }

      if (!foundSpec.fulfilment) {
        return {
          type: LookupTransferResultType.FAILED,
          error: ErrorHandler.Factory.createInternalServerFSPIOPError(
            `missing spec.fulfilment for finalized transferId: ${query.transferId}`
          )
        }
      }

      return {
        type: LookupTransferResultType.FOUND_FINAL,
        finalizedTransfer: {
          completedTimestamp: (new Date(committedTime)).toISOString(),
          transferState: "COMMITTED",
          fulfilment: foundSpec.fulfilment
        }
      }
    } else if (finalTransfer.flags & TransferFlags.void_pending_transfer) {
      const abortedTime = convertBigIntToNumber(finalTransfer.timestamp / 1_000_000n)
      return {
        type: LookupTransferResultType.FOUND_FINAL,
        finalizedTransfer: {
          completedTimestamp: (new Date(abortedTime)).toISOString(),
          transferState: "ABORTED"
        }
      }
    }

    logger.warn(`fulfilTransfer with id: ${finalTransfer.id} had neither 'post_pending_transfer' nor 'void_pending_transfer' flags set.`)
    return {
      type: LookupTransferResultType.FAILED,
      error: ErrorHandler.Factory.createInternalServerFSPIOPError(
        `fulfilTransfer with id: ${finalTransfer.id} had neither 'post_pending_transfer' nor 'void_pending_transfer' flags set.`
      )
    }
  }

  /**
   * @description Looks up a list of transfers that have timed out.
   */
  public async sweepTimedOut(): Promise<SweepResult> {
    const MAX_TRANSFERS_IN_PAGE = 8000
    const MAX_PAGES = 10

    try {
      const bookmarkQuery: QueryFilter = {
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        ledger: Helper.ledgerIds.timeoutHandler,
        code: 0,
        timestamp_min: 0n,
        timestamp_max: 0n,
        limit: 10,
        flags: QueryFilterFlags.reversed
      }
      let bookmarkTransfers = await this.deps.client.queryTransfers(bookmarkQuery)
      if (bookmarkTransfers.length === 0) {
        logger.debug(`sweepTimedOut - no opening bookmark found. creating one now.`)
        // No bookmark transfers exist yet - create one now starting at time=0
        await this._createOpeningBookmarkTransfer()

        bookmarkTransfers = await this.deps.client.queryTransfers(bookmarkQuery)
        if (bookmarkTransfers.length === 0) {
          throw new Error(`sweepTimedOut() - failed, found no bookmark entries even after creating the opening bookmark.`)
        }
      }
      const openingBookmark = bookmarkTransfers[0]
      // TODO: make sure that the latest bookmark is actually the latest!
      const openingBookmarkTimestamp = openingBookmark.user_data_64
      logger.debug(`sweepTimedOut - openingBookmarkTimestamp: ${openingBookmarkTimestamp} `)

      const transfersQuery: QueryFilter = {
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        ledger: 0,
        code: 1,
        timestamp_min: openingBookmarkTimestamp,
        timestamp_max: 0n,
        limit: MAX_TRANSFERS_IN_PAGE,
        flags: QueryFilterFlags.reversed
      }

      let transfers = await this.deps.client.queryTransfers(transfersQuery)
      if (transfers.length === MAX_TRANSFERS_IN_PAGE) {
        let page = 1
        while (page < MAX_PAGES) {
          let pagedQuery: QueryFilter = {
            ...transfersQuery,
            timestamp_max: transfers[transfers.length - 1].timestamp
          }
          const nextPage = await this.deps.client.queryTransfers(pagedQuery)
          transfers = transfers.concat(nextPage)

          if (nextPage.length < MAX_TRANSFERS_IN_PAGE) {
            // we ran out of entries
            break;
          }

          page += 1
        }
      }

      logger.debug(`sweepTimedOut - found ${transfers.length} transfers since ${openingBookmarkTimestamp}`)

      // now that we have all transfers, or when we reach the limit
      const maybeTimedOutTransfers: { [Key: string]: Transfer } = {}
      const postedAndVoidedTransferIds: { [Key: string]: true } = {}
      transfers.forEach(transfer => {
        if (transfer.flags & TransferFlags.pending) {
          maybeTimedOutTransfers[`${transfer.id}`] = transfer
          return
        }

        if (transfer.flags & TransferFlags.post_pending_transfer ||
          transfer.flags & TransferFlags.void_pending_transfer) {
          assert(transfer.pending_id, 'expected post_pending or void_pending transfer to have a pending_id')
          postedAndVoidedTransferIds[`${transfer.pending_id}`] = true
          return
        }
      })

      logger.debug(`sweepTimedOut - filtering out ${Object.keys(postedAndVoidedTransferIds).length} posted and voided transfers.`)

      // Remove the transfers that were posted or voided from the maybeTimedOutTransfers set
      Object.keys(postedAndVoidedTransferIds).forEach(key => {
        delete maybeTimedOutTransfers[key]
      })

      // Remove the in flight transfers from the maybeTimedOutTransfers set

      // TODO(LD): In future versions (0.17), create a dummy transfer to get the last timestamp of the
      // switch, and use this instead of the NodeJS process time here, to protect us from clock
      // drifts.

      // I need to double check the inter-scheme stuff, but I think if we set the timeout to 0,
      // then we know that we are the Remote Ledger for the Payment, and don't have the authority
      // to time it out. 
      // 
      // If we _do_ set the timeout, then we can rely on TigerBeetle to have reverted
      // the pending Transfer and need to communicate that to others.
      // 
      // Otherwise we would have to set the timeout somewhere in the transfer user_data in order to 
      // keep track of when it is safe to abort it and inform interested parties. Which could still
      // be a decent option given that the timeout is given as an absolute time.
      // 
      // OR we could look at TransferSpec and do a query based on timeouts, to get the possible
      // timed out transfers, and abort them accordingly. (that would be the simpler approach)
      //
      const nowNs = BigInt(new Date().getTime()) * 1_000_000n
      Object.keys(maybeTimedOutTransfers).forEach(key => {
        const transfer = maybeTimedOutTransfers[key]
        const timeoutNs = BigInt(transfer.timeout) * NS_PER_SECOND
        if ((transfer.timestamp + timeoutNs) > nowNs) {
          delete maybeTimedOutTransfers[key]
        }
      })

      const timedOutTransfers = Object.values(maybeTimedOutTransfers)
      if (timedOutTransfers.length === 0) {
        logger.debug(`sweepTimedOut - found no timed out transfers. Returning.`)

        return {
          type: 'SUCCESS',
          transfers: []
        }
      }

      logger.debug(`sweepTimedOut - found ${timedOutTransfers.length} timed out transfers`)

      // Lookup the spec for each transfer from the spec database. If this fails, throw an error.
      const specs = await this.deps.specStore.lookupTransferSpec(timedOutTransfers.map(t => Helper.toMojaloopId(t.id)))
      const missingSpec = specs.filter(m => m.type === 'SpecTransferNone')
      assert(missingSpec.length === 0, `lookupTransferSpec() missing ${missingSpec.length} entries`)
      const foundSpec = specs.filter(m => m.type === 'SpecTransfer')
      assert(foundSpec.length === timedOutTransfers.length, `lookupTransferSpec() expected foundSpec.length === ${timedOutTransfers.length}, but instead got: ${foundSpec.length}`)

      const transfersWithSpec: Array<TimedOutTransfer> = []
      foundSpec.forEach(spec => {
        transfersWithSpec.push({
          id: spec.id,
          payerId: spec.payerId,
          payeeId: spec.payeeId,
        })
      })

      // Now we can close the opening bookmark, and set a new bookmark to the time that we just 
      // swept. We do so atomically to make sure that no other racing timeouts have called 
      // sweepTimedOut() and already swept the timed out transfers.
      const lastTimedOutTransfer = timedOutTransfers[timedOutTransfers.length - 1]
      assert(lastTimedOutTransfer)
      const newOpeningTimestamp = lastTimedOutTransfer.timestamp + 1n

      const atomicBookmarks: Array<Transfer> = [
        // Close the last bookmark
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: Helper.accountIds.bookmarkDebit,
          credit_account_id: Helper.accountIds.bookmarkCredit,
          amount: 0n,
          pending_id: openingBookmark.id,
          ledger: Helper.ledgerIds.timeoutHandler,
          code: Helper.transferCodes.timeoutBookmark,
          flags: TransferFlags.void_pending_transfer | TransferFlags.linked,
        },
        // Open a new bookmark
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: Helper.accountIds.bookmarkDebit,
          credit_account_id: Helper.accountIds.bookmarkCredit,
          amount: 0n,
          user_data_64: newOpeningTimestamp,
          ledger: Helper.ledgerIds.timeoutHandler,
          code: Helper.transferCodes.timeoutBookmark,
          flags: TransferFlags.pending,
        },
      ]
      const atomicBookmarkErrors = await this.deps.client.createTransfers(atomicBookmarks)

      const fatalBookmarkErrors = []
      for (const error of atomicBookmarkErrors) {
        // If the error is `pending_transfer_already_voided`, then we know that this call of sweep() raced
        // with another one!
        fatalBookmarkErrors.push(CreateTransferError[error.result])
      }

      if (fatalBookmarkErrors.length > 0) {
        return {
          type: 'FAILURE',
          error: new Error(`sweepTimedOut() - encountered fatal error when closing and opening\n${fatalBookmarkErrors.join(',')}`)
        }
      }

      return {
        type: 'SUCCESS',
        transfers: transfersWithSpec
      }
    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  private async _createOpeningBookmarkTransfer(): Promise<void> {
    const bookmarkControlAcounts: Array<Account> = [
      {
        ...Helper.createAccountTemplate,
        id: Helper.accountIds.bookmarkDebit,
        ledger: Helper.ledgerIds.timeoutHandler,
        code: AccountCode.TIMEOUT,
        flags: 0,
      },
      {
        ...Helper.createAccountTemplate,
        id: Helper.accountIds.bookmarkCredit,
        ledger: Helper.ledgerIds.timeoutHandler,
        code: AccountCode.TIMEOUT,
        flags: 0,
      },
    ]
    const createAccountsErrors = await this.deps.client.createAccounts(bookmarkControlAcounts)

    const fatalAccountErrors = []
    for (const error of createAccountsErrors) {
      if (error.result === CreateAccountError.exists) {
        continue
      }

      fatalAccountErrors.push(CreateAccountError[error.result])
    }

    if (fatalAccountErrors.length > 0) {
      throw new Error(`_createOpeningBookmarkTransfer() - encountered fatal error when creating bookmark control accounts\n${fatalAccountErrors.join(',')}`)
    }

    const openingBookmarkTransfer: Transfer = {
      ...Helper.createTransferTemplate,
      id: id(),
      debit_account_id: Helper.accountIds.bookmarkDebit,
      credit_account_id: Helper.accountIds.bookmarkCredit,
      amount: 0n,
      ledger: Helper.ledgerIds.timeoutHandler,
      code: Helper.transferCodes.timeoutBookmark,
      flags: TransferFlags.pending,
    }
    const createTransfersErrors = await this.deps.client.createTransfers([openingBookmarkTransfer])
    const fatalTransferErrors = []
    for (const error of createTransfersErrors) {
      fatalTransferErrors.push(CreateTransferError[error.result])
    }

    if (fatalTransferErrors.length > 0) {
      throw new Error(`_createOpeningBookmarkTransfer() - encountered fatal error when creating opening bookmark\n${fatalTransferErrors.join(',')}`)
    }
  }

  // ============================================================================
  // Settlement Methods
  // ============================================================================

  public async closeSettlementWindow(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  public async settleClosedWindows(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  /**
   * Private Methods
   */

  /**
   * Get the last N transfers from TigerBeetle
   * @param limit - Number of transfers to retrieve (default: 100)
   * @param ledger - Optional ledger ID to filter by
   * @returns Array of transfers with account mapping information
   */
  public async getRecentTransfers(limit: number = 100, ledger?: number): Promise<Array<Transfer & {
    debitAccountInfo: { dfspId: string, accountName: string, accountCode: AccountCode },
    creditAccountInfo: { dfspId: string, accountName: string, accountCode: AccountCode },
    currency: string | undefined,
    amountReal: number,
    ledgerName: string
  }>> {
    const queryFilter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      code: 0, // No filter by Code
      ledger: ledger ?? 0, // No filter by Ledger unless specified
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit,
      flags: QueryFilterFlags.reversed, // Sort by timestamp in reverse-chronological order
    };

    const transfers = await this.deps.client.queryTransfers(queryFilter);


    // Get all unique account IDs from the transfers
    const accountIds = new Set<bigint>();
    for (const transfer of transfers) {
      accountIds.add(transfer.debit_account_id);
      accountIds.add(transfer.credit_account_id);
    }

    // Look up all accounts from TigerBeetle
    const accounts = await this.deps.client.lookupAccounts(Array.from(accountIds));

    // Build account ID to spec mapping
    const accountIdToSpec = new Map<string, { dfspId: string, accountCode: AccountCode }>();
    const allSpecs = await this.deps.specStore.queryAccountsAll();

    for (const spec of allSpecs) {
      accountIdToSpec.set(spec.deposit.toString(), { dfspId: spec.dfspId, accountCode: AccountCode.Deposit });
      accountIdToSpec.set(spec.unrestricted.toString(), { dfspId: spec.dfspId, accountCode: AccountCode.Unrestricted });
      accountIdToSpec.set(spec.unrestrictedLock.toString(), { dfspId: spec.dfspId, accountCode: AccountCode.Unrestricted_Lock });
      accountIdToSpec.set(spec.restricted.toString(), { dfspId: spec.dfspId, accountCode: AccountCode.Restricted });
      accountIdToSpec.set(spec.reserved.toString(), { dfspId: spec.dfspId, accountCode: AccountCode.Reserved });
      accountIdToSpec.set(spec.commitedOutgoing.toString(), { dfspId: spec.dfspId, accountCode: AccountCode.Committed_Outgoing });
    }

    // Enrich transfers with account information
    const enrichedTransfers = transfers.map(transfer => {
      const debitSpec = accountIdToSpec.get(transfer.debit_account_id.toString());
      const creditSpec = accountIdToSpec.get(transfer.credit_account_id.toString());

      // Get currency from ledger ID
      const currency = this.currencyManager.getCurrencyFromLedger(transfer.ledger);

      // Convert amount to real number - use assetScale of 1 if no currency
      const assetScale = currency ? this.currencyManager.getAssetScale(currency) : 1;
      const amountReal = Helper.toRealAmount(transfer.amount, assetScale);

      // Determine ledger name
      let ledgerName: string;
      switch (transfer.ledger) {
        case Helper.ledgerIds.globalControl:
          ledgerName = 'GLOBAL_CONTROL';
          break;
        case Helper.ledgerIds.timeoutHandler:
          ledgerName = 'TIMEOUT_CONTROL';
          break;
        default: {
          if (currency) {
            const ledgerOperation = this.currencyManager.getLedgerOperation(currency);
            const ledgerControl = this.currencyManager.getLedgerControl(currency);
            switch (transfer.ledger) {
              case ledgerOperation:
                ledgerName = `${currency}_OPERATION`;
                break;
              case ledgerControl:
                ledgerName = `${currency}_CONTROL`;
                break;
              default:
                ledgerName = `UNKNOWN_${transfer.ledger}`;
            }
          } else {
            ledgerName = `UNKNOWN_${transfer.ledger}`;
          }
        }
      }

      return {
        ...transfer,
        debitAccountInfo: debitSpec ? {
          dfspId: debitSpec.dfspId,
          accountName: AccountCode[debitSpec.accountCode],
          accountCode: debitSpec.accountCode
        } : {
          dfspId: 'UNKNOWN',
          accountName: 'UNKNOWN',
          accountCode: AccountCode.Dev_Null
        },
        creditAccountInfo: creditSpec ? {
          dfspId: creditSpec.dfspId,
          accountName: AccountCode[creditSpec.accountCode],
          accountCode: creditSpec.accountCode
        } : {
          dfspId: 'UNKNOWN',
          accountName: 'UNKNOWN',
          accountCode: AccountCode.Dev_Null
        },
        currency,
        amountReal,
        ledgerName
      };
    });

    // Reverse to show oldest first (we fetched newest first with reversed flag)
    return enrichedTransfers.reverse();
  }
}