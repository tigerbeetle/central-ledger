import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import { Enum } from '@mojaloop/central-services-shared';
import assert from "assert";
import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler";
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { ApplicationConfig } from "src/shared/config";
import { QueryResult } from 'src/shared/results';
import { Account, AccountFilterFlags, AccountFlags, amount_max, Client, CreateAccountError, CreateTransferError, id, QueryFilter, QueryFilterFlags, Transfer, TransferFlags } from 'tigerbeetle-node';
import { convertBigIntToNumber } from "../../shared/config/util";
import { logger } from '../../shared/logger';
import { CurrencyManager } from './CurrencyManager';
import { Ledger } from "./Ledger";
import { DfspAccountIds, SpecAccount, SpecDfsp, SpecStore } from "./SpecStore";
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
} from "./types";
import { Help } from 'commander';
import { TIMEOUT } from 'dns';

const NS_PER_MS = 1_000_000n
const NS_PER_SECOND = NS_PER_MS * 1_000n


// TODO(LD): rename DfspAccountType?
// TODO: should we just move this to Helper.accountCodes? Or rename to be AccountCode
export enum AccountCode {
  Settlement_Balance = 10100,
  Deposit = 10200,
  Unrestricted = 20100,
  Unrestricted_Lock = 20101,
  Restricted = 20200,
  Reserved = 20300,
  Committed_Outgoing = 20400,
  Dfsp = 60100,
  Net_Debit_Cap = 60200,
  Net_Debit_Cap_Control = 60201,
  Dev_Null = 60300,

  // Remove me
  TIMEOUT = 9000,
}


/**
 * An internal representation of an Account, combined with Spec
 */
interface InternalLedgerAccount extends Account {
  dfspId: string,
  currency: string,
  // Technically we don't need this since it lives on the account.code, but as a number
  accountCode: AccountCode
}

interface InternalMasterAccount extends Account {
  dfspId: string,
}

export interface TigerBeetleLedgerDependencies {
  config: ApplicationConfig
  client: Client
  specStore: SpecStore
  transferBatcher: TransferBatcher
}

type InternalNetDebitCap = {
  type: 'LIMITED'
  amount: bigint,
} | {
  type: 'UNLIMITED'
}

export default class TigerBeetleLedger implements Ledger {
  private currencyManager: CurrencyManager

  constructor(private deps: TigerBeetleLedgerDependencies) {
    this.currencyManager = new CurrencyManager(this.deps.config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS)
  }

  /**
   * Onboarding/Lifecycle Management
   */

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
          accountSpecResult.netDebitCap,
        ]);
        if (accounts.length === 7) {
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
      const ledgerControl = this.currencyManager.getLedgerControl(currency)
      const accountIdSettlementBalance = this.currencyManager.getAccountIdSettlementBalance(currency)

      const accountIds: DfspAccountIds = {
        deposit: Helper.idSmall(),
        unrestricted: Helper.idSmall(),
        unrestrictedLock: Helper.idSmall(),
        restricted: Helper.idSmall(),
        reserved: Helper.idSmall(),
        commitedOutgoing: Helper.idSmall(),
        netDebitCap: Helper.idSmall(),
        netDebitCapControl: Helper.idSmall(),
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
        // Net Debit Cap - stores the Net Debit Cap for this Dfsp + Currency
        {
          ...Helper.createAccountTemplate,
          id: accountIds.netDebitCap,
          ledger: ledgerControl,
          code: AccountCode.Net_Debit_Cap,
          flags: AccountFlags.linked
        },
        // Net Debit Cap Control account - counterparty for the Net Debit Cap Setting
        {
          ...Helper.createAccountTemplate,
          id: accountIds.netDebitCapControl,
          ledger: ledgerControl,
          code: AccountCode.Net_Debit_Cap_Control,
          flags: AccountFlags.linked
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

      // TODO(LD): Set up the net debit cap with an opening transfer of Maximum amount
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
    if (transferErrors.length === 0) {
      return {
        type: DeactivateDfspResponseType.SUCCESS
      }
    }

    for (const err of transferErrors) {
      if (err.result === CreateTransferError.debit_account_not_found) {
        return {
          type: DeactivateDfspResponseType.CREATE_ACCOUNT
        }
      }

      if (err.result === CreateTransferError.ok) {
        return {
          type: DeactivateDfspResponseType.SUCCESS
        }
      }

      if (err.result === CreateTransferError.credit_account_already_closed) {
        return {
          type: DeactivateDfspResponseType.ALREADY_CLOSED
        }
      }

      return {
        type: DeactivateDfspResponseType.FAILED,
        error: new Error(`_closeDfspMasterAccount failed with unexpected error: ${CreateTransferError[err.result]}`)
      }
    }
    transferErrors.forEach((err, idx) => {
      if (err.result === CreateTransferError.debit_account_not_found) {
        return
      }
    })
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
      // consider this a success, the Account isn't closed!
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

  public async enableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>> {
    throw new Error('not implemented')
  }

  public async disableDfspAccount(cmd: { dfspId: string, accountId: number }): Promise<CommandResult<void>> {
    throw new Error('not implemented')
  }

  // TODO(LD): Come back to the design on this one. I'm a little unsure about how to handle the
  // mismatch between single entry accounting in the original ledger, and double entry here.
  public async deposit(cmd: DepositCommand): Promise<DepositResponse> {
    assert(cmd.amount)
    assert(cmd.currency)
    assert(cmd.dfspId)
    assert(cmd.transferId)

    try {
      const netDebitCapInternal = await this._getNetDebitCapInteral(cmd.currency, cmd.dfspId)
      const specAccounts = await this.deps.specStore.queryAccounts(cmd.dfspId)
      if (specAccounts.length === 0) {
        throw new Error(`no dfspId found: ${cmd.dfspId}`)
      }
      const spec = specAccounts[0]

      const ledgerOperation = this.currencyManager.getLedgerOperation(cmd.currency)
      const assetScale = this.currencyManager.getAssetScale(cmd.currency)
      const amountInternal = BigInt(cmd.amount * assetScale)

      const idLockTransfer = id()
      let netDebitCapLockAmount = amount_max
      if (netDebitCapInternal.type === 'LIMITED') {
        netDebitCapLockAmount = netDebitCapInternal.amount
      }
      const transfers: Array<Transfer> = [
        // Deposit funds into Unrestricted
        {
          ...Helper.createTransferTemplate,
          // TODO: need to derive this id from the command.
          id: id(),
          debit_account_id: spec.deposit,
          credit_account_id: spec.unrestricted,
          amount: amountInternal,
          ledger: ledgerOperation,
          code: 1,
          flags: TransferFlags.linked
        },
        // Temporarily lock up to the net debit cap.
        {
          ...Helper.createTransferTemplate,
          id: idLockTransfer,
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.unrestrictedLock,
          amount: netDebitCapLockAmount,
          ledger: ledgerOperation,
          code: 1,
          flags: TransferFlags.linked | TransferFlags.pending | TransferFlags.balancing_credit
        },
        // Sweep whatever remains in Unrestricted to Restricted.
        {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: spec.unrestricted,
          credit_account_id: spec.restricted,
          amount: amount_max,
          ledger: ledgerOperation,
          code: 1,
          flags: TransferFlags.linked | TransferFlags.balancing_credit
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
          code: 1,
          flags: TransferFlags.void_pending_transfer
        }
      ]

      const fatalErrors = (await this.deps.client.createTransfers(transfers)).reduce((acc, curr) => {
        // if (curr.index === 0 && curr.result === CreateTransferError.ok) {
        //   return acc
        // }
        acc.push(`Transfer at idx: ${curr.index} failed with error: ${CreateTransferError[curr.result]}`)
        return acc
      }, [])
      if (fatalErrors.length > 0) {
        return {
          type: 'FAILURE',
          error: new Error(`failed to create transfers with errors: ${fatalErrors.join(';')}`)
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
    throw new Error('not implemented')
  }

  public async withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    throw new Error('not implemented')
  }

  public async setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    assert(cmd.currency)
    assert(cmd.dfspId)

    let amountNetDebitCap: bigint
    // 8 for limited, 9 for unlimited
    let code: number
    switch (cmd.netDebitCapType) {
      case 'AMOUNT': {
        assert(cmd.amount)
        assert(cmd.amount >= 0, 'expected amount to 0 or a positive integer')
        const assetScale = this.currencyManager.getAssetScale(cmd.currency)
        amountNetDebitCap = BigInt(Math.floor(cmd.amount * assetScale))
        code = 8

        break;
      }
      case 'UNLIMITED': {
        amountNetDebitCap = 0n
        code = 9
      }
    }
    const specAccounts = await this.deps.specStore.queryAccounts(cmd.dfspId)
    if (specAccounts.length === 0) {
      throw new Error(`no dfspId found: ${cmd.dfspId}`)
    }
    const spec = specAccounts[0]
    const ledgerOperation = this.currencyManager.getLedgerOperation(cmd.currency)
    const ledgerControl = this.currencyManager.getLedgerControl(cmd.currency)

    // TODO(LD): transfers to actually move funds between accounts!
    const idLockTransfer = id()
    const transfers: Array<Transfer> = [
      // Set the new Net Debit Cap
      {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.netDebitCapControl,
        credit_account_id: spec.netDebitCap,
        amount: amountNetDebitCap,
        ledger: ledgerControl,
        code,
        flags: TransferFlags.linked,
      },
      // Sweep total balance from Restricted to Unrestricted
      {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.restricted,
        credit_account_id: spec.unrestricted,
        amount: amount_max,
        ledger: ledgerOperation,
        code: 10,
        flags: TransferFlags.linked | TransferFlags.balancing_debit
      },
      // Move the new NDC amount out to a temporary account.
      // if the new NDC amount is unlimited, then move all into temporary account.
      {
        ...Helper.createTransferTemplate,
        id: idLockTransfer,
        debit_account_id: spec.unrestricted,
        credit_account_id: spec.unrestrictedLock,
        amount: cmd.netDebitCapType === 'AMOUNT' && amountNetDebitCap || amount_max,
        ledger: ledgerOperation,
        code: 10,
        flags: TransferFlags.linked | TransferFlags.balancing_credit | TransferFlags.pending
      },
      // Sweep whatever remains in Unrestricted to Restricted.
      {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: spec.unrestricted,
        credit_account_id: spec.restricted,
        amount: amount_max,
        ledger: ledgerOperation,
        code: 10,
        flags: TransferFlags.linked | TransferFlags.balancing_credit
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
        code: 10,
        flags: TransferFlags.void_pending_transfer
      },
    ]
    const fatalErrors = (await this.deps.client.createTransfers(transfers)).reduce((acc, curr) => {
      if (curr.index === 0 && curr.result === CreateTransferError.ok) {
        return acc
      }
      acc.push(CreateTransferError[curr.result])
      return acc
    }, [])
    if (fatalErrors.length > 0) {
      return {
        type: 'FAILURE',
        error: new Error(`failed to create transfers with errors: ${fatalErrors.join(';')}`)
      }
    }

    return {
      type: 'SUCCESS',
      result: undefined
    }
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
   * 
   * TODO: revisit in light of other get dfsp calls, we can probably simplify and 
   * map all in one place
   */
  public async getDfspAccounts(query: GetDfspAccountsQuery): Promise<DfspAccountResponse> {
    throw new Error('refactor me!')

    // const ids = await this.deps.specStore.getDfspAccountSpec(query.dfspId, query.currency)
    // if (ids.type === 'DfspAccountSpecNone') {
    //   return {
    //     type: 'FAILURE',
    //     error: ErrorHandler.Factory.createFSPIOPError(
    //       ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
    //       `failed as getDfspAccountMetata() returned 'DfspAccountSpecNone' for \
    //           dfspId: ${query.dfspId}, and currency: ${query.currency}`.replace(/\s+/g, ' ')
    //     )
    //   }
    // }
    // const tbAccountIds = [
    //   ids.liquidity,
    //   // TODO: is this equivalent to POSITION?
    //   ids.clearing,
    //   ids.collateral,
    //   // TODO: is this equivalent to SETTLEMENT?
    //   ids.settlementMultilateral
    // ]
    // const tbAccounts = await this.deps.client.lookupAccounts(tbAccountIds)
    // if (tbAccounts.length !== tbAccountIds.length) {
    //   return {
    //     type: 'FAILURE',
    //     error: ErrorHandler.Factory.createFSPIOPError(
    //       ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
    //       `failed as getDfspAccountMetata() returned 'DfspAccountSpecNone' for \
    //           dfspId: ${query.dfspId}, and currency: ${query.currency}`.replace(/\s+/g, ' ')
    //     )
    //   }
    // }

    // // TODO(LD): We need to spend more time here figuring out how to adapt from newer double entry
    // // accounts map on to the legacy accounts
    // const accounts: Array<LegacyLedgerAccount> = []
    // let clearingAccount: Account
    // let collateralAccount: Account
    // tbAccounts.forEach(tbAccount => {
    //   if (tbAccount.id === ids.clearing) {
    //     clearingAccount = tbAccount
    //   }
    //   if (tbAccount.id === ids.collateral) {
    //     collateralAccount = tbAccount
    //   }
    // })
    // assert(clearingAccount)
    // assert(collateralAccount)

    // // Legacy Settlement Balance: How much Dfsp has available to settle.
    // // Was a negative number in the legacy API once the dfsp had deposited funds.
    // const legacySettlementBalancePosted = (collateralAccount.debits_posted - collateralAccount.credits_posted) * BigInt(-1)
    // const legacySettlementBalancePending = (collateralAccount.debits_pending - collateralAccount.credits_pending) * BigInt(-1)

    // // Legacy Position Balance: How much Dfsp is owed or how much this Dfsp owes.
    // const clearingBalancePosted = clearingAccount.credits_posted - clearingAccount.debits_posted
    // const clearingBalancePending = clearingAccount.credits_pending - clearingAccount.debits_pending
    // const legacyPositionBalancePosted = (legacySettlementBalancePosted + clearingBalancePosted) * BigInt(-1)
    // const legacyPositionBalancePending = (legacySettlementBalancePending + clearingBalancePending) * BigInt(-1)

    // accounts.push({
    //   id: ids.clearing,
    //   ledgerAccountType: 'POSITION',
    //   currency: query.currency,
    //   isActive: !(clearingAccount.flags & AccountFlags.closed),
    //   value: convertBigIntToNumber(legacyPositionBalancePosted) / 100,
    //   reservedValue: convertBigIntToNumber(legacyPositionBalancePending) / 100,
    //   // We don't have this in TigerBeetle, although we could use the created date
    //   changedDate: new Date(0)
    // })

    // accounts.push({
    //   id: ids.collateral,
    //   ledgerAccountType: 'SETTLEMENT',
    //   currency: query.currency,
    //   isActive: !(collateralAccount.flags & AccountFlags.closed),
    //   value: convertBigIntToNumber(legacySettlementBalancePosted) / 100,
    //   reservedValue: convertBigIntToNumber(legacySettlementBalancePending) / 100,
    //   // We don't have this in TigerBeetle, although we could use the created date
    //   changedDate: new Date(0)
    // })

    // return {
    //   type: 'SUCCESS',
    //   accounts,
    // }
  }

  public async getAllDfspAccounts(query: GetAllDfspAccountsQuery): Promise<DfspAccountResponse> {
    // TODO(LD): Implement this method for TigerBeetle
    // This would require getting account spec for all currencies for the DFSP
    // and then looking up all accounts
    return {
      type: 'FAILURE',
      error: ErrorHandler.Factory.createFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
        'getAllDfspAccounts not yet implemented for TigerBeetleLedger'
      )
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

  private async _getNetDebitCapInteral(currency: string, dfspId: string): Promise<InternalNetDebitCap> {
    const ids = await this.deps.specStore.getAccountSpec(dfspId, currency)
    assert(ids.type === 'SpecAccount', `Could not find spec for dfsp + currency: ${dfspId}, ${currency}`)

    // Net Debit Cap is defined as the amount of the last transfer in the account
    // + code. If the transfer code === 8, then we consider the amount, if it is 9, then
    // we consider the net debit cap to be unlimited.
    const transfers = await this.deps.client.getAccountTransfers({
      account_id: ids.netDebitCap,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      code: 0,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: 1,
      flags: AccountFilterFlags.credits | AccountFilterFlags.reversed,
    })
    if (transfers.length === 0) {
      throw new Error(`no _getNetDebitCapInteral() no net debit cap transfers found for account: ${ids.netDebitCap}`)
    }
    assert(transfers.length === 1, 'Expected to find only 1 transfer')
    const amount = transfers[0].amount
    const code = transfers[0].code

    switch (code) {
      case 9: {
        assert(amount === 0n, 'Expected amount to be 0 for unlimited net debit cap transfer')

        return {
          type: 'UNLIMITED'
        }
      }
      case 8: {
        return {
          type: 'LIMITED',
          amount,
        }
      }
      default: {
        throw new Error(`unexpected code: ${code} for net debit cap transfer.`)
      }
    }
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResult<LegacyLimit>> {
    assert(query.currency)
    assert(query.dfspId)

    const ids = await this.deps.specStore.getAccountSpec(query.dfspId, query.currency)
    if (ids.type === 'SpecAccountNone') {
      return {
        type: 'FAILURE',
        error: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND
            `failed as getDfspAccountMetata() returned 'DfspAccountSpecNone' for \
              dfspId: ${query.dfspId}, and currency: ${query.currency}`.replace(/\s+/g, ' ')
        )
      }
    }

    try {
      const internalNetDebitCap = await this._getNetDebitCapInteral(query.currency, query.dfspId)

      if (internalNetDebitCap.type === 'UNLIMITED') {
        return {
          type: 'FAILURE',
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
            `getNetDebitCap() - no limits found for dfspId: ${query.dfspId}, currency: ${query.currency}, type: 'NET_DEBIT_CAP`
          )
        }
      }

      const assetScale = this.currencyManager.getAssetScale(query.currency)
      const value = Number(internalNetDebitCap.amount / BigInt(assetScale))
      const limit: LegacyLimit = {
        type: "NET_DEBIT_CAP",
        value,
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
   * Clearing Methods
   */

  // TODO(LD): Make this interface batch compatible. This will require the new handlers to be able 
  // to read multiple messages from Kafka at the same point.

  // TODO(LD): We need to save the condition for later validation. We can be tricky and put this in
  // a cache that gets broadcast to all fulfil handlers, or otherwise use Kafka keys to ensure that
  // the condition and fulfil end up on the same handler instance.
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

      // TODO(LD): switch the interface to array based!
      const payerSpec = await this.deps.specStore.getAccountSpec(payer, currency)
      if (payerSpec.type === 'SpecAccountNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payer fsp: ${payer} not found`
          ),
        }
      }
      const payeeSpec = await this.deps.specStore.getAccountSpec(payee, currency)
      if (payeeSpec.type === 'SpecAccountNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payee fsp: ${payee} not found`
          ),
        }
      }

      const prepareId = Helper.fromMojaloopId(input.payload.transferId)
      const clearingLedgerId = this.currencyManager.getClearingLedgerId(currency)
      // TODO(LD): move this to the CurrencyManager
      const amount = Helper.fromMojaloopAmount(amountStr, 2)

      const nowMs = (new Date()).getTime()
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

      // TigerBeetle timeouts are specified in seconds. I'm not sure if we should round this up or
      // down. For now, let's be pessimistic and round down.
      const timeoutMs = expirationMs - nowMs
      assert(timeoutMs > 0)
      const timeoutSeconds = Math.floor(timeoutMs / 1000)
      if (timeoutSeconds === 0) {
        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `transfer expiry must be one or more seconds in the future.`
          ),
        }
      }
      logger.warn(`prepare() - rounding down derived transfer timeout of ${timeoutMs} to ${timeoutSeconds * 1000}`)


      /**
       * Write Last, Read First Rule
       * We write data dependencies first, then write to TigerBeetle
       * Reference: https://tigerbeetle.com/blog/2025-11-06-the-write-last-read-first-rule/
       */
      await this.deps.specStore.saveTransferSpec([
        {
          id: input.payload.transferId,
          currency: currency,
          payerId: payer,
          payeeId: payee,
          condition: input.payload.condition,
          ilpPacket: input.payload.ilpPacket
        }
      ])

      /**
       * Dr Payer_Clearing
       *  Cr Payee_Clearing
       * Flags: pending
       */
      const transfer: Transfer = {
        ...Helper.createTransferTemplate,
        id: prepareId,
        debit_account_id: payerSpec.unrestricted,
        credit_account_id: payeeSpec.restricted,
        amount,
        pending_id: 0n,
        // Also used as a correlation to map between Mojaloop Transfers (1) ---- (*) TigerBeetle Transfers
        user_data_128: prepareId,
        timeout: timeoutSeconds,
        ledger: clearingLedgerId,
        code: 1,
        flags: TransferFlags.pending,
      }

      // TODO(LD): add a 1 value linked transfer between the payer and payee master accounts
      // these will fail if either of the master accounts are disabled

      if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
        return {
          type: PrepareResultType.PASS
        }
      }

      const error = await this.deps.transferBatcher.enqueueTransfer(transfer)
      if (error) {
        // specific error handling cases
        if (error === CreateTransferError.exceeds_credits) {
          return {
            type: PrepareResultType.FAIL_LIQUIDITY,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.PAYER_FSP_INSUFFICIENT_LIQUIDITY
            )
          }
        }

        if (error === CreateTransferError.exists_with_different_amount ||
          error === CreateTransferError.exists_with_different_debit_account_id ||
          error === CreateTransferError.exists_with_different_credit_account_id) {
          return {
            type: PrepareResultType.MODIFIED
          }
        }

        /**
         * Pending Transfer has already been created.
         * 
         * Note that because Mojaloop defines timeouts as expiration times, we can't guarantee that
         * a timeout for a duplicate transfer will always be the same.
         * 
         * Look up what it is, and map to a PrepareResultType
         */
        if (error === CreateTransferError.exists ||
          error === CreateTransferError.exists_with_different_timeout) {
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
                  ${lookupTransferResult.type} after encountering ${error}. This should not be \
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
        }

        // unhandled TigerBeetle Error
        const readableError = CreateTransferError[error]

        return {
          type: PrepareResultType.FAIL_OTHER,
          error: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
            `prepare failed with error: ${readableError}`
          )
        }
      }

      return {
        type: PrepareResultType.PASS
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

    const prepareId = Helper.fromMojaloopId(input.transferId)
    const transfer: Transfer = {
      ...Helper.createTransferTemplate,
      id: id(),
      debit_account_id: 0n,
      credit_account_id: 0n,
      amount: 0n,
      pending_id: prepareId,
      ledger: 0,
      // ledger: clearingLedgerId,
      code: 1,
      flags: TransferFlags.void_pending_transfer,
    }
    const error = await this.deps.transferBatcher.enqueueTransfer(transfer)
    if (error) {
      const readableError = CreateTransferError[error]
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
  }

  // TODO(LD): Make this interface batch compatible. This will require the new handlers to be able 
  // to read multiple messages from Kafka at the same point.
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
      // TODO: this SHOULD be in cache if we use the Kafka key partitioning properly. We should add
      // some observability here to catch misconfiguration errors
      const transferSpecResults = await this.deps.specStore.lookupTransferSpec([input.transferId])
      assert(transferSpecResults.length === 1, `expected transfer spec for id: ${input.transferId}`)
      const transferSpec = transferSpecResults[0]
      assert(transferSpec.type === 'SpecTransfer')
      const clearingLedgerId = this.currencyManager.getClearingLedgerId(transferSpec.currency)

      await this.deps.specStore.saveTransferSpec([
        {
          ...transferSpec,
          fulfilment: input.payload.fulfilment,
        }
      ])
      const prepareId = Helper.fromMojaloopId(input.transferId)

      // Validate that the fulfilment matches the condition
      const fulfilmentAndConditionResult = Helper.validateFulfilmentAndCondition(
        input.payload.fulfilment, transferSpec.condition
      )
      if (fulfilmentAndConditionResult.type === 'FAIL') {
        const transfer: Transfer = {
          ...Helper.createTransferTemplate,
          id: id(),
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: amount_max,
          pending_id: prepareId,
          ledger: clearingLedgerId,
          code: 1,
          flags: TransferFlags.void_pending_transfer,
        }

        const error = await this.deps.transferBatcher.enqueueTransfer(transfer)
        if (error) {
          const readableError = CreateTransferError[error]
          return {
            type: FulfilResultType.FAIL_OTHER,
            error: ErrorHandler.Factory.createFSPIOPError(
              ErrorHandler.Enums.FSPIOPErrorCodes.VALIDATION_ERROR,
              `encountered unexpected error when voiding transfer after invalid fulfilment`
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

      /**
       * Dr Payer_Clearing
       *  Cr Payee_Clearing
       * Flags: post_pending_transfer
       */
      const transfer: Transfer = {
        ...Helper.createTransferTemplate,
        id: id(),
        debit_account_id: 0n,
        credit_account_id: 0n,
        amount: amount_max,
        pending_id: prepareId,
        timeout: 0,
        ledger: clearingLedgerId,
        code: 1,
        flags: TransferFlags.post_pending_transfer,
      }

      const error = await this.deps.transferBatcher.enqueueTransfer(transfer)
      if (error) {
        const readableError = CreateTransferError[error]
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
  public async lookupTransfer(query: LookupTransferQuery): Promise<LookupTransferQueryResponse> {
    const prepareId = Helper.fromMojaloopId(query.transferId)

    // look up all TigerBeetle Transfers related to this MojaloopId
    const relatedTransfers = await this.deps.client.queryTransfers({
      user_data_128: prepareId,
      user_data_64: 0n,
      user_data_32: 0,
      ledger: 0,
      code: 1,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: 3,
      flags: 0
    })

    if (relatedTransfers.length === 0) {
      return {
        type: LookupTransferResultType.NOT_FOUND,
      }
    }

    if (relatedTransfers.length === 1) {
      const pendingTransfer = relatedTransfers[0]
      assert(pendingTransfer)
      assert(pendingTransfer.flags & TransferFlags.pending)
      // TigerBeetle timeout is defined in seconds
      const timeoutNs = BigInt(pendingTransfer.timeout) * 1_000_000_000n;
      const createdAt = pendingTransfer.timestamp
      const expiredAt = createdAt + timeoutNs

      /**
       * TODO(LD): There could be clock mismatch errors here, since we are using our own time
       *   instead of the TigerBeetle time, we don't know 100% for sure that TigerBeetle has
       *   actually timed out the transfer.
       */
      const nowNs = BigInt(Date.now()) * 1_000_000_000n
      const expiredAtMs = convertBigIntToNumber(expiredAt / 1_000_000n)
      if (expiredAt > nowNs) {
        return {
          type: LookupTransferResultType.FOUND_FINAL,
          finalizedTransfer: {
            completedTimestamp: (new Date(expiredAtMs)).toISOString(),
            transferState: "ABORTED"
          }
        }
      }

      return {
        type: LookupTransferResultType.FOUND_NON_FINAL
      }
    }

    if (relatedTransfers.length > 2) {
      return {
        type: LookupTransferResultType.FAILED,
        error: ErrorHandler.Factory.createInternalServerFSPIOPError(
          `Found: ${relatedTransfers.length} related transfers. Expected at most 2.`
        )
      }
    }

    let pendingTransfer: Transfer;
    let finalTransfer: Transfer;
    if (relatedTransfers[0].id === prepareId) {
      [pendingTransfer, finalTransfer] = relatedTransfers
    } else if (relatedTransfers[1].id === prepareId) {
      [finalTransfer, pendingTransfer] = relatedTransfers
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
        await this.createOpeningBookmarkTransfer()

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

  private async createOpeningBookmarkTransfer(): Promise<void> {
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
      throw new Error(`createOpeningBookmarkTransfer() - encountered fatal error when creating bookmark control accounts\n${fatalAccountErrors.join(',')}`)
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
      throw new Error(`createOpeningBookmarkTransfer() - encountered fatal error when creating opening bookmark\n${fatalTransferErrors.join(',')}`)
    }
  }

  /**
   * Settlement Methods
   */

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
      // TODO(LD): Add more account types, especially the special accounts for e.g. DFSP active/inactive, or 
      const keys = [
        // buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Dfsp),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Deposit),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Unrestricted),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Unrestricted_Lock),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Restricted),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Reserved),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Committed_Outgoing),
        buildKey(specAccount.dfspId, specAccount.currency, AccountCode.Net_Debit_Cap),
      ]
      const ids = [
        specAccount.deposit,
        specAccount.unrestricted,
        specAccount.unrestrictedLock,
        specAccount.restricted,
        specAccount.reserved,
        specAccount.commitedOutgoing,
        specAccount.netDebitCap,
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
    // const assetScale = this._assetScaleForCurrency(currency)
    // const valueDivisor = 10 ** assetScale

    const accountUnrestricted: InternalLedgerAccount = input.find(acc => acc.accountCode === AccountCode.Unrestricted)
    assert(accountUnrestricted, 'could not find unrestricted account')

    const accountDeposit: InternalLedgerAccount = input.find(acc => acc.accountCode === AccountCode.Deposit)
    assert(accountDeposit, 'could not find deposit account')

    // Legacy Settlement Balance: How much Dfsp has available to settle.
    // Was a negative number in the legacy API once the dfsp had deposited funds.
    const legacySettlementBalancePosted = (accountDeposit.debits_posted - accountDeposit.credits_posted) * BigInt(-1)
    const legacySettlementBalancePending = (accountDeposit.debits_pending - accountDeposit.credits_pending) * BigInt(-1)

    // Legacy Position Balance: How much Dfsp is owed or how much this Dfsp owes.
    // TODO(LD): I think we need to add together Unrestricted + Restricted
    const clearingBalancePosted = accountUnrestricted.credits_posted - accountUnrestricted.debits_posted
    // TODO(LD): This doesn't make any more sense, since we won't use pending/posted
    // instead this should be the net credit balance of the Reserved account
    const clearingBalancePending = accountUnrestricted.credits_pending - accountUnrestricted.debits_pending
    const legacyPositionBalancePosted = (legacySettlementBalancePosted + clearingBalancePosted) * BigInt(-1)
    const legacyPositionBalancePending = (legacySettlementBalancePending + clearingBalancePending) * BigInt(-1)

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
      value: Helper.toRealAmount(legacySettlementBalancePosted, assetScale),
      reservedValue: Helper.toRealAmount(legacySettlementBalancePending, assetScale),

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
      const ledgerAccount: LedgerAccount = {
        id: acc.id,
        ledgerAccountType: acc.code.toString(),
        currency: acc.currency,
        status: (acc.flags & AccountFlags.closed) && 'DISABLED' || 'ENABLED',
        netCreditsPending: Helper.toRealAmount(acc.credits_pending - acc.debits_pending, assetScale),
        netDebitsPending: Helper.toRealAmount(acc.debits_pending - acc.credits_pending, assetScale),
        netCreditsPosted: Helper.toRealAmount(acc.credits_posted - acc.debits_posted, assetScale),
        netDebitsPosted: Helper.toRealAmount(acc.debits_posted - acc.credits_posted, assetScale)
      }

      return ledgerAccount
    })

  }

  private _assetScaleForCurrency(currency: string): number {
    const matchingCurrencyConfigs = this.deps.config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS
      .filter(c => c.currency === currency)
    assert(matchingCurrencyConfigs.length > 0, `_assetScaleForCurrency - could not find currency: ${currency}`)
    assert(matchingCurrencyConfigs.length < 2, `_assetScaleForCurrency - found more than 1 entry for currency: ${currency}`)

    const currencyConfig = matchingCurrencyConfigs[0]
    assert(typeof currencyConfig.assetScale, 'number')
    assert(currencyConfig.assetScale >= 0, 'Expected assetScale to be greater or equal to than 0')

    return currencyConfig.assetScale
  }
}