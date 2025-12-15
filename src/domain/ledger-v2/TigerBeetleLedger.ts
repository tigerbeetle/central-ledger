import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import assert from "assert";
import Crypto from 'node:crypto';
import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler";
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { ApplicationConfig } from "src/shared/config";
import { Account, AccountFlags, amount_max, Client, CreateAccountError, CreateTransferError, id, QueryFilter, QueryFilterFlags, Transfer, TransferFlags } from 'tigerbeetle-node';
import { convertBigIntToNumber } from "../../shared/config/util";
import { logger } from '../../shared/logger';
import { Ledger } from "./Ledger";
import { DfspAccountIds, SpecAccount, SpecDfsp, SpecStore } from "./SpecStore";
import { TransferBatcher } from "./TransferBatcher";
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
  GetNetDebitCapQuery,
  HubAccountResponse,
  LedgerDfsp,
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
} from "./types";
import { Enum } from '@mojaloop/central-services-shared';
import { QueryResult } from 'src/shared/results';
import Helper from './TigerBeetleLedgerHelper';

export interface TigerBeetleLedgerDependencies {
  config: ApplicationConfig
  client: Client
  specStore: SpecStore
  transferBatcher: TransferBatcher
  participantService: {
    create: (payload: { name: string, isProxy?: boolean }) => Promise<number>
    getById: (id: number) => Promise<{ participantId: number, name: string, isActive: boolean, createdDate: Date, currencyList: any[], isProxy?: boolean }>
  }
}

// reserved for USD
export const LedgerIdUSD = 100
export const LedgerIdTimeoutHandler = 9000
export const LedgerIdSuper = 9001

const NS_PER_MS = 1_000_000n
const NS_PER_SECOND = NS_PER_MS * 1_000n

// TODO(LD): rename DfspAccountType>
export enum AccountType {
  Collateral = 1,
  Liquidity = 2,
  Clearing = 3,
  Settlement_Multilateral = 4,
}

interface InterledgerValidationPass {
  type: 'PASS'
}

interface InterledgerValidationFail {
  type: 'FAIL',
  reason: string
}

export type InterledgerValidationResult = InterledgerValidationPass
  | InterledgerValidationFail


/**
 * An internal representation of an Account, combined with spec
 */
interface InternalLedgerAccount extends Account {
  dfspId: string,
  currency: string,
  accountType: AccountType,
}

interface InternalMasterAccount extends Account {
  dfspId: string,
}

export default class TigerBeetleLedger implements Ledger {
  constructor(private deps: TigerBeetleLedgerDependencies) { }

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
   * Unfortunately the interface from LegacyCompatibleLedger is a little constraining here.
   * In LegacyCompatibleLedger, the limit (e.g. liquidity account value) can be set before depositing
   * collateral. Whereas in this ledger, attempting to set the limit without depositing collateral
   * is impossible. For now, we will simply workaround this problem by doing a:
   * 
   * Dr Collateral 
   *  Cr Reserve 
   * Dr Reserve
   *  Cr Liquidity
   * 
   * Based on the `startingDeposits` in CreateDfspCommand.
   */
  public async createDfsp(cmd: CreateDfspCommand): Promise<CreateDfspResponse> {
    try {
      assert(cmd.dfspId)

      // Get or create the SpecDfsp
      const masterAccountId = await this._getOrCreateSpecDfsp(cmd.dfspId)

      assert.equal(cmd.currencies.length, 1, 'Currently only 1 currency is supported')
      this._assertCurrenciesEnabled(cmd.currencies)
      assert.equal(cmd.startingDeposits.length, cmd.currencies.length)

      const currency = cmd.currencies[0]
      const collateralAmount = cmd.startingDeposits[0]
      assert(Number.isInteger(collateralAmount))
      assert(collateralAmount >= 0)

      // Lookup the dfsp first, ensure it's been correctly created
      const accountSpec = await this.deps.specStore.getAccountSpec(cmd.dfspId, currency)
      if (accountSpec.type === "SpecAccount") {

        const accounts = await this.deps.client.lookupAccounts([
          accountSpec.collateral,
          accountSpec.liquidity,
          accountSpec.clearing,
          accountSpec.settlementMultilateral,
        ]);
        if (accounts.length === 4) {
          return {
            type: 'ALREADY_EXISTS'
          }
        }

        // We have a partial save of accounts, that means spec store and TigerBeetle are out of
        // sync. We simply continue here and allow new accounts to be created in TigerBeetle, and
        // the partial accounts to be ignored in the spec store
        logger.warn(`createDfsp() - found only ${accounts.length} of expected 4 for dfsp: 
        ${cmd.dfspId} and currency: ${currency}. Overwriting old accounts.`)

        // TODO:
        // This is potentially dangerous because somebody could tamper with the spec store by
        // inserting an invalid id, and calling `createDfsp` again. It would be better to be able to 
        // look up a Dfsp's accounts based on a query filter on TigerBeetle itself.
      }

      // TigerBeetle Accounts can be 128 bits, but since the Admin API uses javascript/json numbers
      // to maintain backwards compatibility, we generate our own random accountIds under 
      // Number.MAX_SAFE_INTEGER to be safe.
      const accountIds: DfspAccountIds = {
        collateral: Helper.id53(),
        liquidity: Helper.id53(),
        clearing: Helper.id53(),
        settlementMultilateral: Helper.id53(),
      }

      const accounts: Array<Account> = [
        // Master account. Keeps track of Dfsp active/not active and creation timestamp
        {
          id: BigInt(masterAccountId),
          debits_pending: 0n,
          debits_posted: 0n,
          credits_pending: 0n,
          credits_posted: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          reserved: 0,
          ledger: LedgerIdSuper,
          code: AccountType.Collateral,
          flags: 0,
          timestamp: 0n,
        },
        // Collateral Account. Funds Switch holds in security to ensure Dfsp meets it's obligations
        {
          id: accountIds.collateral,
          debits_pending: 0n,
          debits_posted: 0n,
          credits_pending: 0n,
          credits_posted: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          reserved: 0,
          ledger: LedgerIdUSD,
          code: AccountType.Collateral,
          flags: AccountFlags.linked | AccountFlags.credits_must_not_exceed_debits,
          timestamp: 0n,
        },
        // Liquidity Account. Depositing Collateral unlocks liquidity that a Dfsp can use to make
        // commitments to other Dfsps.
        {
          id: accountIds.liquidity,
          debits_pending: 0n,
          debits_posted: 0n,
          credits_pending: 0n,
          credits_posted: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          reserved: 0,
          ledger: LedgerIdUSD,
          code: AccountType.Liquidity,
          flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
          timestamp: 0n,
        },
        // Clearing Account. Payments from this Dfsp where Dfsp is Payer are debits, payments to this
        // Dfsp where Dfsp is Payee, are credits.
        {
          id: accountIds.clearing,
          debits_pending: 0n,
          debits_posted: 0n,
          credits_pending: 0n,
          credits_posted: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          reserved: 0,
          ledger: LedgerIdUSD,
          code: AccountType.Clearing,
          flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
          timestamp: 0n,
        },
        // Settlement_Multilateral. Records the settlement obligations that this Dfsp holds
        // to other Dfsps in the scheme.
        {
          id: accountIds.settlementMultilateral,
          debits_pending: 0n,
          debits_posted: 0n,
          credits_pending: 0n,
          credits_posted: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          reserved: 0,
          ledger: LedgerIdUSD,
          code: AccountType.Settlement_Multilateral,
          flags: AccountFlags.debits_must_not_exceed_credits,
          timestamp: 0n,
        }
      ]

      await this.deps.specStore.associateAccounts(cmd.dfspId, currency, accountIds)
      const createAccountsErrors = await this.deps.client.createAccounts(accounts)

      let failed = false
      const readableErrors = []
      createAccountsErrors.forEach((error, idx) => {
        // ignore exists error for masterAccount
        if (error.index === 0 && error.result === CreateAccountError.exists) {
          return
        }

        readableErrors.push(CreateAccountError[error.result])
        console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`)
        failed = true
      })

      if (failed) {
        // if THIS fails, then we have dangling entries in the database
        await this.deps.specStore.tombstoneAccounts(cmd.dfspId, currency, accountIds)

        return {
          type: 'FAILURE',
          error: new Error(`LedgerError: ${readableErrors.join(',')}`)
        }
      }
      assert.strictEqual(createAccountsErrors.length, 0)


      // TODO: we should adjust the command to make it an amount string
      const collateralAmountStr = `${collateralAmount}`
      const amount = TigerBeetleLedger.fromMojaloopAmount(collateralAmountStr, 2)

      // Now deposit collateral and unlock liquidity, as well as make funds available for clearing.
      //
      // Dr Collateral 
      //  Cr Liquidity 
      // Dr Liquidity
      //  Cr Clearing
      //
      const transfers = [
        {
          id: id(),
          debit_account_id: accounts[0].id,
          credit_account_id: accounts[1].id,
          amount,
          pending_id: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          ledger: LedgerIdUSD,
          code: 1,
          flags: TransferFlags.linked,
          timestamp: 0n,
        },
        {
          id: id(),
          debit_account_id: accounts[1].id,
          credit_account_id: accounts[2].id,
          amount,
          pending_id: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          ledger: LedgerIdUSD,
          code: 1,
          flags: 0,
          timestamp: 0n,
        }
      ]
      if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
        return {
          type: 'SUCCESS'
        }
      }
      const createTransferErrors = await this.deps.client.createTransfers(transfers);

      for (const error of createTransferErrors) {
        readableErrors.push(CreateTransferError[error.result])
        console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`)
        failed = true
      }

      if (failed) {
        return {
          type: 'FAILURE',
          error: new Error(`LedgerError: ${readableErrors.join(',')}`)
        }
      }

      assert.strictEqual(createTransferErrors.length, 0)

      // Create the participant in the legacy system
      // TODO: this is required when running properly, but causes problems in test
      // indeed we should actually create the participant first before creating the account in 
      // TigerBeetle
      // await this.deps.participantService.create({ name: cmd.dfspId })

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
    logger.info('disableDfsp() - noop')

    // TODO: look up the SpecDfsp, get the account id and close the account!

    return {
      type: 'SUCCESS',
      result: undefined
    }
  }

  public async enableDfsp(cmd: { dfspId: string }): Promise<CommandResult<void>> {
    logger.info('enableDfsp() - noop')

    // TODO: look up the SpecDfsp, get the account id and reopen the account! We probably need
    // to store the closing id somehwere

    return {
      type: 'SUCCESS',
      result: undefined
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
    logger.warn('deposit() - noop')

    return {
      type: 'SUCCESS'
    }
  }

  public async withdrawPrepare(cmd: WithdrawPrepareCommand): Promise<WithdrawPrepareResponse> {
    throw new Error('not implemented')
  }

  public async withdrawCommit(cmd: WithdrawCommitCommand): Promise<WithdrawCommitResponse> {
    throw new Error('not implemented')
  }

  public async setNetDebitCap(cmd: SetNetDebitCapCommand): Promise<CommandResult<void>> {
    throw new Error('Method not implemented.');
  }

  public async getDfsp(query: { dfspId: string; }): Promise<QueryResult<LedgerDfsp>> {
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

      const ledgerDfsp: LedgerDfsp = {
        name: query.dfspId,
        isActive: (masterAccount.flags & AccountFlags.closed) === 1,
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
          isActive: (masterAccount.flags & AccountFlags.closed) === 1,
          created: new Date(Number(masterAccount.timestamp / NS_PER_MS)),
          accounts: dfspLegacyAccounts
        } as LedgerDfsp;
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



  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<QueryResult<LegacyLimit>> {
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
    const tbAccountIds = [
      // TODO: we need to define the limit as an account in TigerBeetle
      ids.collateral,
    ]
    const tbAccounts = await this.deps.client.lookupAccounts(tbAccountIds)
    if (tbAccounts.length !== tbAccountIds.length) {
      return {
        type: 'FAILURE',
        error: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
          `getNetDebitCap() failed - expected ${tbAccountIds.length} accounts from \
          client.lookupAccounts(), but instead found: ${tbAccounts.length}.`.replace(/\s+/g, ' ')
        )
      }
    }

    const limit: LegacyLimit = {
      type: "NET_DEBIT_CAP",
      // TODO(LD): load from the tigerbeetle account
      value: 100000,
      alarmPercentage: 0
    }

    return {
      type: 'SUCCESS',
      result: limit
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

      const prepareId = TigerBeetleLedger.fromMojaloopId(input.payload.transferId)
      const amount = TigerBeetleLedger.fromMojaloopAmount(amountStr, 2)

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
          payerId: input.payload.payerFsp,
          payeeId: input.payload.payeeFsp,
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
        id: prepareId,
        debit_account_id: payerSpec.clearing,
        credit_account_id: payeeSpec.clearing,
        amount,
        pending_id: 0n,
        // Also used as a correlation to map between Mojaloop Transfers (1) ---- (*) TigerBeetle Transfers
        user_data_128: prepareId,
        user_data_64: 0n,
        user_data_32: 0,
        timeout: timeoutSeconds,
        ledger: LedgerIdUSD,
        code: 1,
        flags: TransferFlags.pending,
        timestamp: 0n
      }

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

    const prepareId = TigerBeetleLedger.fromMojaloopId(input.transferId)
    const transfer: Transfer = {
      id: id(),
      debit_account_id: 0n,
      credit_account_id: 0n,
      amount: 0n,
      pending_id: prepareId,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: LedgerIdUSD,
      code: 1,
      flags: TransferFlags.void_pending_transfer,
      timestamp: 0n
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

      await this.deps.specStore.saveTransferSpec([
        {
          ...transferSpec,
          fulfilment: input.payload.fulfilment,
        }
      ])
      const prepareId = TigerBeetleLedger.fromMojaloopId(input.transferId)

      // Validate that the fulfilment matches the condition
      const fulfilmentAndConditionResult = TigerBeetleLedger.validateFulfilmentAndCondition(
        input.payload.fulfilment, transferSpec.condition
      )
      if (fulfilmentAndConditionResult.type === 'FAIL') {
        const transfer: Transfer = {
          id: id(),
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: amount_max,
          pending_id: prepareId,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          ledger: LedgerIdUSD,
          code: 1,
          flags: TransferFlags.void_pending_transfer,
          timestamp: 0n
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
        id: id(),
        debit_account_id: 0n,
        credit_account_id: 0n,
        amount: amount_max,
        pending_id: prepareId,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        timeout: 0,
        ledger: LedgerIdUSD,
        code: 1,
        flags: TransferFlags.post_pending_transfer,
        timestamp: 0n
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
    const prepareId = TigerBeetleLedger.fromMojaloopId(query.transferId)

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
   * 
   * 
   */
  public async sweepTimedOut(): Promise<SweepResult> {
    const MAX_TRANSFERS_IN_PAGE = 8000
    const MAX_PAGES = 10

    try {
      const bookmarkQuery: QueryFilter = {
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        ledger: LedgerIdTimeoutHandler,
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
        ledger: LedgerIdUSD,
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
      const specs = await this.deps.specStore.lookupTransferSpec(timedOutTransfers.map(t => TigerBeetleLedger.toMojaloopId(t.id)))
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
          id: id(),
          debit_account_id: 1000n,
          credit_account_id: 1001n,
          amount: 0n,
          pending_id: openingBookmark.id,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          ledger: LedgerIdTimeoutHandler,
          code: 9000,
          flags: TransferFlags.void_pending_transfer | TransferFlags.linked,
          timestamp: 0n
        },
        // Open a new bookmark
        {
          id: id(),
          debit_account_id: 1000n,
          credit_account_id: 1001n,
          amount: 0n,
          pending_id: 0n,
          user_data_128: 0n,
          user_data_64: newOpeningTimestamp,
          user_data_32: 0,
          timeout: 0,
          ledger: LedgerIdTimeoutHandler,
          code: 9000,
          flags: TransferFlags.pending,
          timestamp: 0n
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
        // TODO(LD): Find better account ids
        id: 1000n,
        debits_pending: 0n,
        debits_posted: 0n,
        credits_pending: 0n,
        credits_posted: 0n,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        reserved: 0,
        ledger: LedgerIdTimeoutHandler,
        code: 9000,
        flags: 0,
        timestamp: 0n
      },
      {
        id: 1001n,
        debits_pending: 0n,
        debits_posted: 0n,
        credits_pending: 0n,
        credits_posted: 0n,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        reserved: 0,
        ledger: LedgerIdTimeoutHandler,
        code: 9000,
        flags: 0,
        timestamp: 0n
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
      id: id(),
      debit_account_id: 1000n,
      credit_account_id: 1001n,
      amount: 0n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: LedgerIdTimeoutHandler,
      code: 9000,
      flags: TransferFlags.pending,
      timestamp: 0n
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

    const accountId = Helper.id53()
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
    const buildKey = (dfspId: string, currency: string, accountType: AccountType) => `${dfspId};${currency};${accountType}`
    const dfspIdMap: Record<string, null> = {}
    const accountKeys: Array<string> = []
    const accountIds: Array<bigint> = []

    specAccounts.forEach(specAccount => {
      dfspIdMap[specAccount.dfspId] = null
      // TODO(LD): Add more account types, especially the special accounts for e.g. DFSP active/inactive, or 
      const keys = [
        buildKey(specAccount.dfspId, specAccount.currency, AccountType.Clearing),
        buildKey(specAccount.dfspId, specAccount.currency, AccountType.Collateral),
        buildKey(specAccount.dfspId, specAccount.currency, AccountType.Liquidity),
        buildKey(specAccount.dfspId, specAccount.currency, AccountType.Settlement_Multilateral),
      ]
      const ids = [
        specAccount.clearing,
        specAccount.collateral,
        specAccount.liquidity,
        specAccount.settlementMultilateral,
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
      const [dfspId, currency, accountTypeStr] = key.split(';')
      assert(dfspId)
      assert(currency)
      assert(accountTypeStr)
      const accountType = parseInt(accountTypeStr) as AccountType
      const tigerbeetleAccount = accountResult.result[idx]

      internalLedgerAccounts.push({
        dfspId,
        currency,
        accountType,
        ...tigerbeetleAccount
      })
    }

    return internalLedgerAccounts
  }

  /**
   * @description Map from an internal TigerBeetle Ledger representation of a LedgerAccount to a backwards compatible 
   * representation
   */
  private _fromInternalAccountsToLegacyLedgerAccounts(input: Array<InternalLedgerAccount>): Array<LegacyLedgerAccount> {
    const accounts: Array<LegacyLedgerAccount> = []
    const currencies = [...new Set(input.map(item => item.currency))]
    input.map(internalAccount => internalAccount.currency)
    assert.equal(currencies.length, 1, '_fromInternalAccountsToLegacyLedgerAccounts expects accounts of only 1 currency at a time.')
    const currency = currencies[0]
    const assetScale = this._assetScaleForCurrency(currency)
    const valueDivisor = 10 ** assetScale

    const clearingAccount: InternalLedgerAccount = input.find(acc => acc.accountType === AccountType.Clearing)
    const collateralAccount: InternalLedgerAccount = input.find(acc => acc.accountType === AccountType.Collateral)
    assert(clearingAccount, 'could not find clearing account')
    assert(collateralAccount, 'could not find colateral account')

    // Legacy Settlement Balance: How much Dfsp has available to settle.
    // Was a negative number in the legacy API once the dfsp had deposited funds.
    const legacySettlementBalancePosted = (collateralAccount.debits_posted - collateralAccount.credits_posted) * BigInt(-1)
    const legacySettlementBalancePending = (collateralAccount.debits_pending - collateralAccount.credits_pending) * BigInt(-1)

    // Legacy Position Balance: How much Dfsp is owed or how much this Dfsp owes.
    const clearingBalancePosted = clearingAccount.credits_posted - clearingAccount.debits_posted
    const clearingBalancePending = clearingAccount.credits_pending - clearingAccount.debits_pending
    const legacyPositionBalancePosted = (legacySettlementBalancePosted + clearingBalancePosted) * BigInt(-1)
    const legacyPositionBalancePending = (legacySettlementBalancePending + clearingBalancePending) * BigInt(-1)

    accounts.push({
      id: clearingAccount.id,
      ledgerAccountType: 'POSITION',
      currency,
      isActive: !(clearingAccount.flags & AccountFlags.closed),
      value: convertBigIntToNumber(legacyPositionBalancePosted) / valueDivisor,
      reservedValue: convertBigIntToNumber(legacyPositionBalancePending) / valueDivisor,
      // We don't have this in TigerBeetle, although we could use the created date
      changedDate: new Date(0)
    })

    accounts.push({
      id: collateralAccount.id,
      ledgerAccountType: 'SETTLEMENT',
      currency,
      isActive: !(collateralAccount.flags & AccountFlags.closed),
      value: convertBigIntToNumber(legacySettlementBalancePosted) / valueDivisor,
      reservedValue: convertBigIntToNumber(legacySettlementBalancePending) / valueDivisor,
      // We don't have this in TigerBeetle, although we could use the created date
      changedDate: new Date(0)
    })

    return accounts;
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

  /**
   * Check that the currencies are enabled on the switch, throw if they are not
   */
  private _assertCurrenciesEnabled(currencies: Array<string>) {
    const configCurrencyLedgers = this.deps.config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS
    const enabledCurrencies = [...new Set(configCurrencyLedgers.map(cl => cl.currency))]
    currencies.forEach(currency => {
      if (enabledCurrencies.indexOf(currency) === -1) {
        throw new Error(`_assertCurrenciesEnabled - currency ${currency} not enabled in config.`)
      }
    })
    return
  }

  /**
   * Utility Methods
   */
  public static fromMojaloopId(mojaloopId: string): bigint {
    assert(mojaloopId)
    // TODO: assert that this actually is a uuid

    const hex = mojaloopId.replace(/-/g, '');
    return BigInt(`0x${hex}`);
  }

  public static toMojaloopId(id: bigint): string {
    assert(id !== undefined && id !== null, 'id is required')

    // Convert bigint to hex string (without 0x prefix)
    let hex = id.toString(16);

    // Pad to 32 characters (128 bits = 16 bytes = 32 hex chars)
    hex = hex.padStart(32, '0');

    // Insert dashes to create UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  }

  /**
   * Converts a Mojaloop amount string to a bigint representation based on the currency's scale
   */
  public static fromMojaloopAmount(amountStr: string, currencyScale: number): bigint {
    assert(currencyScale >= 0)
    assert(currencyScale <= 10)
    // Validate input
    if (typeof amountStr !== 'string') {
      throw new Error('Amount must be a string');
    }

    if (!/^-?\d+(\.\d+)?$/.test(amountStr.trim())) {
      throw new Error('Invalid amount format. Expected format: "123.45" or "123"');
    }

    const trimmed = amountStr.trim();
    const [integerPart, decimalPart = ''] = trimmed.split('.');

    const normalizedDecimal = decimalPart.padEnd(currencyScale, '0').slice(0, currencyScale);
    const combinedStr = integerPart + normalizedDecimal;

    return BigInt(combinedStr);
  }

  /**
   * Checks that the fulfilment matches the preimage
   * 
   * From the Mojaloop FSPIOP Specification v1.1:
   * https://docs.mojaloop.io/api/fspiop/v1.1/api-definition.html#interledger-payment-request-2
   * 
   * > The fulfilment is submitted to the Payee FSP ledger to instruct the ledger to commit the 
   * > reservation in favor of the Payee. The ledger will validate that the SHA-256 hash of the
   * > fulfilment matches the condition attached to the transfer. If it does, it commits the 
   * > reservation of the transfer. If not, it rejects the transfer and the Payee FSP rejects the 
   * > payment and cancels the previously-performed reservation.
   * 
   */
  public static validateFulfilmentAndCondition(fulfilment: string, condition: string): InterledgerValidationResult {
    try {
      assert(fulfilment)
      assert(condition)
      const preimage = Buffer.from(fulfilment, 'base64url')
      if (preimage.length !== 32) {
        return {
          type: 'FAIL',
          reason: 'Interledger preimages must be exactly 32 bytes'
        }
      }

      const calculatedCondition = Crypto.createHash('sha256')
        .update(preimage)
        .digest('base64url')

      if (calculatedCondition !== condition) {
        return {
          type: 'FAIL',
          reason: 'Condition and Fulfulment mismatch'
        }
      }

      return {
        type: 'PASS'
      }
    } catch (err) {
      return {
        type: "FAIL",
        reason: err.message
      }
    }
  }
}