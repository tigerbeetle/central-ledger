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
import { DfspAccountIds, MetadataStore } from "./MetadataStore";
import { TransferBatcher } from "./TransferBatcher";
import {
  CreateDFSPCommand,
  CreateDFSPResponse,
  CreateHubAccountCommand,
  CreateHubAccountResponse,
  DepositCollateralCommand,
  DepositCollateralResponse,
  DFSPAccountResponse,
  FulfilResult,
  FulfilResultType,
  GetDFSPAccountsQuery,
  GetNetDebitCapQuery,
  LegacyLedgerAccount,
  LegacyLimit,
  LookupTransferQuery,
  LookupTransferQueryResponse,
  LookupTransferResultType,
  NetDebitCapResponse,
  PrepareResult,
  PrepareResultType,
  SweepResult,
  TimedOutTransfer
} from "./types";
import { Enum } from '@mojaloop/central-services-shared';

export interface TigerBeetleLedgerDependencies {
  config: ApplicationConfig
  client: Client
  metadataStore: MetadataStore
  transferBatcher: TransferBatcher
  participantService: {
    create: (payload: { name: string, isProxy?: boolean }) => Promise<number>
    getById: (id: number) => Promise<{ participantId: number, name: string, isActive: boolean, createdDate: Date, currencyList: any[], isProxy?: boolean }>
  }
}

// reserved for USD
export const LedgerIdUSD = 100
export const LedgerIdTimeoutHandler = 9000

// 1 second = 1,000,000,000 nanoseconds (1 billion)
const NS_PER_SECOND = 1_000_000_000n

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


export default class TigerBeetleLedger implements Ledger {
  constructor(private deps: TigerBeetleLedgerDependencies) {

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
    // but instead use separate collateral accounts for each DFSP + Currency combination
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
   * Based on the `initialLimits` in CreateDFSPCommand.
   */
  public async createDfsp(cmd: CreateDFSPCommand): Promise<CreateDFSPResponse> {
    assert(cmd.dfspId)
    assert.equal(cmd.currencies.length, 1, 'Currently only 1 currency is supported')
    assert.equal(cmd.currencies[0], 'USD', 'Currently only USD is supported.')
    assert.equal(cmd.initialLimits.length, cmd.currencies.length)

    const currency = cmd.currencies[0]
    const collateralAmount = cmd.initialLimits[0]
    assert(Number.isInteger(collateralAmount))
    assert(collateralAmount >= 0)

    // Lookup the dfsp first, ensure it's been correctly created
    const accountMetadata = await this.deps.metadataStore.getDfspAccountMetadata(cmd.dfspId, currency)
    if (accountMetadata.type === "DfspAccountMetadata") {

      const accounts = await this.deps.client.lookupAccounts([
        accountMetadata.collateral,
        accountMetadata.liquidity,
        accountMetadata.clearing,
        accountMetadata.settlementMultilateral,
      ]);
      if (accounts.length === 4) {
        return {
          type: 'ALREADY_EXISTS'
        }
      }

      // We have a partial save of accounts, that means metadata store and TigerBeetle are out of
      // sync. We simply continue here and allow new accounts to be created in TigerBeetle, and
      // the partial accounts to be ignored in the metadata store
      logger.warn(`createDfsp() - found only ${accounts.length} of expected 4 for dfsp: 
        ${cmd.dfspId} and currency: ${currency}. Overwriting old accounts.`)

      // TODO:
      // This is potentially dangerous because somebody could tamper with the metadata store by
      // inserting an invalid id, and calling `createDfsp` again. It would be better to be able to 
      // look up a DFSP's accounts based on a query filter on TigerBeetle itself.
    }

    const accountIds: DfspAccountIds = {
      collateral: id(),
      liquidity: id(),
      clearing: id(),
      settlementMultilateral: id()
    }

    const accounts: Array<Account> = [
      // Collateral Account. Funds Switch holds in security to ensure DFSP meets it's obligations
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
      // Liquidity Account. Depositing Collateral unlocks liquidity that a DFSP can use to make
      // commitments to other DFSPs.
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
      // Clearing Account. Payments from this DFSP where DFSP is Payer are debits, payments to this
      // DFSP where DFSP is Payee, are credits.
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
      // Settlement_Multilateral. Records the settlement obligations that this DFSP holds
      // to other DFSPs in the scheme.
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

    await this.deps.metadataStore.associateDfspAccounts(cmd.dfspId, currency, accountIds)
    const createAccountsErrors = await this.deps.client.createAccounts(accounts)

    let failed = false
    const readableErrors = []
    for (const error of createAccountsErrors) {
      readableErrors.push(CreateAccountError[error.result])
      console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`)
      failed = true
    }

    if (failed) {
      // if THIS fails, then we have dangling entries in the database
      await this.deps.metadataStore.tombstoneDfspAccounts(cmd.dfspId, currency, accountIds)

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
    await this.deps.participantService.create({ name: cmd.dfspId })

    return {
      type: 'SUCCESS'
    }
  }

  public async disableDfsp(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  public async enableDfsp(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  // TODO(LD): Come back to the design on this one. I'm a little unsure about how to handle the
  // mismatch between single entry accounting in the original ledger, and double entry here.
  public async depositCollateral(cmd: DepositCollateralCommand): Promise<DepositCollateralResponse> {
    logger.warn('depositCollateral() - noop')

    return {
      type: 'SUCCESS'
    }
  }

  public async withdrawCollateral(thing: unknown): Promise<unknown> {
    throw new Error('not implemented')
  }

  /**
   * @method getAccounts
   * @description Lookup the accounts for a DFSP + Currency
   */
  public async getAccounts(query: GetDFSPAccountsQuery): Promise<DFSPAccountResponse> {
    const ids = await this.deps.metadataStore.getDfspAccountMetadata(query.dfspId, query.currency)
    if (ids.type === 'DfspAccountMetadataNone') {
      return {
        type: 'FAILURE',
        fspiopError: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND,
          `failed as getDfspAccountMetata() returned 'DfspAccountMetadataNone' for \
              dfspId: ${query.dfspId}, and currency: ${query.currency}`.replace(/\s+/g, ' ')
        )
      }
    }
    const tbAccountIds = [
      ids.liquidity,
      // TODO: is this equivalent to POSITION?
      ids.clearing,
      ids.collateral,
      // TODO: is this equivalent to SETTLEMENT?
      ids.settlementMultilateral
    ]
    const tbAccounts = await this.deps.client.lookupAccounts(tbAccountIds)
    if (tbAccounts.length !== tbAccountIds.length) {
      return {
        type: 'FAILURE',
        fspiopError: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.INTERNAL_SERVER_ERROR,
          `failed as getDfspAccountMetata() returned 'DfspAccountMetadataNone' for \
              dfspId: ${query.dfspId}, and currency: ${query.currency}`.replace(/\s+/g, ' ')
        )
      }
    }

    // TODO(LD): We need to spend more time here figuring out how to adapt from newer double entry
    // accounts map on to the legacy accounts
    const accounts: Array<LegacyLedgerAccount> = []
    let clearingAccount: Account
    let collateralAccount: Account
    tbAccounts.forEach(tbAccount => {
      if (tbAccount.id === ids.clearing) {
        clearingAccount = tbAccount
      }
      if (tbAccount.id === ids.collateral) {
        collateralAccount = tbAccount
      }
    })
    assert(clearingAccount)
    assert(collateralAccount)

    // Legacy Settlement Balance: How much DFSP has available to settle.
    // Was a negative number in the legacy API once the dfsp had deposited funds.
    const legacySettlementBalancePosted = (collateralAccount.debits_posted - collateralAccount.credits_posted) * BigInt(-1)
    const legacySettlementBalancePending = (collateralAccount.debits_pending - collateralAccount.credits_pending) * BigInt(-1)

    // Legacy Position Balance: How much DFSP is owed or how much this DFSP owes.
    const clearingBalancePosted = clearingAccount.credits_posted - clearingAccount.debits_posted
    const clearingBalancePending = clearingAccount.credits_pending - clearingAccount.debits_pending
    const legacyPositionBalancePosted = (legacySettlementBalancePosted + clearingBalancePosted) * BigInt(-1)
    const legacyPositionBalancePending = (legacySettlementBalancePending + clearingBalancePending) * BigInt(-1)

    accounts.push({
      id: ids.clearing,
      ledgerAccountType: 'POSITION',
      currency: query.currency,
      isActive: !(clearingAccount.flags & AccountFlags.closed),
      value: convertBigIntToNumber(legacyPositionBalancePosted) / 100,
      reservedValue: convertBigIntToNumber(legacyPositionBalancePending) / 100,
      // We don't have this in TigerBeetle, although we could use the created date
      changedDate: new Date(0)
    })

    accounts.push({
      id: ids.collateral,
      ledgerAccountType: 'SETTLEMENT',
      currency: query.currency,
      isActive: !(collateralAccount.flags & AccountFlags.closed),
      value: convertBigIntToNumber(legacySettlementBalancePosted) / 100,
      reservedValue: convertBigIntToNumber(legacySettlementBalancePending) / 100,
      // We don't have this in TigerBeetle, although we could use the created date
      changedDate: new Date(0)
    })

    return {
      type: 'SUCCESS',
      accounts,
    }
  }

  public async getNetDebitCap(query: GetNetDebitCapQuery): Promise<NetDebitCapResponse> {
    const ids = await this.deps.metadataStore.getDfspAccountMetadata(query.dfspId, query.currency)
    if (ids.type === 'DfspAccountMetadataNone') {
      return {
        type: 'FAILURE',
        fspiopError: ErrorHandler.Factory.createFSPIOPError(
          ErrorHandler.Enums.FSPIOPErrorCodes.ID_NOT_FOUND
            `failed as getDfspAccountMetata() returned 'DfspAccountMetadataNone' for \
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
        fspiopError: ErrorHandler.Factory.createFSPIOPError(
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
      limit
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

      const payerMetadata = await this.deps.metadataStore.getDfspAccountMetadata(payer, currency)
      if (payerMetadata.type === 'DfspAccountMetadataNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          fspiopError: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payer fsp: ${payer} not found`
          ),
        }
      }
      const payeeMetadata = await this.deps.metadataStore.getDfspAccountMetadata(payee, currency)
      if (payeeMetadata.type === 'DfspAccountMetadataNone') {
        return {
          type: PrepareResultType.FAIL_OTHER,
          fspiopError: ErrorHandler.Factory.createFSPIOPError(
            ErrorHandler.Enums.FSPIOPErrorCodes.PARTY_NOT_FOUND,
            `payee fsp: ${payee} not found`
          ),
        }
      }

      const prepareId = TigerBeetleLedger.fromMojaloopId(input.payload.transferId)
      const amount = TigerBeetleLedger.fromMojaloopAmount(amountStr, 2)

      /**
       * Dr Payer_Clearing
       *  Cr Payee_Clearing
       * Flags: pending
       */

      // TODO(LD): The issue with this chart of account design is that for a net-receiver of funds
      // DFSP, they will end up being over their net debit cap, and funds need to be moved out of 
      // the Clearing account back to the Liquidity account. But we can come back to this later.

      const transfer: Transfer = {
        id: prepareId,
        debit_account_id: payerMetadata.clearing,
        credit_account_id: payeeMetadata.clearing,
        amount,
        pending_id: 0n,
        // Also used as a correlation to map between Mojaloop Transfers (1) ---- (*) TigerBeetle Transfers
        user_data_128: prepareId,
        user_data_64: 0n,
        user_data_32: 0,
        // TODO(LD): we can use this timeout in the future, once we implement our scanning timeout
        // handler or CDC
        timeout: 0,
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
            fspiopError: ErrorHandler.Factory.createFSPIOPError(
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
         * Look up what it is, and map to a PrepareResultType
         */
        if (error === CreateTransferError.exists) {
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
                fspiopError: ErrorHandler.Factory.createInternalServerFSPIOPError(
                  `TigerBeetleLedger.prepare() - TigerBeetleLedger.lookupTransfer() got result \
                  ${lookupTransferResult.type} after encountering ${error}. This should not be \
                  possible`.replace(/\s+/g, ' ')
                )
              }
            }
            case LookupTransferResultType.FAILED: {
              return {
                type: PrepareResultType.FAIL_OTHER,
                fspiopError: lookupTransferResult.fspiopError
              }
            }
          }
        }

        // unhandled TigerBeetle Error
        const readableError = CreateTransferError[error]

        return {
          type: PrepareResultType.FAIL_OTHER,
          fspiopError: ErrorHandler.Factory.createFSPIOPError(
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
        fspiopError: err

      }
    }
  }

  private async abort(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
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
        fspiopError: ErrorHandler.Factory.createFSPIOPError(
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
    if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
      return {
        type: FulfilResultType.PASS
      }
    }

    if (input.action === Enum.Events.Event.Action.ABORT) {
      return this.abort(input)
    }

    try {
      const prepareId = TigerBeetleLedger.fromMojaloopId(input.transferId)

      // TODO(LD): Validate that the fulfilment matches the condition
      // for now, we're just putting this in here to simulate the peformance of doing this
      // from a condition that is already in memory
      const dummyFulfilment = 'V-IalzIzy-zxy0SrlY1Ku2OE9aS4KgGZ0W-Zq5_BeC0'
      const dummyCondition = 'GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA'
      const fulfilmentAndConditionResult = TigerBeetleLedger.validateFulfilmentAndCondition(dummyFulfilment, dummyCondition)
      if (fulfilmentAndConditionResult.type === 'FAIL') {
        return {
          type: FulfilResultType.FAIL_VALIDATION,
          fspiopError: ErrorHandler.Factory.createFSPIOPError(
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
          fspiopError: ErrorHandler.Factory.createFSPIOPError(
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
        fspiopError: err
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
        fspiopError: ErrorHandler.Factory.createInternalServerFSPIOPError(
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
        fspiopError: ErrorHandler.Factory.createInternalServerFSPIOPError(
          `Found: ${relatedTransfers.length} related transfers. Expected at most 2.`
        )
      }
    }

    if (finalTransfer.flags & TransferFlags.post_pending_transfer) {
      const committedTime = convertBigIntToNumber(finalTransfer.timestamp / 1_000_000n)
      const transferMetadata = await this.deps.metadataStore.lookupTransferMetadata([query.transferId])
      assert(transferMetadata.length === 1, 'expected exactly one transferMetadata result')

      const foundMetadata = transferMetadata[0]
      if (foundMetadata.type === 'TransferMetadataNone') {
        return {
          type: LookupTransferResultType.FAILED,
          fspiopError: ErrorHandler.Factory.createInternalServerFSPIOPError(
            `missing transfer metadata for finalized transferId: ${query.transferId}`
          )
        }
      }

      if (!foundMetadata.fulfilment) {
        return {
          type: LookupTransferResultType.FAILED,
          fspiopError: ErrorHandler.Factory.createInternalServerFSPIOPError(
            `missing metadata.fulfilment for finalized transferId: ${query.transferId}`
          )
        }
      }

      return {
        type: LookupTransferResultType.FOUND_FINAL,
        finalizedTransfer: {
          completedTimestamp: (new Date(committedTime)).toISOString(),
          transferState: "COMMITTED",
          fulfilment: foundMetadata.fulfilment
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
      fspiopError: ErrorHandler.Factory.createInternalServerFSPIOPError(
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
        ledger: LedgerIdTimeoutHandler,
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

      // TODO(LD): Ideally we could get the cluster time here, because if the server time has drifted
      // too far, we might be filtering transfers out that have already timed out.
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

      // Lookup the metadata for each transfer from the metadata database. If this fails, throw an error.
      // const metadata = await this.lookupTransfersMetadata(timedOutTransfers.map(t => TigerBeetleLedger.toMojaloopId(t.id)))
      const metadata = await this.deps.metadataStore.lookupTransferMetadata(timedOutTransfers.map(t => TigerBeetleLedger.toMojaloopId(t.id)))
      const missingMetadata = metadata.filter(m => m.type === 'TransferMetadataNone')
      assert(missingMetadata.length === 0, `lookupTransferMetadata() missing ${missingMetadata.length} entries`)
      const foundMetadata = metadata.filter(m => m.type === 'TransferMetadata')

      const transfersWithMetadata: Array<TimedOutTransfer> = []
      foundMetadata.forEach(metadata => {
        transfersWithMetadata.push({
          id: metadata.id,
          payerId: metadata.payerId,
          payeeId: metadata.payeeId,
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
          pending_id: openingBookmark.id,
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
        transfers: transfersWithMetadata
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
        ledger: 0,
        code: 0,
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
        ledger: 0,
        code: 0,
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