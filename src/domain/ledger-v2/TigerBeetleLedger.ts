import { ApplicationConfig } from "src/shared/config";
import { logger } from '../../shared/logger';
import { CreateDFSPCommand, CreateDFSPResponse, CreateHubAccountCommand, CreateHubAccountResponse, DepositCollateralCommand, DepositCollateralResponse, FulfilResult, FulfilResultType, PrepareResult, PrepareResultType } from "./types";
import { FusedPrepareHandlerInput } from "src/handlers-v2/FusedPrepareHandler";
import { FusedFulfilHandlerInput } from "src/handlers-v2/FusedFulfilHandler";
import { Account, AccountFlags, amount_max, Client, CreateAccountError, CreateTransferError, id, Transfer, TransferFlags } from 'tigerbeetle-node'
import assert, { fail } from "assert";
import * as ErrorHandler from '@mojaloop/central-services-error-handling';
import { TransferBatcher } from "./TransferBatcher";
import { Ledger } from "./Ledger";
import { DfspAccountIds, MetadataStore } from "./MetadataStore";

export interface TigerBeetleLedgerDependencies {
  config: ApplicationConfig
  client: Client
  metadataStore: MetadataStore
  transferBatcher: TransferBatcher
}

// reserved for USD
export const LedgerIdUSD = 100

export enum AccountType {
  Collateral = 1,
  Liquidity = 2,
  Clearing = 3,
  Settlement_Multilateral = 4,
}

export default class TigerBeetleLedger implements Ledger {
  constructor(private deps: TigerBeetleLedgerDependencies) {

  }

  /**
   * Onboarding/Lifecycle Management
   */
  public async createHubAccount(cmd: CreateHubAccountCommand): Promise<CreateHubAccountResponse> {
    // TODO(LD): I don't know if we need this at the moment, since we won't have common accounts
    // but instead use separate collateral accounts for each DFSP + Currency combination
    //
    // We will however need to set up:
    // 1. Settlement Models/Configuration
    // 2. Enable certain currencies
    logger.warn('depositCollateral() - noop')

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
        type: 'FAILED',
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
    const createTransferErrors = await this.deps.client.createTransfers(transfers);

    for (const error of createTransferErrors) {
      readableErrors.push(CreateTransferError[error.result])
      console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`)
      failed = true
    }

    if (failed) {
      return {
        type: 'FAILED',
        error: new Error(`LedgerError: ${readableErrors.join(',')}`)
      }
    }

    assert.strictEqual(createTransferErrors.length, 0)
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
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        // TODO(LD): we can use this timeout in the future, once we hook up CDC to get the timeout
        // events back out of TigerBeetle
        timeout: 0,
        ledger: LedgerIdUSD,
        code: 1,
        flags: TransferFlags.pending,
        timestamp: 0n
      }

      const error = await this.deps.transferBatcher.enqueueTransfer(transfer)
      if (error) {
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

  // TODO(LD): Make this interface batch compatible. This will require the new handlers to be able 
  // to read multiple messages from Kafka at the same point.
  public async fulfil(input: FusedFulfilHandlerInput): Promise<FulfilResult> {
    if (this.deps.config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE) {
      return {
        type: FulfilResultType.PASS
      }
    }

    try {
      const prepareId = TigerBeetleLedger.fromMojaloopId(input.transferId)

      // TODO(LD): Validate that the fulfilment matches the condition

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
}