const { createClient, id, AccountFlags, CreateAccountError, CreateTransferError, TransferFlags, amount_max } = require('tigerbeetle-node')
const Database = require('better-sqlite3')
const CachedMetadataStore = require('./CachedMetadataStore')
const assert = require('assert')
const { MLNumber } = require('@mojaloop/ml-number/src/mlnumber')
const Helper = require('./helper')
const Hydrator = require('./hydrator')
const AccountType = require('./AccountType')
const TransferBatcher = require('./transfer-batcher')
const util = require('util')

// TODO: expose these to config options
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '250');
const BATCH_INTERVAL_MS = parseInt(process.env.BATCH_INTERVAL_MS || '2');
const SKIP_TIGERBEETLE = (process.env.SKIP_TIGERBEETLE || 'false').toLowerCase() === 'true'

/**
 * @class Abstraction over TigerBeetle Ledger + Metadata Store that implements
 *   Mojaloop related business-logic
 */
class Ledger {
  constructor(tbClient, metadataStore) {
    this._tbClient = tbClient
    this._metadataStore = metadataStore
    this._transferBatcher = new TransferBatcher(this._tbClient, BATCH_SIZE, BATCH_INTERVAL_MS)

    if (SKIP_TIGERBEETLE === true) {
      console.log("LD: Warn - `SKIP_TIGERBEETLE` is true - skipping tigerbeetle calls")
    }
  }

  _assertPrepareDtos(prepareDtos) {
    if (prepareDtos.length === 0) {
      throw new Error(`_assertPrepareDtos expected more than one prepareDto in batch`)
    }

    const errors = {}
    const currencies = {}
    prepareDtos.forEach((dto, idx) => {
      try {
        assert(dto)
        assert(dto.transferId)
        assert(dto.payerFsp)
        assert(dto.payeeFsp)
        assert(dto.amount)
        assert(dto.amount.amount)
        assert(dto.amount.currency)

        currencies[dto.amount.currency] = true
      } catch (err) {
        errors[idx] = err
      }
    })

    if (Object.keys(errors).length > 0) {
      throw new Error(`_assertPrepareDtos: the following dtos failed validation: ${util.inspect(errors)}`)
    }

    assert(Object.keys(currencies).length === 1, 'expected only single currency in a batch')
  }


  /**
   * Prepare side - take a list of Mojaloop Pending Transfers and convert them to TigerBeetle Transfers
   */
  async assemblePrepareBatch(prepareDtos) {
    this._assertPrepareDtos(prepareDtos)

    // TODO: handle multiple currencies in one batch
    const currency = prepareDtos[0].amount.currency

    // prefetch the list of dfsps to get the clearing account ids
    const dfspIdMap = prepareDtos.reduce((acc, dto) => {
      const payerFsp = dto.payerFsp
      const payeeFsp = dto.payeeFsp
      if (!acc[payerFsp]) {
        acc[payerFsp] = true
      }

      if (!acc[payeeFsp]) {
        acc[payeeFsp] = true
      }

      return acc
    }, {})
    const dfspIds = Object.keys(dfspIdMap)
    const clearingAccountIds = await Promise.all(dfspIds.map(dfspId => {
      return this._metadataStore.getAccountId(
        AccountType.Clearing, dfspId, currency
      )
    }))
    assert(dfspIds.length, clearingAccountIds)

    const clearingAccountIdMap = dfspIds.reduce((acc, curr, idx) => {
      acc[curr] = clearingAccountIds[idx]
      return acc
    }, {})


    const batch = []
    prepareDtos.forEach(dto => {
      const payerFsp = dto.payerFsp
      const payeeFsp = dto.payeeFsp
      const amountStr = dto.amount.amount
      const transferId = dto.transferId

      // TODO: verify that this is accurate, and shouldn't be replaced with BigInt
      // MLNumber is based on BigNumber, which is a 3rd party dependency
      const amount = BigInt((new MLNumber(amountStr)).toNumber())

      const id = Helper.fromMojaloopId(transferId)
      const transfer = {
        id,
        debit_account_id: clearingAccountIdMap[payerFsp],
        credit_account_id: clearingAccountIdMap[payeeFsp],
        amount,
        pending_id: 0n,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        timeout: 0,
        ledger: 1,
        code: 1,
        flags: TransferFlags.pending,
        timestamp: 0n,
      }

      batch.push(transfer)
    })

    return batch
  }

  _assertFulfilDtosAndContext(fulfilDtos, prepareContext) {
    assert(fulfilDtos.length, prepareContext.length)

    if (fulfilDtos.length === 0) {
      throw new Error(`_assertFulfilDtosAndContext expected more than one prepareDto in batch`)
    }

    const errors = {}
    const currencies = {}
    fulfilDtos.forEach((dto, idx) => {
      try {
        // At the moment, there's nothing on the fulfilment dto that 
        // needs validation.

        const context = prepareContext[idx]
        assert(context.transferId)
        assert(context.amount)
        assert(context.amount.amount)
        assert(context.amount.currency)
        assert(context.payerFsp)
        assert(context.payeeFsp)

        currencies[context.amount.currency] = true
      } catch (err) {
        errors[idx] = err
      }
    })

    if (Object.keys(errors).length > 0) {
      throw new Error(`_assertFulfilDtosAndContext: the following dtos failed validation: ${util.inspect(errors)}`)
    }

    assert(Object.keys(currencies).length === 1, 'expected only single currency in a batch')
  }


  async assembleFulfilBatch(fulfilDtos, prepareContext) {
    this._assertFulfilDtosAndContext(fulfilDtos, prepareContext)

    // TODO: handle multiple currencies in one batch
    const currency = prepareContext[0].amount.currency

    // prefetch the list of dfsps to get the settlement account ids
    const dfspIdMap = prepareContext.reduce((acc, context) => {
      const payerFsp = context.payerFsp
      const payeeFsp = context.payeeFsp
      if (!acc[payerFsp]) {
        acc[payerFsp] = true
      }

      if (!acc[payeeFsp]) {
        acc[payeeFsp] = true
      }

      return acc
    }, {})
    const dfspIds = Object.keys(dfspIdMap)
    const settlementAccountIds = await Promise.all(dfspIds.map(dfspId => {
      return this._metadataStore.getAccountId(
        AccountType.Settlement_Multilateral, dfspId, currency
      )
    }))
    assert(dfspIds.length, settlementAccountIds)

    const settlementAccountIdMap = dfspIds.reduce((acc, curr, idx) => {
      acc[curr] = settlementAccountIds[idx]
      return acc
    }, {})

    const batch = []
    fulfilDtos.forEach((dto, idx) => {
      const context = prepareContext[idx]

      const payerFsp = context.payerFsp
      const payeeFsp = context.payeeFsp
      const amountStr = context.amount.amount
      const transferId = context.transferId

      // TODO: verify that this is accurate, and shouldn't be replaced with BigInt
      // MLNumber is based on BigNumber, which is a 3rd party dependency
      const amount = BigInt((new MLNumber(amountStr)).toNumber())
      const pendingId = Helper.fromMojaloopId(transferId)

      batch.push(
        {
          id: id(),
          debit_account_id: 0n,
          credit_account_id: 0n,
          amount: amount_max,
          pending_id: pendingId,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          ledger: 0,
          code: 0,
          flags: TransferFlags.post_pending_transfer & TransferFlags.linked,
          timestamp: 0n,
        },
        {
          id: id(),
          debit_account_id: settlementAccountIdMap[payerFsp],
          credit_account_id: settlementAccountIdMap[payeeFsp],
          amount: BigInt(amount),
          pending_id: 0n,
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          ledger: 1,
          code: 2,
          flags: 0,
          timestamp: 0n,
        }
      )
    })

    return batch
  }


  /**
   * Commit side - take a list of Mojaloop Fulfilled Transfers and convert them to TigerBeetle Transfers
   */
  async buildPostedTransfers(transferDto) {
    const transferId = transferDto.transferId
    const pendingId = Helper.fromMojaloopId(transferId)

    try {
      assert(transferDto)
      assert(transferDto.transferId)
      assert(transferDto.amount)
      assert(transferDto.amount.amount)
      assert(transferDto.amount.currency)
    } catch (err) {
      console.log(`LD buildPostedTransfers validation failed - transferDto is: ${JSON.stringify(transferDto)}`)
      throw err
    }

    const payerFsp = transferDto.payerFsp
    const payeeFsp = transferDto.payeeFsp
    const currency = transferDto.amount.currency
    const amountStr = transferDto.amount.amount
    // TODO: verify that this is accurate, and shouldn't be replaced with BigInt
    // MLNumber is based on BigNumber, which is a 3rd party dependency
    const amount = BigInt((new MLNumber(amountStr)).toNumber())

    const settlementMultilateralAccountIdPayer = await this._metadataStore.getAccountId(
      AccountType.Settlement_Multilateral, payerFsp, currency
    )
    const settlementMultilateralAccountIdPayee = await this._metadataStore.getAccountId(
      AccountType.Settlement_Multilateral, payeeFsp, currency
    )

    return [
      {
        id: id(),
        debit_account_id: 0n,
        credit_account_id: 0n,
        amount: amount_max,
        pending_id: pendingId,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        timeout: 0,
        ledger: 0,
        code: 0,
        // TODO: add back linked flag, but kinda annoying since the way we are batching
        // doesn't ensure that both these transfers end up in the same batch
        flags: TransferFlags.post_pending_transfer,
        timestamp: 0n,
      },
      {
        id: id(),
        debit_account_id: settlementMultilateralAccountIdPayer,
        credit_account_id: settlementMultilateralAccountIdPayee,
        amount: BigInt(amount),
        pending_id: 0n,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        timeout: 0,
        ledger: 1,
        code: 2,
        flags: 0,
        timestamp: 0n,
      }
    ]
  }


  // in the future we also should store to our metadatabase as well
  async createTransfers(batch) {
    if (SKIP_TIGERBEETLE === true) {
      // skip tigerbeetle altogether, see what happens to performance
      return Promise.resolve()
    }

    // TODO: handle the errors nicely here
    return this._tbClient.createTransfers(batch);
  }

  async enqueueTransfer(transfer) {
    if (SKIP_TIGERBEETLE === true) {
      // skip tigerbeetle altogether, see what happens to performance
      return Promise.resolve()
    }

    // send to the batch processor for processing
    return this._transferBatcher.enqueueTransfer(transfer)
  }

  async onboardDfsp(fspId, currency) {
    console.log('Ledger.onboardDfsp with fspId:', fspId, 'currency:', currency)

    const accountMetadata = [
      {
        fspId,
        currency,
        accountType: AccountType.Collateral,
        tigerBeetleId: id(),
      }, {
        fspId,
        currency,
        accountType: AccountType.Reserve,
        tigerBeetleId: id(),
      }, {
        fspId,
        currency,
        accountType: AccountType.Clearing,
        tigerBeetleId: id(),
      }, {
        fspId,
        currency,
        accountType: AccountType.Settlement_Multilateral,
        tigerBeetleId: id(),
      },
    ]

    const accounts = [
      Helper.accountWithIdAndFlags(accountMetadata[0].tigerBeetleId, AccountFlags.linked | AccountFlags.credits_must_not_exceed_debits),
      Helper.accountWithIdAndFlags(accountMetadata[1].tigerBeetleId, AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits),
      Helper.accountWithIdAndFlags(accountMetadata[2].tigerBeetleId, AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits),
      Helper.accountWithIdAndFlags(accountMetadata[3].tigerBeetleId, 0),
    ]
    const errors = await this._tbClient.createAccounts(accounts)

    // TODO: handle errors better
    for (const error of errors) {
      console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`)
    }
    assert.strictEqual(errors.length, 0)

    // now save to sqlite
    await this._metadataStore.saveDfspAccountMetadata(accountMetadata)
  }

  /**
   * @method recordCollateralDeposit
   * @description
   * 
   * The Switch records a collateral deposit at the switch
   * 
   *   1. Dr Collateral_A x
   *        Cr Reserve_A x
   *   Note: DFSP_A deposits collateral of x at the switch
   */
  async recordCollateralDeposit(fspId, amount, currency) {
    assert(amount > 0, 'Expected amount to be greater than 0')

    // TODO(LD): need to make sure this operation is idempotent - Admin API exposes
    // a transferId for us we can use here
    const transferId = id();

    const debitAccountId = await this._metadataStore.getAccountId(AccountType.Collateral, fspId, currency)
    const creditAccountId = await this._metadataStore.getAccountId(AccountType.Reserve, fspId, currency)

    // TODO: convert amount to ledger specific amount (later on)
    const transfers = [{
      id: transferId,
      debit_account_id: debitAccountId,
      credit_account_id: creditAccountId,
      amount: BigInt(amount),
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: 0,
      timestamp: 0n,
    }]
    const errors = await this._tbClient.createTransfers(transfers);

    for (const error of errors) {
      console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`)
    }
    assert.strictEqual(errors.length, 0)
  }

  /**
   * @method makeFundsAvailableForClearing
   * @description
   * 
   * The DFSP moves funds from Reserved to Clearing accounts
   * 
   *   1. Dr Reserve_A x
   *        Cr Clearing_a x
   * Note: DFSP_A sets the net debit cap to 10, reserves 20
   */
  async makeFundsAvailableForClearing(fspId, amount, currency) {
    assert(amount > 0, 'Expected amount to be greater than 0')

    // TODO: convert amount to ledger specific amount (later on)
    // not hot path, we can saftely store this id somewhere
    const transferId = id();

    const debitAccountIdClearing = await this._metadataStore.getAccountId(AccountType.Reserve, fspId, currency)
    const creditAccountIdClearing = await this._metadataStore.getAccountId(AccountType.Clearing, fspId, currency)

    const transfers = [{
      id: transferId,
      debit_account_id: debitAccountIdClearing,
      credit_account_id: creditAccountIdClearing,
      amount: BigInt(amount),
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: 0,
      timestamp: 0n,
    }]
    const errors = await this._tbClient.createTransfers(transfers);

    for (const error of errors) {
      console.error(`Batch account at ${error.index} failed to create: ${CreateTransferError[error.result]}.`)
    }
    assert.strictEqual(errors.length, 0)
  }


  async getAccountsForFspId(fspId) {
    const accountMeta = await this._metadataStore.getAccountsForFspId(fspId)
    assert(accountMeta.length > 0, `No accounts found for fspId: ${fspId}`)

    const accounts = await this._tbClient.lookupAccounts(
      accountMeta.map(metadata => metadata.tigerBeetleId)
    )

    return accountMeta.map((metadata, idx) => {
      const tigerBeetleAccount = accounts[idx]
      return Hydrator.hydrateLedgerAccount(metadata, tigerBeetleAccount)
    })
  }

}

// TODO: globals are a real pain to deal with in testing, but let's just do this for now
// TODO: move this to the other shared setup stuff

const tbClient = createClient({
  cluster_id: 0n,
  replica_addresses: process.env.TB_ADDRESS && process.env.TB_ADDRESS.split(',') || ['3000'],
});
const sqliteClient = new Database('metdata.db');
const metadataStore = new CachedMetadataStore(sqliteClient)

const ledger = new Ledger(tbClient, metadataStore)

module.exports = ledger