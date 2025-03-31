const { createClient, id, AccountFlags, CreateAccountError, CreateTransferError } = require('tigerbeetle-node')
const Database = require('better-sqlite3')
const MetadataStore = require('./MetadataStore')
const assert = require('assert')
const { MLNumber } = require('@mojaloop/ml-number/src/mlnumber')
const Helper = require('./helper')
const Hydrator = require('./hydrator')
const AccountType = require('./AccountType')


/**
 * @class Abstraction over TigerBeetle Ledger + Metadata Store that implements
 *   Mojaloop related business-logic
 */
class Ledger {
  constructor(tbClient, metadataStore) {
    this._tbClient = tbClient
    this._metadataStore = metadataStore
  }

  /**
   * Prepare side - take a list of Mojaloop Pending Transfers and convert them to TigerBeetle Transfers
   * 
   * TODO: ideally we could abstract over the transferlist here and not pass the transferList directly through
   * or possibly introduce a mapping layer 
   */
  async buildPendingTransferBatch(transferList) {
    assert(transferList.length === 1, 'buildPendingTransferBatch currently only handles 1 tx at a time')

    // get all of the clearing account ids we need
    console.log('transferList is', JSON.stringify(transferList[0]))

    const payerFsp = transferList[0].value.content.payload.payerFsp
    const payeeFsp = transferList[0].value.content.payload.payeeFsp
    const currency = transferList[0].value.content.payload.amount.currency
    const amountStr = transferList[0].value.content.payload.amount.amount
    const transferId = transferList[0].value.content.payload.transferId

    // TODO: verify that this is accurate, and shouldn't be replaced with BigInt
    // MLNumber is based on BigNumber, which is a 3rd party dependency
    const amount = (new MLNumber(amountStr)).toBigInt()

    const clearingAccountIdPayer = await this._metadataStore.getAccountId(
      AccountType.Clearing, payerFsp, currency
    )
    const clearingAccountIdPayee = await this._metadataStore.getAccountId(
      AccountType.Clearing, payeeFsp, currency
    )

    const transfer = {
      id: transferId,
      debit_account_id: clearingAccountIdPayer,
      credit_account_id: clearingAccountIdPayee,
      amount,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 0,
      flags: TransferFlags.pending,
      timestamp: 0n,
    }
    return transfer
  }


  /**
   * Commit side - take a list of Mojaloop Fulfilled Transfers and convert them to TigerBeetle Transfers
   */
  buildPostedTransferBatch(transferList) {

  }



  enqueueTransferBatch(transferBatch) {
    // send to the batch processor for processing
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
  async makeFundsAvailableForClearing(fspId, amount, currency){
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
    console.log('accountMeta is', accountMeta)

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

const tbClient = createClient({
  cluster_id: 0n,
  replica_addresses: process.env.TB_ADDRESS && process.env.TB_ADDRESS.split(',') || ['3000'],
});
const sqliteClient = new Database('metdata.db');
const metadataStore = new MetadataStore(sqliteClient)

const ledger = new Ledger(tbClient, metadataStore)

module.exports = ledger