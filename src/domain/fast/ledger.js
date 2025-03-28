const { createClient } = require('tigerbeetle-node')
const Database = require('better-sqlite3')
const MetadataStore = require('./MetadataStore')
const assert = require('assert')
const { MLNumber } = require('@mojaloop/ml-number/src/mlnumber')

const AccountType = Object.freeze({
  Collateral: Symbol(1),
  Reserve: Symbol(2),
  Clearing: Symbol(3),
  Settlement_Multilateral: Symbol(4),
  Settlement_Bilateral: Symbol(5),
})


class Ledger {

  constructor(tbClient, metadataStore) {
    this._tbClient = tbClient
    this._metadataStore = metadataStore
  }

  /**
   * Prepare side - take a list of Mojaloop Pending Transfers and convert them to TigerBeetle Transfers
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
    const amountMLNumber = new MLNumber(amountStr)
    const amount = BigInt(amountMLNumber.toFixed())

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


}

// TODO: globals are a real pain to deal with in testing, but let's just do this for now

const tbClient = createClient({
  cluster_id: 0n,
  replica_addresses: process.env.TB_ADDRESS && process.env.TB_ADDRESS.split(',') || ['3000'],
});
const sqliteClient = new Database('metdata.db');

const metadataStore = new MetadataStore(sqliteClient)
// const accountsDb = new Database('accounts.db');
// const metadataStore = new CachedMetadataStore(accountsDb)

const ledger = new Ledger(tbClient, metadataStore)

module.exports = ledger