const { createClient } = require('tigerbeetle-node')
const Database = require('better-sqlite3')
const MetadataStore = require('./MetadataStore')


class Ledger {

  constructor(tbClient, metadataStore) {
    this._tbClient = tbClient
    this._metadataStore = metadataStore
  }

  /**
   * Prepare side - take a list of Mojaloop Pending Transfers and convert them to TigerBeetle Transfers
   */
  buildPendingTransferBatch(transferList) {

    // get all of the clearing account ids we need


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