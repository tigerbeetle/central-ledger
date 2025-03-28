
/**
 * @class MetadataStore
 * @description A store to keep track of Participant account ids and other metadata associated with
 *   clearing transfers
 */
class MetadataStore {

  constructor(client) {
    console.log('init metadata store')
    this._client = client

    this._client.exec(`
      CREATE TABLE IF NOT EXISTS accounts (
        fspId TEXT NOT NULL,
        currency TEXT NOT NULL,
        accountType INTEGER NOT NULL,
        tigerBeetleId TEXT NOT NULL,
        PRIMARY KEY (dfspId, transferType, accountType)
      )
    `);  
  }

  getAccountId(accountType, fspId, currency) {
    const whereStatement = this._client.prepare(`
      SELECT tigerBeetleId FROM accounts 
      WHERE accountType = ? AND fspId = ? AND currency = ?
    `);
    const row = whereStatement.get(accountType, fspId, currency)

    if (!row || !row.tigerBeetleId) {
      throw new Error(`account not found for accountType: ${accountType}, fspId: ${fspId}, currency: ${currency}`)
    }

    return BigInt(row.tigerBeetleId)
  }
  
}

module.exports = MetadataStore