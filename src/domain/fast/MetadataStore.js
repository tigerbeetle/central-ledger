
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

  async saveDfspAccountMetadata(accountDescriptors)  {
    const dehydratedAccounts = accountDescriptors.map(accountDescriptors => ({
      fspId: accountDescriptors.fspId,
      currency: accountDescriptors.currency,
      accountType: accountDescriptors.accountType,
      tigerBeetleId: accountDescriptors.tigerBeetleId.toString(),
    }))

    const insertStatement = this._client.prepare(`
      INSERT INTO accounts (fspId, currency, accountType, tigerBeetleId)
      VALUES (@fspId, @currency, @accountType, @tigerBeetleId)
    `);

    const insertMany = this._client.transaction((dehydratedAccounts) => {
      dehydratedAccounts.forEach(account => insertStatement.run(account))
    });
  
    insertMany(dehydratedAccounts);
  }
  
}

module.exports = MetadataStore