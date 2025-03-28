export default class MetadataStore {

  constructor(client) {
    console.log('init metadata store')
    this._client = client

    this._client.exec(`
      CREATE TABLE IF NOT EXISTS accounts (
        dfspId INTEGER NOT NULL,
        transferType INTEGER NOT NULL,
        accountType INTEGER NOT NULL,
        tigerBeetleId TEXT NOT NULL,
        PRIMARY KEY (dfspId, transferType, accountType)
      )
    `);  
  }

  getAccountId(accountType, dfspId, transferType) {
    const whereStatement = this._client.prepare(`
      SELECT tigerBeetleId FROM accounts 
      WHERE accountType = ? AND dfspId = ? AND transferType = ?
    `);
    const row = whereStatement.get(accountType, dfspId, transferType)

    if (!row || !row.tigerBeetleId) {
      throw new Error(`account not found for accountType: ${accountType}, dfspId: ${dfspId}, transferType: ${transferType}`)
    }

    return BigInt(row.tigerBeetleId)
  }
  
}