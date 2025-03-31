const MetadataStore = require('./MetadataStore')

/**
 * Simple in-memory cache for MetadataStore operations.
 * 
 * This class extends MetadataStore to provide caching functionality, 
 * optimizing lookups for account IDs which rarely change.
 * 
 * @class CachedMetadataStore
 * @extends {MetadataStore}
 */
class CachedMetadataStore extends MetadataStore {
  /**
   * In-memory cache for account IDs.
   * @private
   * @type {Object.<string, bigint>}
   */
  _accountIdCache;

  /**
   * Creates a new CachedMetadataStore instance.
   * 
   * @param {Database} client - The database client to use for operations.
   */
  constructor(client) {
    super(client);
    this._accountIdCache = {};
  }

  /**
   * Generates a unique cache key for an account.
   * 
   * @private
   * @param {AccountType} accountType - The type of account.
   * @param {number} fspId - The FSP ID.
   * @param {string} transferType - The transfer type.
   * @returns {string} A unique string key for the account.
   */
  _accountIdKey(currency, fspId, transferType) {
    return `${transferType}/${fspId}/${currency}`;
  }
  
  /**
   * Retrieves an account ID, first checking the cache before querying the database.
   * Cache doesn't expire as accounts rarely change.
   * 
   * @async
   * @param {AccountType} accountType - The type of account.
   * @param {number} fspId - The FSP ID.
   * @param {string} currency - The transfer type.
   * @returns {Promise<bigint>} The account ID.
   * @override
   */
  async getAccountId(accountType, fspId, currency) {
    // Check the cache first
    const key = this._accountIdKey(accountType, fspId, currency);
    const value = this._accountIdCache[key];
    if (value) {
      return value;
    }

    // Not in cache - look up from parent class
    const accountId = await super.getAccountId(accountType, fspId, currency);

    // Cache the result
    this._accountIdCache[key] = accountId;
    return accountId;
  }
}

module.exports = CachedMetadataStore