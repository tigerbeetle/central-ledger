const assert = require('assert');

const CLEANUP_INTERVAL = 500;

/**
 * @class TransferMessageCache
 * @description Cache transfer messages until their expiry to access them on the hot path
 *   In the future, we could tier this to something like Redis for access across horizontally scaled
 *   handlers, but for now, let's assume that the POST /transfers and PUT /transfer messages end up 
 *   on the same server
 */
class TransferMessageCache {
  constructor() {
    console.log('TransferMessageCache.constructor')
    // map of transferId => transfer metadata
    this._transfers = {}

    // map of transferId => expiry timestamp
    this._expiries = {}

    setInterval(() => this._cleanup((new Date()).getTime()), CLEANUP_INTERVAL)
  }

  _assertCacheValid() {
    assert.equal(
      Object.keys(this._transfers).length,
      Object.keys(this._expiries).length,
      'Cache somehow got out of sync'
    )
  }

  put(transferId, payload, expiry) {
    this._assertCacheValid()
    // console.log('TransferMessageCache PUT transferId', transferId)

    this._transfers[transferId] = payload
    this._expiries[transferId] = expiry
  }

  get(transferId) {
    this._assertCacheValid()
    const payload = this._transfers[transferId]
    if (!payload) {
      throw new Error(`TransferMessageCache could not find transfer for id: ${transferId}`)
    }

    return payload
  }

  /**
   * @description Get the transfer payload from the cache, and immediately expire it
   */
  getAndImmediatelyExpire(transferId) {
    this._assertCacheValid()
    const payload = this.get(transferId)

    delete this._transfers[transferId];
    delete this._expiries[transferId];

    return payload
  }


  /**
   * @description Called periodically to remove expired records
   */
  _cleanup(expireBefore) {
    if (!expireBefore) {
      expireBefore = (new Date()).getTime()
    }
    // console.log('TransferMessageCache._cleanup. cleaning up before', expireBefore);
    const transferIdTombstones = []

    Object.values(this._expiries).forEach((expiry, idx) => {
      const transferId = Object.keys(this._expiries)[idx]
      if (expiry <= expireBefore) {
        transferIdTombstones.push(transferId)
      }
    })

    transferIdTombstones.forEach(transferId => {
      delete this._transfers[transferId];
      delete this._expiries[transferId];
    })
  }
}

const transferMessageCache = new TransferMessageCache();

module.exports = transferMessageCache