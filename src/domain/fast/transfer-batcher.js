const { createClient, id, AccountFlags, CreateAccountError, CreateTransferError, TransferFlags } = require('tigerbeetle-node')


/**
 * @class TransferBatcher
 * @description Promise based queue to let us efficently batch together transfers
 */
class TransferBatcher {
  /**
   * @private
   * @type {Client}
   */
  _client;

  /**
   * @private
   * @type {{ transfer: Transfer; resolve: () => void; reject: (error: any) => void }[]}
   */
  _transferQueue = [];

  /**
   * @private
   * @description The maximum size a batch should reach before being sent to TigerBeetle
   * @type {number}
   */
  _batchSize;

  /**
   * @private
   * @description How often (ms) the batch should be shipped to TigerBeetle
   * @type {number}
   */
  _batchInterval;

  /**
   * @private
   * @type {NodeJS.Timeout | null}
   */
  _timer = null;



  constructor(client, batchSize, batchInterval) {
    this._client = client
    this._batchSize = batchSize;
    this._batchInterval = batchInterval;

    // Send off the batches in an event loop or something
    this._timer = setInterval(() => this.flushQueue(), this._batchInterval)
  }

  async enqueueTransfer(transfer) {
    return new Promise((resolve, reject) => {
      this._transferQueue.push({ transfer, resolve, reject });

      // If batch is full, process immediately
      if (this._transferQueue.length >= this._batchSize) {
        console.log('transfer Queue ready to ship!')

        // TODO: determine if we should be awaiting this promise here!
        this.flushQueue();
      }
    });
  }

  async flushQueue() {
    if (this._transferQueue.length === 0) {
      return
    }

    const batch = this._transferQueue.splice(0, this._batchSize)
    console.log(`LD TransferBatcher.flushQueue() shipping batch of size: ${batch.length} to TigerBeetle.`)
    const errors = await this._client.createTransfers(batch.map(t => t.transfer))  
    // make into a dict for faster lookup
    const errorIndices = errors.reduce((acc, curr) => {
      acc[curr.index] = curr.result
      return acc
    }, {})

    batch.map((item, idx) => {
      if (errorIndices[idx]) {
        item.reject(CreateTransferError[errorIndices[idx]]);
      } else {
        item.resolve();
      }
    })
  }
}

module.exports = TransferBatcher