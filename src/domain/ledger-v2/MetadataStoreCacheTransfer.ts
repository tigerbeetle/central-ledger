import assert from "assert";
import { TransferMetadata } from "./MetadataStore";

type CacheHit<T> = {
  type: 'HIT',
  contents: T
}

type CacheMiss<T> = {
  type: 'MISS'
}

type CacheMissOrHit<T> = CacheHit<T> | CacheMiss<T>


export class MetadataStoreCacheTransfer {
  /**
   * Allow at most this number of transfers in the cache before we expire them in a FIFO 
   */
  private MAX_CACHE_SIZE = 1_000_000
  private cacheMap: Record<string, TransferMetadata> = {};
  private idQueue: Array<string> = []

  get(ids: Array<string>): Array<CacheMissOrHit<TransferMetadata>> {
    const results: Array<CacheMissOrHit<TransferMetadata>> = []
    ids.forEach(id => {
      if (!this.cacheMap[id]) {
        results.push({ type: 'MISS' })
        return
      }

      results.push({
        type: 'HIT',
        contents: this.cacheMap[id]
      })
    })

    return results
  }

  put(metadata: Array<TransferMetadata>): void {
    metadata.forEach(tm => {
      assert(tm.id)
      assert(tm.payeeId)
      assert(tm.payerId)
      assert(tm.condition)
      assert(tm.ilpPacket)

      if (tm.fulfilment) {
        assert(typeof tm.fulfilment === 'string')
      }

      this.cacheMap[tm.id] = tm;
      this.idQueue.push(tm.id)
    })

    this.maybeSweep()
  }

  /**
   * Explicitly delete from the cache
   */
  delete(ids: Array<string>) {
    ids.forEach(id => {
      if (this.cacheMap[id]) {
        delete this.cacheMap[id];
      }
    })
    const idSet = new Set(ids)
    this.idQueue = this.idQueue.filter(id => !idSet.has(id))
  }

  /**
   * If we have exceeded the MAX_CACHE_SIZE, then sweep!
   */
  private maybeSweep(): void {
    if (this.idQueue.length <= this.MAX_CACHE_SIZE) {
      return
    }

    const idsToDelete = this.idQueue.slice(0, this.idQueue.length - this.MAX_CACHE_SIZE)
    this.delete(idsToDelete)
  }

  private key(dfspId: string, currency: string): string {
    return `${dfspId}+${currency}`;
  }
}
