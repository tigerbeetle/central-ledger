import assert from "assert";
import { AttachTransferSpecFulfilment, SpecTransfer } from "./SpecStore";

type CacheHit<T> = {
  type: 'HIT',
  contents: T
}

type CacheMiss<T> = {
  type: 'MISS'
}

type CacheMissOrHit<T> = CacheHit<T> | CacheMiss<T>


export class SpecStoreCacheTransfer {
  /**
   * Allow at most this number of transfers in the cache before we expire them in a FIFO 
   */
  private MAX_CACHE_SIZE = 1_000_000
  private cacheMap: Record<string, SpecTransfer> = {};
  private idQueue: Array<string> = []

  get(ids: Array<string>): Array<CacheMissOrHit<SpecTransfer>> {
    const results: Array<CacheMissOrHit<SpecTransfer>> = []
    ids.forEach(id => {
      console.log(`SpecStoreCacheTransfer.get(): ${id}`)
      if (!this.cacheMap[id]) {
        results.push({ type: 'MISS' })
        return
      }

      console.log(`SpecStoreCacheTransfer.get(): ${id} - ${JSON.stringify(this.cacheMap[id])}`)

      results.push({
        type: 'HIT',
        contents: this.cacheMap[id]
      })
    })

    return results
  }

  put(spec: Array<SpecTransfer>): void {
    spec.forEach(tm => {
      console.log(`SpecStoreCacheTransfer.put(): ${tm.id}`)
      console.log(`SpecStoreCacheTransfer.put(): ${tm.id} - ${JSON.stringify(tm)}`)

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

  putFulfilments(attachments: Array<AttachTransferSpecFulfilment>): void {
    attachments.forEach(attachment => {
      const existing = this.cacheMap[attachment.id]
      console.log(`SpecStoreCacheTransfer.putFulfilments(): ${attachment.id}`)
      console.log(`SpecStoreCacheTransfer.putFulfilments(): ${attachment.id} - ${JSON.stringify(attachment)}`)
      
      // not already cached, no big deal
      if (!existing) {
        return
      }

      this.cacheMap[attachment.id] = {
        ...existing,
        fulfilment: attachment.fulfilment
      }
    })
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
}
