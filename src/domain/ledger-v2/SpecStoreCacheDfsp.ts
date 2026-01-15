import assert from "assert"
import { SpecDfsp } from "./SpecStore"

type CacheHit<SpecDfsp> = {
  type: 'HIT',
  contents: SpecDfsp
}

type CacheMiss<SpecDfsp> = {
  type: 'MISS'
}

type CacheMissOrHit<SpecDfsp> = CacheHit<SpecDfsp> | CacheMiss<SpecDfsp>

export class SpecStoreCacheDfsp {
  private cache: Record<string, SpecDfsp>

  constructor() {
    this.cache = {}
  }

  get(dfspId: string): CacheMissOrHit<SpecDfsp> {
    const cached = this.cache[dfspId]
    if (cached) {
      return {
        type: 'HIT',
        contents: cached
      }
    }

    return {
      type: 'MISS'
    }
  }

  put(dfspId: string, spec: SpecDfsp): void {
    assert(spec.type === 'SpecDfsp')
    assert(spec.dfspId === dfspId, 'dfspId mismatch')
    assert(typeof spec.accountId === 'bigint', 'accountId must be bigint')

    this.cache[dfspId] = spec
  }

  delete(dfspId: string): void {
    delete this.cache[dfspId]
  }
}
