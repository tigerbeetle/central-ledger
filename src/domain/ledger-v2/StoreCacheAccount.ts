import assert from "assert";
import { SpecAccount } from "./SpecStore";

type CacheHit<T> = {
  type: 'HIT',
  contents: T
}

type CacheMiss<T> = {
  type: 'MISS'
}

type CacheMissOrHit<T> = CacheHit<T> | CacheMiss<T>

export class SpecStoreCacheAccount {
  private cacheMap: Record<string, SpecAccount> = {};

  get(dfspId: string, currency: string): CacheMissOrHit<SpecAccount> {
    const key = this.key(dfspId, currency);
    if (!this.cacheMap[key]) {
      return { type: 'MISS' };
    }

    return {
      type: 'HIT',
      contents: this.cacheMap[key]
    };
  }

  put(dfspId: string, currency: string, spec: SpecAccount): void {
    assert.equal(dfspId, spec.dfspId);
    assert.equal(currency, spec.currency);
    assert(typeof spec.clearing === 'bigint');
    assert(typeof spec.collateral === 'bigint');
    assert(typeof spec.liquidity === 'bigint');
    assert(typeof spec.settlementMultilateral === 'bigint');

    const key = this.key(dfspId, currency);
    this.cacheMap[key] = spec;
  }

  delete(dfspId: string, currency: string) {
    const key = this.key(dfspId, currency);
    if (this.cacheMap[key]) {
      delete this.cacheMap[key];
    }
  }

  private key(dfspId: string, currency: string): string {
    return `${dfspId}+${currency}`;
  }
}
