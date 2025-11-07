import assert from "assert";
import { DfspAccountMetadata } from "./MetadataStore";

type CacheHit<T> = {
  type: 'HIT',
  contents: T
}

type CacheMiss<T> = {
  type: 'MISS'
}

type CacheMissOrHit<T> = CacheHit<T> | CacheMiss<T>

export class MetadataStoreCacheAccount {
  private cacheMap: Record<string, DfspAccountMetadata> = {};

  get(dfspId: string, currency: string): CacheMissOrHit<DfspAccountMetadata> {
    const key = this.key(dfspId, currency);
    if (!this.cacheMap[key]) {
      return { type: 'MISS' };
    }

    return {
      type: 'HIT',
      contents: this.cacheMap[key]
    };
  }

  put(dfspId: string, currency: string, metadata: DfspAccountMetadata): void {
    assert.equal(dfspId, metadata.dfspId);
    assert.equal(currency, metadata.currency);
    assert(typeof metadata.clearing === 'bigint');
    assert(typeof metadata.collateral === 'bigint');
    assert(typeof metadata.liquidity === 'bigint');
    assert(typeof metadata.settlementMultilateral === 'bigint');

    const key = this.key(dfspId, currency);
    this.cacheMap[key] = metadata;
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
