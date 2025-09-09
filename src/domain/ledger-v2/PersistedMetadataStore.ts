import assert from "node:assert";
import { DfspAccountIds, DfspAccountMetadata, DfspAccountMetadataNone, MetadataStore } from "./MetadataStore";

interface AccountMetadataRecord {
  id: number;
  dfspId: string;
  currency: string;
  collateralAccountId: string;
  liquidityAccountId: string;
  clearingAccountId: string;
  settlementMultilateralAccountId: string;
  isTombstoned: boolean;
  createdDate: string;
  updatedDate: string;
}

interface Database {
  from(tableName: string): {
    where(conditions: any): any;
    orderBy(column: string, direction: 'asc' | 'desc'): any;
    first(): Promise<any>;
    insert(data: any): Promise<any>;
    update(data: any): Promise<any>;
  };
}

type CacheHit<T> = {
  type: 'HIT',
  contents: T
}

type CacheMiss<T> = {
  type: 'MISS'
}

type CacheMissOrHit<T> = CacheHit<T> | CacheMiss<T>

class MetadataStoreCache {
  private cacheMap: Record<string, DfspAccountMetadata> = {}

  get(dfspId: string, currency: string): CacheMissOrHit<DfspAccountMetadata> {
    const key = this.key(dfspId, currency)
    if (!this.cacheMap[key]) {
      return { type: 'MISS' }
    }

    return {
      type: 'HIT',
      contents: this.cacheMap[key]
    }
  }

  put(dfspId: string, currency: string, metadata: DfspAccountMetadata): void {
    assert.equal(dfspId, metadata.dfspId)
    assert.equal(currency, metadata.currency)

    const key = this.key(dfspId, currency)
    this.cacheMap[key] = metadata
  }

  delete(dfspId: string, currency: string) {
    const key = this.key(dfspId, currency)
    if (this.cacheMap[key]) {
      delete this.cacheMap[key]
    }
  }

  private key(dfspId: string, currency: string): string {
    return `${dfspId}+${currency}`
  }
}

export class PersistedMetadataStore implements MetadataStore {
  private cache: MetadataStoreCache

  constructor(private db: Database) {
    this.cache = new MetadataStoreCache()
  }

  async getDfspAccountMetadata(dfspId: string, currency: string): Promise<DfspAccountMetadata | DfspAccountMetadataNone> {
    // These values do't change very often, so it's safe to cache them
    const cacheResult = this.cache.get(dfspId, currency)
    if (cacheResult.type === 'HIT') {
      return cacheResult.contents
    }

    const result = await this.db.from('tigerBeetleAccountMetadata')
      .where({
        dfspId,
        currency,
        isTombstoned: false
      })
      .orderBy('createdDate', 'desc')
      .first();

    if (!result) {
      return { type: 'DfspAccountMetadataNone' };
    }

    const record = result as AccountMetadataRecord;
    this.cache.put(dfspId, currency, result)
    
    return {
      type: 'DfspAccountMetadata',
      dfspId: record.dfspId,
      currency: record.currency,
      collateral: BigInt(record.collateralAccountId),
      liquidity: BigInt(record.liquidityAccountId),
      clearing: BigInt(record.clearingAccountId),
      settlementMultilateral: BigInt(record.settlementMultilateralAccountId)
    };
  }

  async associateDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
    this.cache.delete(dfspId, currency)

    await this.db.from('tigerBeetleAccountMetadata').insert({
      dfspId,
      currency,
      collateralAccountId: accounts.collateral.toString(),
      liquidityAccountId: accounts.liquidity.toString(),
      clearingAccountId: accounts.clearing.toString(),
      settlementMultilateralAccountId: accounts.settlementMultilateral.toString(),
      isTombstoned: false
    });
  }

  async tombstoneDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
    await this.db.from('tigerBeetleAccountMetadata')
      .where({
        dfspId,
        currency,
        collateralAccountId: accounts.collateral.toString(),
        liquidityAccountId: accounts.liquidity.toString(),
        clearingAccountId: accounts.clearing.toString(),
        settlementMultilateralAccountId: accounts.settlementMultilateral.toString()
      })
      .update({
        isTombstoned: true,
        updatedDate: new Date()
      });

    this.cache.delete(dfspId, currency)
  }
}