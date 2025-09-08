import { DfspAccountIds, DfspAccountMetadata, DfspAccountMetadataNone, MetadataStore } from "./MetadataStore";

interface DatabaseRecord {
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

export class PersistedMetadataStore implements MetadataStore {
  constructor(private db: Database) {}

  async getDfspAccountMetadata(dfspId: string, currency: string): Promise<DfspAccountMetadata | DfspAccountMetadataNone> {
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

    const record = result as DatabaseRecord;
    
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
  }
}