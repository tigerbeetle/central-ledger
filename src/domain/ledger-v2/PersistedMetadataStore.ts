import assert from "assert";
import { Knex } from "knex";
import { DfspAccountIds, DfspAccountMetadata, DfspAccountMetadataNone, MetadataStore, SaveTransferMetadataResult, TransferMetadata, TransferMetadataNone } from "./MetadataStore";
import { MetadataStoreCacheAccount } from "./MetadataStoreCacheAccount";
import { MetadataStoreCacheTransfer } from "./MetadataStoreCacheTransfer";
import { logger } from '../../shared/logger';

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

interface TransferMetadataRecord {
  id: string
  payerId: string
  payeeId: string
  ilpCondition: string
  ilpPacket: string
  fulfilment?: string
}

export class PersistedMetadataStore implements MetadataStore {
  private cacheAccount: MetadataStoreCacheAccount
  private cacheTransfer: MetadataStoreCacheTransfer

  constructor(private db: Knex) {
    this.cacheAccount = new MetadataStoreCacheAccount()
    this.cacheTransfer = new MetadataStoreCacheTransfer()
  }

  async getDfspAccountMetadata(dfspId: string, currency: string): Promise<DfspAccountMetadata | DfspAccountMetadataNone> {
    // These values do't change very often, so it's safe to cache them
    const cacheResult = this.cacheAccount.get(dfspId, currency)
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
    const metadata: DfspAccountMetadata = {
      type: 'DfspAccountMetadata',
      dfspId: record.dfspId,
      currency: record.currency,
      collateral: BigInt(record.collateralAccountId),
      liquidity: BigInt(record.liquidityAccountId),
      clearing: BigInt(record.clearingAccountId),
      settlementMultilateral: BigInt(record.settlementMultilateralAccountId)
    }
    this.cacheAccount.put(dfspId, currency, metadata)

    return metadata
  }

  async associateDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
    this.cacheAccount.delete(dfspId, currency)

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

    this.cacheAccount.delete(dfspId, currency)
  }

  async lookupTransferMetadata(ids: Array<string>): Promise<Array<TransferMetadata | TransferMetadataNone>> {
    // First port of call, check the cache
    const transferMetadataCached = this.cacheTransfer.get(ids)
    const results: Array<TransferMetadata | TransferMetadataNone> = []

    const transferMetadataFoundSet: Record<string, TransferMetadata> = {}
    const missingIds: Array<string> = []
    transferMetadataCached.forEach((hitOrMiss, idx) => {
      if (hitOrMiss.type === 'HIT') {
        transferMetadataFoundSet[hitOrMiss.contents.id] = hitOrMiss.contents
        return
      }

      const missingId = ids[idx]
      missingIds.push(missingId)
    })

    // Everything was in cache, we don't need to go to the database.
    if (missingIds.length === 0) {
      return transferMetadataCached.map(tm => {
        assert(tm.type === 'HIT')
        return tm.contents
      })
    }

    const tranferMetadataPersisted = await this.lookupTransferMetadataPersisted(missingIds)
    tranferMetadataPersisted.forEach(tm => {
      if (tm.type === 'TransferMetadata') {
        transferMetadataFoundSet[tm.id] = tm
        return
      }
    })

    // maintain ordering
    return ids.map(id => {
      if (transferMetadataFoundSet[id]) {
        return transferMetadataFoundSet[id]
      }
      return {
        type: 'TransferMetadataNone',
        id
      }
    })
  }

  private async lookupTransferMetadataPersisted(ids: Array<string>): Promise<Array<TransferMetadata | TransferMetadataNone>> {
    assert(ids)

    const queryResult = await this.db.from('tigerBeetleTransferMetadata')
      .whereIn('id', ids)

    // maintain order of results, even when we find nulls
    const resultSet: Record<string, TransferMetadataRecord> = queryResult.reduce((acc, curr) => {
      const record = curr as TransferMetadataRecord
      assert(record.id)
      assert(record.payeeId)
      assert(record.payerId)
      assert(record.ilpCondition)
      assert(record.ilpPacket)

      acc[record.id] = record
    }, {})

    const results: Array<TransferMetadata | TransferMetadataNone> = []
    ids.forEach(id => {
      if (!resultSet[id]) {
        results.push({
          type: 'TransferMetadataNone',
          id
        })
        return
      }

      const record = resultSet[id]
      results.push({
        type: 'TransferMetadata',
        id: record.id,
        payerId: record.payerId,
        payeeId: record.payeeId,
        condition: record.ilpCondition,
        ilpPacket: record.ilpPacket,
        fulfilment: record.fulfilment ? record.fulfilment : undefined
      })
    })

    return results
  }

  async saveTransferMetadata(metadata: Array<TransferMetadata>): Promise<Array<SaveTransferMetadataResult>> {

    try {
      const records: Array<TransferMetadataRecord> = metadata.map(m => {
        const record: TransferMetadataRecord = {
          id: m.id,
          payerId: m.payerId,
          payeeId: m.payeeId,
          ilpCondition: m.condition,
          ilpPacket: m.ilpPacket,
        }
        if (m.fulfilment) {
          record.fulfilment = m.fulfilment
        }

        return record
      })

      // TODO: when saving make sure it upserts properly
      await this.db.from('tigerBeetleTransferMetadata')
        .insert(records)
        .onConflict('id')
        .merge(['fulfilment']);
      this.cacheTransfer.put(metadata)

      return metadata.map(m => {
        return {
          type: 'SUCCESS'
        }
      })
    } catch (err) {
      logger.error(`saveTransferMetadata() - failed with error: ${err.message}`)
      return metadata.map(m => {
        return {
          type: 'FAILURE'
        }
      })
    }
  }
}