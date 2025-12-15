import assert from "assert";
import { Knex } from "knex";
import { DfspAccountIds, DfspAccountSpec, DfspAccountSpecNone, SpecStore, SaveTransferSpecCommand, SaveTransferSpecResult, TransferSpec, TransferSpecNone } from "./SpecStore";
import { SpecStoreCacheAccount } from "./StoreCacheAccount";
import { logger } from '../../shared/logger';
import { SpecStoreCacheTransfer } from "./SpecStoreCacheTransfer";

interface AccountSpecRecord {
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

interface TransferSpecRecord {
  id: string
  payerId: string
  payeeId: string
  ilpCondition: string
  ilpPacket: string
  fulfilment?: string
}

const TABLE_ACCOUNT = 'tigerBeetleAccountSpec'
const TABLE_TRANSFER = 'tigerBeetleTransferSpec'


function hydrateSpecAccount(result: any): DfspAccountSpec {
  const record = result as AccountSpecRecord;

  assert(record.dfspId)
  assert(record.currency)
  assert(record.collateralAccountId)
  assert(record.liquidityAccountId)
  assert(record.clearingAccountId)
  assert(record.settlementMultilateralAccountId)

  const spec: DfspAccountSpec = {
    type: 'DfspAccountSpec',
    dfspId: record.dfspId,
    currency: record.currency,
    collateral: BigInt(record.collateralAccountId),
    liquidity: BigInt(record.liquidityAccountId),
    clearing: BigInt(record.clearingAccountId),
    settlementMultilateral: BigInt(record.settlementMultilateralAccountId)
  }

  return spec
}

function dehydrateSpecAccount(spec: DfspAccountSpec): any {
  const record = {
    dfspId: spec.dfspId,
    currency: spec.currency,
    collateralAccountId: spec.collateral.toString(),
    liquidityAccountId: spec.liquidity.toString(),
    clearingAccountId: spec.clearing.toString(),
    settlementMultilateralAccountId: spec.settlementMultilateral.toString()
  }

  return record
}

export class PersistedSpecStore implements SpecStore {
  private cacheAccount: SpecStoreCacheAccount
  private cacheTransfer: SpecStoreCacheTransfer

  constructor(private db: Knex) {
    this.cacheAccount = new SpecStoreCacheAccount()
    this.cacheTransfer = new SpecStoreCacheTransfer()
  }

  async queryAccountsAll(): Promise<Array<DfspAccountSpec>> {
    // Don't go to the cache
    const records = await this.db.from(TABLE_ACCOUNT)
      .orderBy('dfspId', 'desc')
      .orderBy('currency', 'desc')
      .limit(1000)
    if (records.length === 1000) {
      throw new Error(`getAllDfspAccountSpec - found ${records.length} records, something has probably gone terribly wrong.`)
    }

    const hydrated = records.map(record => hydrateSpecAccount(record))
    return hydrated
  }

  async queryAccountsDfsp(dfspId: string): Promise<Array<DfspAccountSpec>> {
    // Don't go to the cache
    const records = await this.db.from(TABLE_ACCOUNT)
      .where({dfspId})
      .orderBy('dfspId', 'desc')
      .orderBy('currency', 'desc')
      .limit(1000)
    if (records.length === 1000) {
      throw new Error(`getAllDfspAccountSpec - found ${records.length} records, something has probably gone terribly wrong.`)
    }

    const hydrated = records.map(record => hydrateSpecAccount(record))
    return hydrated
  }

  async getDfspAccountSpec(dfspId: string, currency: string): Promise<DfspAccountSpec | DfspAccountSpecNone> {
    // These values don't change very often, so it's safe to cache them
    const cacheResult = this.cacheAccount.get(dfspId, currency)
    if (cacheResult.type === 'HIT') {
      return cacheResult.contents
    }

    const result = await this.db.from(TABLE_ACCOUNT)
      .where({
        dfspId,
        currency,
        isTombstoned: false
      })
      .orderBy('createdDate', 'desc')
      .first();

    if (!result) {
      return { type: 'DfspAccountSpecNone' };
    }

    const spec = hydrateSpecAccount(result)
    this.cacheAccount.put(dfspId, currency, spec)

    return spec
  }

  async associateDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
    this.cacheAccount.delete(dfspId, currency)

    const record = dehydrateSpecAccount({
      type: "DfspAccountSpec",
      dfspId,
      currency,
      ...accounts,
    })
    await this.db.from(TABLE_ACCOUNT).insert({
      ...record,
      isTombstoned: false
    });
  }

  async tombstoneDfspAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
    await this.db.from(TABLE_ACCOUNT)
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

  async lookupTransferSpec(ids: Array<string>): Promise<Array<TransferSpec | TransferSpecNone>> {
    // First port of call, check the cache
    const transferSpecCached = this.cacheTransfer.get(ids)
    const transferSpecFoundSet: Record<string, TransferSpec> = {}
    const missingIds: Array<string> = []
    transferSpecCached.forEach((hitOrMiss, idx) => {
      if (hitOrMiss.type === 'HIT') {
        transferSpecFoundSet[hitOrMiss.contents.id] = hitOrMiss.contents
        return
      }

      const missingId = ids[idx]
      missingIds.push(missingId)
    })

    // Everything was in cache, we don't need to go to the database.
    if (missingIds.length === 0) {
      return transferSpecCached.map(tm => {
        assert(tm.type === 'HIT')
        return tm.contents
      })
    }

    const tranferSpecPersisted = await this.lookupTransferSpecPersisted(missingIds)
    tranferSpecPersisted.forEach(tm => {
      if (tm.type === 'TransferSpec') {
        transferSpecFoundSet[tm.id] = tm
        return
      }
    })

    // maintain ordering
    return ids.map(id => {
      if (transferSpecFoundSet[id]) {
        return transferSpecFoundSet[id]
      }
      return {
        type: 'TransferSpecNone',
        id
      }
    })
  }

  private async lookupTransferSpecPersisted(ids: Array<string>): Promise<Array<TransferSpec | TransferSpecNone>> {
    assert(ids)

    const queryResult = await this.db.from(TABLE_TRANSFER)
      .whereIn('id', ids)

    // maintain order of results, even when we find nulls
    const resultSet: Record<string, TransferSpecRecord> = queryResult.reduce((acc, curr) => {
      const record = curr as TransferSpecRecord
      assert(record.id)
      assert(record.payeeId)
      assert(record.payerId)
      assert(record.ilpCondition)
      assert(record.ilpPacket)

      acc[record.id] = record
    }, {})

    const results: Array<TransferSpec | TransferSpecNone> = []
    ids.forEach(id => {
      if (!resultSet[id]) {
        results.push({
          type: 'TransferSpecNone',
          id
        })
        return
      }

      const record = resultSet[id]
      results.push({
        type: 'TransferSpec',
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

  async saveTransferSpec(spec: Array<SaveTransferSpecCommand>): Promise<Array<SaveTransferSpecResult>> {
    try {
      const records: Array<TransferSpecRecord> = spec.map(m => {
        const record: TransferSpecRecord = {
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
      await this.db.from(TABLE_TRANSFER)
        .insert(records)
        .onConflict('id')
        .merge(['fulfilment']);
      this.cacheTransfer.put(spec.map(m => ({ type: 'TransferSpec', ...m })))

      return spec.map(m => {
        return {
          type: 'SUCCESS'
        }
      })
    } catch (err) {
      logger.error(`saveTransferSpec() - failed with error: ${err.message}`)
      return spec.map(m => {
        return {
          type: 'FAILURE'
        }
      })
    }
  }

  // updateTransferSpecFulfilment(transfersToUpdate: Array<{ id: string; fulfilment: string; }>): Promise<Array<SaveTransferSpecResult>> {
  //   try {
  //    // TODO: 


  //   } catch (err) {
  //     logger.error(`updateTransferSpecFulfilment() - failed with error: ${err.message}`)
  //     return transfersToUpdate.map(m => {
  //       return {
  //         type: 'FAILURE'
  //       }
  //     })
  //   }
  // }
}