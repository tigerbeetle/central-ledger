import assert from "assert";
import { Knex } from "knex";
import { DfspAccountIds, SpecStore, SaveTransferSpecCommand, SpecAccount, SpecAccountNone, SpecTransfer, SpecTransferNone, SaveSpecTransferResult, SpecDfsp, SpecDfspNone } from "./SpecStore";
import { SpecStoreCacheAccount } from "./StoreCacheAccount";
import { logger } from '../../shared/logger';
import { SpecStoreCacheTransfer } from "./SpecStoreCacheTransfer";

interface SpecRecordAccount {
  id: number;
  dfspId: string;
  currency: string;
  // TODO: remove
  collateralAccountId: string;
  liquidityAccountId: string;
  clearingAccountId: string;
  settlementMultilateralAccountId: string;
  netDebitCapAccountId: string;

  isTombstoned: boolean;
  createdDate: string;
  updatedDate: string;

  deposit: string,
  unrestricted: string,
  unrestrictedLock: string,
  restricted: string,
  reserved: string,
  commitedOutgoing: string,
}

interface SpecRecordTransfer {
  id: string
  currency: string
  payerId: string
  payeeId: string
  ilpCondition: string
  ilpPacket: string
  fulfilment?: string
}

interface SpecRecordDfsp {
  dfspId: string,
  accountId: bigint
}

const TABLE_ACCOUNT = 'tigerBeetleSpecAccount'
const TABLE_TRANSFER = 'tigerBeetleSpecTransfer'
const TABLE_DFSP = 'tigerBeetleSpecDfsp'

function hydrateSpecAccount(result: any): SpecAccount {
  const record = result as SpecRecordAccount;

  assert(record.dfspId)
  assert(record.currency)
  assert(record.collateralAccountId)
  assert(record.liquidityAccountId)
  assert(record.clearingAccountId)
  assert(record.settlementMultilateralAccountId)

  assert(record.deposit)
  assert(record.unrestricted)
  assert(record.unrestrictedLock)
  assert(record.restricted)
  assert(record.reserved)
  assert(record.commitedOutgoing)

  const spec: SpecAccount = {
    type: 'SpecAccount',
    dfspId: record.dfspId,
    currency: record.currency,
    // TODO(LD): remove me
    collateral: BigInt(record.collateralAccountId),
    liquidity: BigInt(record.liquidityAccountId),
    clearing: BigInt(record.clearingAccountId),
    settlementMultilateral: BigInt(record.settlementMultilateralAccountId),

    
    deposit: BigInt(record.deposit),
    unrestricted: BigInt(record.unrestricted),
    unrestrictedLock: BigInt(record.unrestrictedLock),
    restricted: BigInt(record.restricted),
    reserved: BigInt(record.reserved),
    commitedOutgoing: BigInt(record.commitedOutgoing),
    netDebitCap: BigInt(record.netDebitCapAccountId),
  }

  return spec
}

function dehydrateSpecAccount(spec: SpecAccount): any {
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

  async associateDfsp(dfspId: string, accountId: bigint): Promise<void> {
    await this.db.from(TABLE_DFSP).insert({
      dfspId,
      accountId: accountId.toString()
    });
  }

  async queryDfspsAll(): Promise<Array<SpecDfsp>> {
    const records = await this.db.from(TABLE_DFSP)
      .orderBy('dfspId', 'asc')
      .limit(1000)

    if (records.length === 1000) {
      throw new Error(`queryDfspsAll - found ${records.length} records, something has probably gone terribly wrong.`)
    }

    return records.map(record => {
      const specRecord = record as SpecRecordDfsp
      return {
        type: 'SpecDfsp',
        dfspId: specRecord.dfspId,
        accountId: BigInt(specRecord.accountId)
      }
    })
  }

  async queryDfsp(dfspId: string): Promise<SpecDfsp | SpecDfspNone> {
    const result = await this.db.from(TABLE_DFSP)
      .where({ dfspId })
      .first();

    if (!result) {
      return { type: 'SpecDfspNone' };
    }

    const specRecord = result as SpecRecordDfsp
    return {
      type: 'SpecDfsp',
      dfspId: specRecord.dfspId,
      accountId: BigInt(specRecord.accountId),
    }
  }

  async queryAccountsAll(): Promise<Array<SpecAccount>> {
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

  async queryAccounts(dfspId: string): Promise<Array<SpecAccount>> {
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

  async getAccountSpec(dfspId: string, currency: string): Promise<SpecAccount | SpecAccountNone> {
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
      return { type: 'SpecAccountNone' };
    }

    const spec = hydrateSpecAccount(result)
    this.cacheAccount.put(dfspId, currency, spec)

    return spec
  }

  async associateAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
    this.cacheAccount.delete(dfspId, currency)

    const record = dehydrateSpecAccount({
      type: 'SpecAccount',
      dfspId,
      currency,
      ...accounts,
    })
    await this.db.from(TABLE_ACCOUNT).insert({
      ...record,
      isTombstoned: false
    });
  }

  async tombstoneAccounts(dfspId: string, currency: string, accounts: DfspAccountIds): Promise<void> {
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

  async lookupTransferSpec(ids: Array<string>): Promise<Array<SpecTransfer | SpecTransferNone>> {
    // First port of call, check the cache
    const transferSpecCached = this.cacheTransfer.get(ids)
    const transferSpecFoundSet: Record<string, SpecTransfer> = {}
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
      if (tm.type === 'SpecTransfer') {
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
        type: 'SpecTransferNone',
        id
      }
    })
  }

  private async lookupTransferSpecPersisted(ids: Array<string>): Promise<Array<SpecTransfer | SpecTransferNone>> {
    assert(ids)

    const queryResult = await this.db.from(TABLE_TRANSFER)
      .whereIn('id', ids)

    // maintain order of results, even when we find nulls
    const resultSet: Record<string, SpecRecordTransfer> = queryResult.reduce((acc, curr) => {
      const record = curr as SpecRecordTransfer
      assert(record.id)
      assert(record.currency)
      assert(record.payeeId)
      assert(record.payerId)
      assert(record.ilpCondition)
      assert(record.ilpPacket)

      acc[record.id] = record
    }, {})

    const results: Array<SpecTransfer | SpecTransferNone> = []
    ids.forEach(id => {
      if (!resultSet[id]) {
        results.push({
          type: 'SpecTransferNone',
          id
        })
        return
      }

      const record = resultSet[id]
      results.push({
        type: 'SpecTransfer',
        id: record.id,
        currency: record.currency,
        payerId: record.payerId,
        payeeId: record.payeeId,
        condition: record.ilpCondition,
        ilpPacket: record.ilpPacket,
        fulfilment: record.fulfilment ? record.fulfilment : undefined
      })
    })

    return results
  }

  async saveTransferSpec(spec: Array<SaveTransferSpecCommand>): Promise<Array<SaveSpecTransferResult>> {
    try {
      const records: Array<SpecRecordTransfer> = spec.map(m => {
        const record: SpecRecordTransfer = {
          id: m.id,
          currency: m.currency,
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
      this.cacheTransfer.put(spec.map(m => ({ type: 'SpecTransfer', ...m })))

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
}