import assert from "assert";
import { Knex } from "knex";
import { logger } from '../../shared/logger';
import { AttachTransferSpecFulfilment, DfspAccountIds, FundingAction, GetSpecNetDebitCapResult, SaveFundingSpecCommand, SaveSpecFundingResult, SaveSpecNetDebitCapResult, SaveSpecTransferResult, SaveTransferSpecCommand, SpecAccount, SpecAccountNone, SpecDfsp, SpecDfspNone, SpecFunding, SpecFundingNone, SpecNetDebitCap, SpecStore, SpecTransfer, SpecTransferNone, ValidateParticipantsResult } from "./SpecStore";
import { SpecStoreCacheDfsp } from "./SpecStoreCacheDfsp";
import { SpecStoreCacheTransfer } from "./SpecStoreCacheTransfer";
import { SpecStoreCacheAccount } from "./StoreCacheAccount";

interface SpecRecordAccount {
  id: number;
  dfspId: string;
  currency: string;
  deposit: string,
  unrestricted: string,
  unrestrictedLock: string,
  restricted: string,
  reserved: string,
  commitedOutgoing: string,
  clearingCredit: string,
  clearingSetup: string,
  clearingLimit: string,
  isTombstoned: boolean;
  createdDate: string;
  updatedDate: string;
}

interface SpecRecordTransfer {
  id: string
  currency: string
  amount: string
  expiration: string
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

interface SpecRecordFunding {
  transferId: string
  dfspId: string
  currency: string
  action: FundingAction
  reason: string
}

interface SpecRecordNetDebitCap {
  id: number
  dfspId: string
  currency: string
  type: 'UNLIMITED' | 'LIMITED'
  amount: string | null
  createdDate: string
  updatedDate: string
}

const TABLE_ACCOUNT = 'tigerBeetleSpecAccount'
const TABLE_TRANSFER = 'tigerBeetleSpecTransfer'
const TABLE_DFSP = 'tigerBeetleSpecDfsp'
const TABLE_FUNDING = 'tigerBeetleSpecFunding'
const TABLE_NET_DEBIT_CAP = 'tigerBeetleSpecNetDebitCap'

function hydrateSpecAccount(result: any): SpecAccount {
  const record = result as SpecRecordAccount;

  assert(record.dfspId)
  assert(record.currency)
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
    deposit: BigInt(record.deposit),
    unrestricted: BigInt(record.unrestricted),
    unrestrictedLock: BigInt(record.unrestrictedLock),
    restricted: BigInt(record.restricted),
    reserved: BigInt(record.reserved),
    commitedOutgoing: BigInt(record.commitedOutgoing),
    clearingCredit: BigInt(record.clearingCredit),
    clearingSetup: BigInt(record.clearingSetup),
    clearingLimit: BigInt(record.clearingLimit),
  }

  return spec
}

function dehydrateSpecAccount(spec: SpecAccount): any {
  const record = {
    dfspId: spec.dfspId,
    currency: spec.currency,
    deposit: spec.deposit,
    unrestricted: spec.unrestricted,
    unrestrictedLock: spec.unrestrictedLock,
    restricted: spec.restricted,
    reserved: spec.reserved,
    commitedOutgoing: spec.commitedOutgoing,
    clearingCredit: spec.clearingCredit,
    clearingSetup: spec.clearingSetup,
    clearingLimit: spec.clearingLimit,
  }

  return record
}

export class PersistedSpecStore implements SpecStore {
  private cacheAccount: SpecStoreCacheAccount
  private cacheTransfer: SpecStoreCacheTransfer
  private cacheDfsp: SpecStoreCacheDfsp

  constructor(private db: Knex) {
    this.cacheAccount = new SpecStoreCacheAccount()
    this.cacheTransfer = new SpecStoreCacheTransfer()
    this.cacheDfsp = new SpecStoreCacheDfsp()
  }

  async associateDfsp(dfspId: string, accountId: bigint): Promise<void> {
    // Invalidate cache before insert
    this.cacheDfsp.delete(dfspId)

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
    // Check cache first
    const cacheResult = this.cacheDfsp.get(dfspId)
    if (cacheResult.type === 'HIT') {
      return cacheResult.contents
    }

    // Cache miss - query database
    const result = await this.db.from(TABLE_DFSP)
      .where({ dfspId })
      .first();

    if (!result) {
      return { type: 'SpecDfspNone' };
    }

    const specRecord = result as SpecRecordDfsp
    const spec: SpecDfsp = {
      type: 'SpecDfsp',
      dfspId: specRecord.dfspId,
      accountId: BigInt(specRecord.accountId),
    }

    // Populate cache before returning
    this.cacheDfsp.put(dfspId, spec)

    return spec
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
      .where({ dfspId })
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
        deposit: accounts.deposit.toString(),
        unrestricted: accounts.unrestricted.toString(),
        unrestrictedLock: accounts.unrestrictedLock.toString(),
        restricted: accounts.restricted.toString(),
        reserved: accounts.reserved.toString(),
        commitedOutgoing: accounts.commitedOutgoing.toString(),
        clearingCredit: accounts.clearingCredit.toString(),
        clearingSetup: accounts.clearingSetup.toString(),
        clearingLimit: accounts.clearingLimit.toString(),
      })
      .update({
        isTombstoned: true,
        updatedDate: new Date()
      });

    this.cacheAccount.delete(dfspId, currency)
  }

  // The cache should be warmed up based on the prepare(), provided that the Kafka key partitioning
  // is set up properly. The only time we would expect a lot of misses is when scaling out the 
  // services or after a crash.
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
    
    logger.warn(`lookupTransferSpec() - cache miss for: ${missingIds.length}/${ids.length} ids.`)
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
        amount: record.amount,
        expiration: record.expiration,
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
          amount: m.amount,
          expiration: m.expiration,
          payerId: m.payerId,
          payeeId: m.payeeId,
          ilpCondition: m.condition,
          ilpPacket: m.ilpPacket,
        }

        return record
      })

      await this.db.from(TABLE_TRANSFER)
        .insert(records)
        .onConflict('id')
        .ignore()
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

  async attachTransferSpecFulfilment(attachments: Array<AttachTransferSpecFulfilment>):
    Promise<Array<SaveSpecTransferResult>> {
    try {
      await this.db.transaction(async (trx) => {
        for (const attachment of attachments) {
          await trx(TABLE_TRANSFER)
            .where({ id: attachment.id })
            .update({ fulfilment: attachment.fulfilment });
        }
      });

      this.cacheTransfer.putFulfilments(attachments)

      return attachments.map(() => ({
        type: 'SUCCESS'
      }))
    } catch (err) {
      logger.error(`attachTransferSpecFulfilment() - failed with error: ${err.message}`)
      return attachments.map(m => {
        return {
          type: 'FAILURE'
        }
      })
    }
  }

  async lookupFundingSpec(transferIds: Array<string>): Promise<Array<SpecFunding | SpecFundingNone>> {
    assert(transferIds)

    const queryResult = await this.db.from(TABLE_FUNDING)
      .whereIn('transferId', transferIds)

    // maintain order of results, even when we find nulls
    const resultSet: Record<string, SpecRecordFunding> = queryResult.reduce((acc, curr) => {
      const record = curr as SpecRecordFunding
      assert(record.transferId)
      assert(record.dfspId)
      assert(record.currency)
      assert(record.action)
      assert(record.reason)

      acc[record.transferId] = record
      return acc
    }, {})

    const results: Array<SpecFunding | SpecFundingNone> = []
    transferIds.forEach(transferId => {
      if (!resultSet[transferId]) {
        results.push({
          type: 'SpecFundingNone',
          transferId
        })
        return
      }

      const record = resultSet[transferId]
      results.push({
        type: 'SpecFunding',
        transferId: record.transferId,
        dfspId: record.dfspId,
        currency: record.currency,
        action: record.action,
        reason: record.reason
      })
    })

    return results
  }

  async saveFundingSpec(spec: Array<SaveFundingSpecCommand>): Promise<Array<SaveSpecFundingResult>> {
    try {
      const records: Array<SpecRecordFunding> = spec.map(m => {
        const record: SpecRecordFunding = {
          transferId: m.transferId,
          dfspId: m.dfspId,
          currency: m.currency,
          action: m.action,
          reason: m.reason
        }

        return record
      })

      await this.db.from(TABLE_FUNDING)
        .insert(records)

      return spec.map(m => {
        return {
          type: 'SUCCESS'
        }
      })
    } catch (err) {
      // Check if this is a duplicate key error
      if (err.code === 'ER_DUP_ENTRY' || err.errno === 1062) {
        logger.info(`saveFundingSpec() - funding spec already exists`)
        return spec.map(m => {
          return {
            type: 'EXISTS'
          }
        })
      }

      logger.error(`saveFundingSpec() - failed with error: ${err.message}`)
      return spec.map(m => {
        return {
          type: 'FAILURE'
        }
      })
    }
  }

  async saveSpecNetDebitCaps(netDebitCaps: Array<SpecNetDebitCap>): Promise<Array<SaveSpecNetDebitCapResult>> {
    const results: Array<SaveSpecNetDebitCapResult> = []

    for (const spec of netDebitCaps) {
      try {
        const record: Partial<SpecRecordNetDebitCap> = {
          dfspId: spec.dfspId,
          currency: spec.currency,
          type: spec.type,
          amount: spec.type === 'LIMITED' ? String(spec.amount) : null
        }

        // Use insert with onConflict to handle upsert
        await this.db.from(TABLE_NET_DEBIT_CAP)
          .insert(record)
          .onConflict(['dfspId', 'currency'])
          .merge(['type', 'amount', 'updatedDate'])

        results.push({ type: 'SUCCESS' })
      } catch (err) {
        logger.error(`saveSpecNetDebitCaps() - failed for dfspId: ${spec.dfspId}, currency: ${spec.currency}`, err)
        results.push({
          type: 'FAILURE',
          error: err instanceof Error ? err : new Error(String(err))
        })
      }
    }

    return results
  }

  async getSpecNetDebitCaps(dfspCurrencies: Array<{ dfspId: string; currency: string; }>): Promise<Array<GetSpecNetDebitCapResult>> {
    try {
      // Build WHERE IN clause for bulk query
      const queryBuilder = this.db.from(TABLE_NET_DEBIT_CAP)

      // Use whereIn with composite key matching
      queryBuilder.where(function() {
        for (const { dfspId, currency } of dfspCurrencies) {
          this.orWhere({ dfspId, currency })
        }
      })

      const records = await queryBuilder

      // Create a map for fast lookup
      const recordMap: Record<string, SpecRecordNetDebitCap> = {}
      for (const record of records) {
        const specRecord = record as SpecRecordNetDebitCap
        const key = `${specRecord.dfspId}:${specRecord.currency}`

        // Assertions to validate record integrity
        assert(specRecord.type === 'UNLIMITED' || specRecord.type === 'LIMITED',
          `Invalid type: ${specRecord.type}`)

        if (specRecord.type === 'UNLIMITED') {
          assert(specRecord.amount === null,
            `Expected amount to be NULL for UNLIMITED, got: ${specRecord.amount}`)
        } else {
          assert(specRecord.amount !== null && specRecord.amount !== undefined,
            `Expected amount to exist for LIMITED, got: ${specRecord.amount}`)
        }

        recordMap[key] = specRecord
      }

      // Map results in the same order as input queries
      const results: Array<GetSpecNetDebitCapResult> = dfspCurrencies.map(query => {
        const key = `${query.dfspId}:${query.currency}`
        const specRecord = recordMap[key]

        if (!specRecord) {
          return {
            type: 'FAILURE',
            query,
            error: new Error(`Net debit cap not found for dfspId: ${query.dfspId}, currency: ${query.currency}`)
          }
        }

        // Construct the discriminated union based on type
        const spec: SpecNetDebitCap = specRecord.type === 'UNLIMITED'
          ? {
              type: 'UNLIMITED',
              dfspId: specRecord.dfspId,
              currency: specRecord.currency
            }
          : {
              type: 'LIMITED',
              amount: Number(specRecord.amount),
              dfspId: specRecord.dfspId,
              currency: specRecord.currency
            }

        return {
          type: 'SUCCESS',
          result: spec
        }
      })

      return results
    } catch (err) {
      logger.error(`getSpecNetDebitCaps() - bulk query failed`, err)

      // Return FAILURE for all queries if the bulk operation fails
      return dfspCurrencies.map(query => ({
        type: 'FAILURE',
        query,
        error: err instanceof Error ? err : new Error(String(err))
      }))
    }
  }

  async validateTransferParticipants(params: {
    payerId: string
    payeeId: string
    currency: string
  }): Promise<ValidateParticipantsResult> {
    // Perform sequential lookups with early exit on first failure

    // Check payer DFSP
    const dfspSpecPayer = await this.queryDfsp(params.payerId)
    if (dfspSpecPayer.type === 'SpecDfspNone') {
      return {
        type: 'error',
        entity: 'dfsp_payer',
        participantId: params.payerId
      }
    }

    // Check payee DFSP
    const dfspSpecPayee = await this.queryDfsp(params.payeeId)
    if (dfspSpecPayee.type === 'SpecDfspNone') {
      return {
        type: 'error',
        entity: 'dfsp_payee',
        participantId: params.payeeId
      }
    }

    // Check payer account
    const accountSpecPayer = await this.getAccountSpec(params.payerId, params.currency)
    if (accountSpecPayer.type === 'SpecAccountNone') {
      return {
        type: 'error',
        entity: 'account_payer',
        participantId: params.payerId
      }
    }

    // Check payee account
    const accountSpecPayee = await this.getAccountSpec(params.payeeId, params.currency)
    if (accountSpecPayee.type === 'SpecAccountNone') {
      return {
        type: 'error',
        entity: 'account_payee',
        participantId: params.payeeId
      }
    }

    // All validations passed
    return {
      type: 'success',
      dfspSpecPayer,
      dfspSpecPayee,
      accountSpecPayer,
      accountSpecPayee
    }
  }
}