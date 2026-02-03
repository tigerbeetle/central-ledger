import { Knex } from "knex";
import { CommandResult, GetSettlementQuery, GetSettlementQueryResponse, GetSettlementWindowsQuery, GetSettlementWindowsQueryResponse, InternalSettlementState, LegacySettlementState, SettlementAccount, SettlementCloseWindowCommand, SettlementModel, SettlementParticipant, SettlementPrepareCommand, SettlementWindow, SettlementWindowState } from "./types";
import { QueryResult } from "src/shared/results";
import { logger } from '../../shared/logger';
import assert from "assert";
import Helper from './TigerBeetleLedgerHelper'


const TABLE_SETTLEMENT_WINDOW = `tigerbeetleSettlementWindow`
const TABLE_SETTLEMENT_WINDOW_MAPPING = `tigerbeetleSettlementWindowMapping`
const TABLE_SETTLEMENT = `tigerbeetleSettlement`
const TABLE_SETTLEMENT_BALANCE = `tigerbeetleSettlementBalance`


type SettlementWindowRecordState = 'OPEN' | 'CLOSED' | 'SETTLED';

type SettlementWindowRecord = {
  id: number,
  state: SettlementWindowRecordState,
  opened_at: Date,
  closed_at: Date | null,
  reason: string
}

interface SettlementLookupRecord {
  settlement_window_id: number,
  settlement_window_state: SettlementWindowRecordState,
  settlement_window_opened_at: Date,
  settlement_window_closed_at: Date,
  settlement_window_reason: string,
  settlement_window_created_at: Date,
  settlement_balance_id: number,
  settlement_balance_settlement_id: number,
  settlement_balance_dfspId: string,
  settlement_balance_currency: string,
  settlement_balance_amount: string,
  settlement_balance_direction: 'INBOUND' | 'OUTBOUND',
  settlement_balance_state: 'PENDING' | 'RESERVED' | 'COMMITTED' | 'ABORTED',
  settlement_balance_external_reference: string,
  settlement_balance_created_at: Date,
  settlement_balance_updated_at: Date,
  settlement_id: number,
  settlement_state: 'PENDING' | 'PROCESSING' | 'COMMITTED' | 'ABORTED',
  settlement_model: string,
  settlement_reason: string,
  settlement_created_at: Date,
}

type SettlementRecord = {
  id: bigint,
  state: InternalSettlementState,
  model: string,
  reason?: string,
  created_at: Date
}

function dedupeArray<T>(input: Array<T>, accessor: (accessor: T) => string | number): Array<T> {
  const map: Record<string | number, T> = {}
  input.forEach(item => map[accessor(item)] = item)
  return Object.values(map)
}

function extractSettlementWindowRecords(records: Array<SettlementLookupRecord>): Array<SettlementWindowRecord> {
  const settlementWindowRecords = records
    .filter(record => record.settlement_window_id!!)
    .map(record => {
      const settlementWindowRecord: SettlementWindowRecord = {
        id: record.settlement_window_id,
        state: record.settlement_window_state,
        opened_at: record.settlement_window_opened_at,
        closed_at: record.settlement_window_closed_at,
        reason: record.settlement_window_reason
      }
      assert(settlementWindowRecord.id, 'extractSettlementWindowRecords missing .id')
      assert(settlementWindowRecord.state, 'extractSettlementWindowRecords missing .state')
      assert(settlementWindowRecord.opened_at, 'extractSettlementWindowRecords missing .opened_at')
      assert(settlementWindowRecord.reason, 'extractSettlementWindowRecords missing .reason')

      return settlementWindowRecord
    })

  return dedupeArray(settlementWindowRecords, (r) => r.id)
}

const mapInternalSettlementStateToLegacy = (state: InternalSettlementState): LegacySettlementState => {
  switch (state) {
    case 'PENDING': return 'PENDING_SETTLEMENT'
    case 'PROCESSING': return 'PS_TRANSFERS_RECORDED'
    // case 'COMMITTED': return 'PS_TRANSFERS_COMMITTED'
    case 'COMMITTED': return 'SETTLED'
    case 'ABORTED': return 'ABORTED'
    default: {
      throw new Error(`unexpected state: ${state}. Expected it to be one of [PENDING | PROCESSED | COMMITTED | ABORTED]`)
    }
  }
}

export default class TigerBeetleSettlementModel {
  constructor(private db: Knex) { }

  public async closeSettlementWindow(cmd: SettlementCloseWindowCommand): Promise<CommandResult<void>> {
    const { id, reason } = cmd

    try {
      return await this.db.transaction(async (trx) => {
        // Special case: if id = 1, ensure it exists first
        if (id === 1) {
          await this.ensureOpenSettlementWindow(trx)
        }

        // 1. Verify the window exists and is OPEN
        const window = await trx('tigerbeetleSettlementWindow')
          .where('id', id)
          .first()

        if (!window) {
          return {
            type: 'FAILURE',
            error: new Error(`no existing window found`)
          }
        }

        if (window.state !== 'OPEN') {
          return {
            type: 'FAILURE',
            error: new Error(`Settlement window ${id} is not open (current state: ${window.state})`)
          }
        }

        // 2. Close the current window
        await trx('tigerbeetleSettlementWindow')
          .where('id', id)
          .update({
            state: 'CLOSED',
            closed_at: new Date(),
            reason
          })

        // 3. Create a new OPEN window
        await trx('tigerbeetleSettlementWindow').insert({
          state: 'OPEN',
          opened_at: new Date(),
          reason: 'New settlement window opened'
        })

        return { type: 'SUCCESS' }
      })
    } catch (err: any) {
      logger.error(`closeSettlementWindow failed: ${err.message}`, { err })
      return {
        type: 'FAILURE',
        error: new Error(`Failed to close settlement window: ${err.message}`)
      }
    }
  }

  public async getSettlementWindows(query: GetSettlementWindowsQuery):
    Promise<QueryResult<GetSettlementWindowsQueryResponse>> {

    try {
      return await this.db.transaction(async trx => {
        await this._ensureOpenSettlementWindowInTransaction(trx)

        let settlementLookupQuery = trx<SettlementLookupRecord>(
          `${TABLE_SETTLEMENT_WINDOW} as settlement_window`
        )
          .leftJoin(
            `${TABLE_SETTLEMENT_WINDOW_MAPPING} as settlement_window_mapping`,
            'settlement_window_mapping.window_id',
            'settlement_window.id'
          )
          .leftJoin(
            `${TABLE_SETTLEMENT_BALANCE} as settlement_balance`,
            'settlement_balance.settlement_id',
            'settlement_window_mapping.settlement_id'
          )
          .leftJoin(
            `${TABLE_SETTLEMENT} as settlement`,
            'settlement.id',
            'settlement_window_mapping.settlement_id'
          )
          .select(
            'settlement_window.id as settlement_window_id',
            'settlement_window.state as settlement_window_state',
            'settlement_window.opened_at as settlement_window_opened_at',
            'settlement_window.closed_at as settlement_window_closed_at',
            'settlement_window.reason as settlement_window_reason',
            'settlement_window.created_at as settlement_window_created_at',
            'settlement_balance.id as settlement_balance_id',
            'settlement_balance.settlement_id as settlement_balance_settlement_id',
            'settlement_balance.dfspId as settlement_balance_dfspId',
            'settlement_balance.currency as settlement_balance_currency',
            'settlement_balance.owing as settlement_balance_owing',
            'settlement_balance.owed as settlement_balance_owed',
            'settlement_balance.state as settlement_balance_state',
            'settlement_balance.external_reference as settlement_balance_external_reference',
            'settlement_balance.created_at as settlement_balance_created_at',
            'settlement_balance.updated_at as settlement_balance_updated_at',
            'settlement.id as settlement_id',
            'settlement.state as settlement_state',
            'settlement.model as settlement_model',
            'settlement.reason as settlement_reason',
            'settlement.created_at as settlement_created_at',
          )
          .orderBy('settlement_window.opened_at', 'desc')

        settlementLookupQuery = this.applySettlementWindowFilters(settlementLookupQuery, query)
        const settlementLookupRows: Array<SettlementLookupRecord> = await settlementLookupQuery

        // Now map and turn into what we need to!
        const windowRows = extractSettlementWindowRecords(settlementLookupRows)

        if (settlementLookupRows.length === 0) return { type: 'SUCCESS', result: [] }

        const windows = windowRows.map((row: SettlementWindowRecord) => ({
          id: row.id,
          state: row.state,
          reason: row.reason,
          createdDate: row.opened_at,
          changedDate: row.closed_at || undefined,
          // content: contentByWindow.get(row.id) || []
          content: []
        }))


        return {
          type: 'SUCCESS',
          result: windows
        }
      })
    } catch (err) {
      return {
        type: 'FAILURE',
        error: err
      }
    }
  }

  private async lookupCurrencyForSettlementModel(trx: Knex.Transaction, model: string): Promise<string> {
    assert(trx)
    assert(model)
    assert(typeof model === 'string')
    try {
      const [row] = await trx<{ currency: string }>('tigerbeetleSettlementModel')
        .where('name', model)
        .select('currency')
        .limit(1) as Array<{ currency: string }>
      assert(row, `could not find currency for model: ${model}`)
      assert(row.currency, `could not find currency for model: ${model}`)

      return row.currency
    } catch (err) {
      logger.error(`lookupCurrencyForSettlementModel: failed with error: ${err.message}`)
      throw err
    }
  }

  public async settlementPrepare(
    cmd: SettlementPrepareCommand,
    paymentSummer: (startTime: Date, endTime: Date, currency: string) => Promise<Record<string, { owing: number, owed: number }>>):
    Promise<CommandResult<{ id: number }>> {

    const { windowIds, model, reason, now } = cmd
    assert(windowIds)
    assert(Array.isArray(windowIds))
    windowIds.forEach(windowId => {
      assert(typeof windowId === 'number', `expected windowId to be a number, instead found: \
        ${typeof windowId}`
      )
    })
    assert(model)
    assert(reason)
    assert(now)

    try {
      return await this.db.transaction(async (trx) => {
        const currency = await this.lookupCurrencyForSettlementModel(trx, model)

        // Create the settlement record
        const [settlementId] = await trx(TABLE_SETTLEMENT).insert({
          state: 'PENDING',
          model,
          reason,
          created_at: now
        })

        // Link windows to this settlement
        const windowMappings: Array<{ settlement_id: number, window_id: number }> = windowIds.map(windowId => {
          return {
            settlement_id: settlementId,
            window_id: windowId
          }
        })
        await trx(TABLE_SETTLEMENT_WINDOW_MAPPING).insert(windowMappings)

        // Get time ranges for all windows
        const windows = await trx('tigerbeetleSettlementWindow')
          .whereIn('id', windowIds)
          .select('id', 'opened_at', 'closed_at')

        // Get the net movements for each window, and sum together
        const [firstWindow, ...remainingWindows] = windows
        let settlementSum = await paymentSummer(
          firstWindow.opened_at,
          firstWindow.closed_at,
          currency
        )
        for (const window of remainingWindows) {
          const windowNet = await paymentSummer(
            window.opened_at, window.closed_at, currency
          )

          // Merge together
          settlementSum = Helper.mergeWith(settlementSum, windowNet, (a, b) => {
            return {
              owing: a.owing + b.owing,
              owed: a.owed + b.owed
            }
          })
        }

        // Convert to settlement balances and insert
        const balances = []
        for (const dfspId of Object.keys(settlementSum)) {
          const { owing, owed } = settlementSum[dfspId]

          balances.push({
            settlement_id: settlementId,
            dfspId,
            currency,
            owing,
            owed,
            state: 'PENDING'
          })
        }

        if (balances.length > 0) {
          await trx(TABLE_SETTLEMENT_BALANCE).insert(balances)
        }

        return { type: 'SUCCESS', result: { id: settlementId } }
      })
    } catch (err: any) {
      logger.error(`settlementPrepare failed: ${err.message}`, { err })
      return {
        type: 'FAILURE',
        error: new Error(`Failed to prepare settlement: ${err.message}`)
      }
    }

  }

  public async getSettlement(query: GetSettlementQuery): Promise<GetSettlementQueryResponse> {
    try {
      logger.info(`TigerBeetleSettlementModel.getSettlement() with id: ${query.id}`)

      // Get settlement record
      const settlement = await this.db<SettlementRecord>(TABLE_SETTLEMENT)
        .where('id', query.id)
        .first()

      if (!settlement) {
        return { type: 'NOT_FOUND' }
      }

      // Get settlement windows
      const windowRows = await this.db(`${TABLE_SETTLEMENT_WINDOW_MAPPING} as tswm`)
        .join('tigerbeetleSettlementWindow as tsw', 'tsw.id', 'tswm.window_id')
        .where('tswm.settlement_id', query.id)
        .select(
          'tsw.id',
          'tsw.state',
          'tsw.reason',
          'tsw.opened_at',
          'tsw.closed_at'
        )

      // Get window content (same pattern as getSettlementWindows)
      const windowIds = windowRows.map((w: any) => w.id)
      const contentRows = windowIds.length > 0
        ? await this.db(`${TABLE_SETTLEMENT_WINDOW_MAPPING} as tswm`)
          .join('tigerbeetleSettlement as ts', 'ts.id', 'tswm.settlement_id')
          .join('tigerbeetleSettlementBalance as tsb', 'tsb.settlement_id', 'ts.id')
          .whereIn('tswm.window_id', windowIds)
          .select(
            'tswm.window_id',
            'tsb.id',
            'ts.state',
            'tsb.currency as currencyId',
            'ts.created_at as createdDate',
            'ts.created_at as changedDate'
          )
        : []

      // Group content by window_id
      const contentByWindow = new Map<number, Array<any>>()
      for (const row of contentRows) {
        if (!contentByWindow.has(row.window_id)) {
          contentByWindow.set(row.window_id, [])
        }
        contentByWindow.get(row.window_id)!.push({
          id: row.id,
          state: row.state,
          ledgerAccountType: 'POSITION',
          currencyId: row.currencyId,
          createdDate: row.createdDate,
          changedDate: row.changedDate
        })
      }

      // Transform windows with content
      const settlementWindows: SettlementWindow[] = windowRows.map((w: any) => ({
        id: w.id,
        state: w.state as SettlementWindowState,
        reason: w.reason ?? '',
        createdDate: w.opened_at,
        changedDate: w.closed_at ?? w.opened_at,
        content: contentByWindow.get(w.id) || []
      }))

      // 5. Get participant balances with participantId from participant table
      const balanceRows = await this.db(`${TABLE_SETTLEMENT_BALANCE} as tsb`)
        .join('participant as p', 'p.name', 'tsb.dfspId')
        .where('tsb.settlement_id', query.id)
        .select(
          'tsb.id',
          'p.participantId',
          'tsb.currency',
          'tsb.owing',
          'tsb.owed',
          'tsb.state'
        )

      // 6. Group and transform participant balances
      const participantMap = new Map<number, SettlementAccount[]>()

      for (const balance of balanceRows) {
        const account: SettlementAccount = {
          id: balance.id,
          state: balance.state,
          reason: '',
          owing: balance.owing,
          owed: balance.owed,
          currency: balance.currency,
          netSettlementAmount: {
            amount: `${balance.owing - balance.owed}`,
            currency: balance.currency
          }
        }

        if (!participantMap.has(balance.participantId)) {
          participantMap.set(balance.participantId, [])
        }
        participantMap.get(balance.participantId)!.push(account)
      }

      const participants: SettlementParticipant[] = Array.from(participantMap.entries())
        .map(([participantId, accounts]) => ({
          id: participantId,
          accounts
        }))

      // 7. Return response
      return {
        type: 'FOUND',
        id: Number(settlement.id),
        settlementModel: settlement.model,
        state: mapInternalSettlementStateToLegacy(settlement.state),
        reason: settlement.reason ?? '',
        createdDate: settlement.created_at,
        changedDate: settlement.created_at,
        settlementWindows,
        participants
      }
    } catch (err: any) {
      logger.error(`TigerBeetleSettlementModel.getSettlement() failed: ${err.message}`, { err })
      return {
        type: 'FAILED',
        error: err
      }
    }
  }


  /**
   * Apply filters to settlement window query
   */
  private applySettlementWindowFilters(queryBuilder: Knex.QueryBuilder, query: GetSettlementWindowsQuery) {
    if (query.state) {
      queryBuilder.where('settlement_window.state', query.state)
    }

    if (query.fromDateTime) {
      queryBuilder.where('settlement_window.opened_at', '>=', query.fromDateTime)
    }

    if (query.toDateTime) {
      queryBuilder.where('settlement_window.opened_at', '<=', query.toDateTime)
    }

    if (query.participantId !== undefined) {
      queryBuilder.where('settlement_window.participant_id', query.participantId.toString())
    }

    if (query.currency) {
      queryBuilder.where('settlement_window.currency', query.currency)
    }

    return queryBuilder
  }

  /**
   * Helper to ensure settlement window 1 exists within a transaction
   */
  private async _ensureOpenSettlementWindowInTransaction(trx: Knex.Transaction): Promise<void> {
    const window = await trx(TABLE_SETTLEMENT_WINDOW)
      .where('id', 1)
      .first()

    if (!window) {
      logger.info('Creating initial settlement window (id=1) with unix epoch timestamp')
      await trx('tigerbeetleSettlementWindow').insert({
        id: 1,
        state: 'OPEN',
        opened_at: new Date(0), // Unix epoch
        reason: 'Initial settlement window'
      })
    }
  }

  /**
   * Ensures an OPEN settlement window exists with id=1.
   * Creates the first window with opened_at = unix epoch if it doesn't exist.
   * Can be called with or without a transaction - will create one if not provided.
   */
  private async ensureOpenSettlementWindow(trx?: Knex.Transaction): Promise<void> {
    if (trx) {
      await this._ensureOpenSettlementWindowInTransaction(trx)
    } else {
      await this.db.transaction(async (newTrx) => {
        await this._ensureOpenSettlementWindowInTransaction(newTrx)
      })
    }
  }





}