import { Knex } from 'knex';
import assert from 'node:assert';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { CommitTransferDto, CreateTransferDto } from '../../handlers-v2/types';
import { IntegrationHarness } from '../../testing/harness/harness';
import { TestUtils } from '../../testing/testutils';
import { checkSnapshotObject, checkSnapshotString, unwrapSnapshot } from '../../testing/snapshot';
import TigerBeetleLedger from "./TigerBeetleLedger";
import TigerBeetleSettlementModel from './TigerBeetleSettlementModel';
import { GetSettlementQueryResponse, GetSettlementWindowsQuery, GetSettlementWindowsQueryResponse, SettlementPrepareCommand, SettlementPrepareResult } from './types';
import * as snapshots from './__snapshots__/TigerBeetleSettlementModel.int.snapshot'

const participantService = require('../participant')

describe('TigerBeetleSettlementModel', () => {
  let harness: IntegrationHarness;
  let ledger: TigerBeetleLedger;
  let db: Knex
  let settlementModel: TigerBeetleSettlementModel


  const setupDfsp = async (dfspId: string, depositAmount: number, currency: string = 'USD') => {
    await participantService.ensureExists(dfspId)
    TestUtils.unwrapSuccess(await ledger.createDfsp({
      dfspId,
      currencies: [currency]
    }))
    TestUtils.unwrapSuccess(await ledger.deposit({
      transferId: randomUUID(),
      dfspId,
      currency,
      amount: depositAmount,
      reason: 'Initial deposit'
    }))
  }

  const sendFromTo = async (payer: string, payee: string, amount: string, currency: string = 'USD') => {
    // Send 50 from b1 -> b2, so that b2 will have Clearing Credit of 50
    const transferId = randomUUID()
    const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
    const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)
    const payload: CreateTransferDto = {
      transferId,
      payerFsp: payer,
      payeeFsp: payee,
      amount: { amount: amount, currency: currency },
      ilpPacket,
      condition,
      expiration: new Date(Date.now() + 60000).toISOString()
    }
    const prepareInput = TestUtils.buildValidPrepareInput(transferId, payload)
    const prepareResult = await ledger.prepare(prepareInput)
    assert.equal(prepareResult.type, 'PASS')

    const fulfilPayload: CommitTransferDto = {
      transferState: 'COMMITTED',
      fulfilment,
      completedTimestamp: new Date().toISOString()
    }
    const fulfilInput = TestUtils.buildValidFulfilInput(transferId, fulfilPayload, payee)
    const fulfilResult = await ledger.fulfil(fulfilInput)
    assert.equal(fulfilResult.type, 'PASS')
  }

  before(async () => {
    harness = await IntegrationHarness.create({
      hubCurrencies: ['USD'],
      provisionDfsps: [
        { dfspId: 'dfsp_a', currencies: ['USD'], startingDeposits: [100000] },
        { dfspId: 'dfsp_b', currencies: ['USD'], startingDeposits: [100000] },
        { dfspId: 'dfsp_c', currencies: ['USD'], startingDeposits: [100000] },
      ]
    })

    ledger = harness.getResources().ledger as TigerBeetleLedger
    db = harness.getResources().db
    settlementModel = new TigerBeetleSettlementModel(db)

    // Global middleware to log all queries
    // TODO: add into an option on the harness
    // db.on('query', (query) => {
    //   console.log('SQL:', query.sql);
    //   console.log('Bindings:', query.bindings);
    // });
  })

  after(async () => {
    await harness.teardown()
  })

  describe('getSettlementWindows', () => {
    it('gets the default settlement window', async () => {
      // Arrange
      const query: GetSettlementWindowsQuery = {
        fromDateTime: new Date(0)
      }

      // Act
      const window = TestUtils.unwrapSuccess<GetSettlementWindowsQueryResponse>(
        await settlementModel.getSettlementWindows(query)
      )

      // Assert
      const snapshot = [
        {
          id: 1,
          createdDate: new Date(0),
          reason: 'Initial settlement window',
          state: 'OPEN',
          content: []
        }
      ]
      unwrapSnapshot(checkSnapshotObject(window, snapshot))
    })

    it('gets the settlement windows after closing the settlement window', async () => {
      // Arrange
      let windows = TestUtils.unwrapSuccess<GetSettlementWindowsQueryResponse>(
        await settlementModel.getSettlementWindows({ fromDateTime: new Date(0) })
      )
      TestUtils.unwrapSuccess<GetSettlementWindowsQueryResponse>(
        await settlementModel.closeSettlementWindow({ id: windows[0].id, reason: 'test close' })
      )

      // Act
      windows = TestUtils.unwrapSuccess<GetSettlementWindowsQueryResponse>(
        await settlementModel.getSettlementWindows({ fromDateTime: new Date(0) })
      )

      // Assert
      const snapshot = [
        {
          id: 1,
          createdDate: new Date(0),
          changedDate: ':ignore',
          reason: 'test close',
          state: 'CLOSED',
          content: []
        },
        {
          id: 2,
          createdDate: ':ignore',
          reason: 'New settlement window opened',
          state: 'OPEN',
          content: []
        }
      ]
      unwrapSnapshot(checkSnapshotObject(windows, snapshot))
    })
  })

  describe('settlementPrepare', () => {
    it.only('prepares the settlement', async () => {
      // Arrange
      await sendFromTo('dfsp_a', 'dfsp_b', '50.00')
      await sendFromTo('dfsp_b', 'dfsp_c', '75.00')
      await sendFromTo('dfsp_a', 'dfsp_c', '10.00')
      await sendFromTo('dfsp_c', 'dfsp_b', '10.00')
      await sendFromTo('dfsp_c', 'dfsp_a', '45.00')

      

      let windows = TestUtils.unwrapSuccess<GetSettlementWindowsQueryResponse>(
        await settlementModel.getSettlementWindows({ fromDateTime: new Date(0) })
      )
      TestUtils.unwrapSuccess<GetSettlementWindowsQueryResponse>(
        await settlementModel.closeSettlementWindow({ id: windows[0].id, reason: 'test close' })
      )

      // Act
      const now = new Date()
      const cmdSettlementPrepare: SettlementPrepareCommand = {
        windowIds: [windows[0].id],
        model: 'DEFERRED_MULTILATERAL_NET_USD',
        reason: 'settlement prepare test',
        now,
      }
      const result = await settlementModel.settlementPrepare(
        cmdSettlementPrepare, ledger.paymentSummer.bind(ledger)
      )

      // Assert
      assert(result.type === 'SUCCESS', 'expected settlementPrepare() to return .type of SUCCESS')
      const settlementId = result.result.id
      const { type, ...settlement } = await settlementModel.getSettlement({ id: settlementId })
      assert(type === 'FOUND')
      const snapshot = snapshots.prepares_the_settlement
      unwrapSnapshot(checkSnapshotObject(settlement, snapshot))
    })
  })
})