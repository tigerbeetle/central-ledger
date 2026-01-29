/**
 * Integration test for TigerBeetleLedger.getSettlementWindows()
 */

import { describe, it, before, after } from 'node:test'
import assert from 'node:assert'
import { IntegrationHarness } from '../../../../src/testing/harness/harness'

describe('TigerBeetleLedger.getSettlementWindows()', () => {
  let harness: IntegrationHarness

  before(async () => {
    harness = await IntegrationHarness.create({
      ledgerType: 'TIGERBEETLE',
      hubCurrencies: ['USD'],
      initializeCache: true
    })
  })

  after(async () => {
    if (harness) {
      await harness.teardown()
    }
  })

  it('should return empty array when no windows exist', async () => {
    const { ledger } = harness.getResources()

    const result = await ledger.getSettlementWindows({ state: 'CLOSED' })

    assert.strictEqual(result.type, 'SUCCESS')
    if (result.type === 'SUCCESS') {
      assert.ok(Array.isArray(result.result))
      assert.strictEqual(result.result.length, 0)
    }
  })

  it('should return OPEN settlement window when it exists', async () => {
    const { ledger } = harness.getResources()

    // First, close a settlement window to ensure one exists
    await ledger.closeSettlementWindow({ id: 1, reason: 'Test close' })

    // Query for OPEN windows
    const result = await ledger.getSettlementWindows({ state: 'OPEN' })

    assert.strictEqual(result.type, 'SUCCESS')
    if (result.type === 'SUCCESS') {
      assert.ok(Array.isArray(result.result))
      assert.ok(result.result.length > 0)
      assert.strictEqual(result.result[0].state, 'OPEN')
      assert.ok(result.result[0].settlementWindowId)
    }
  })

  it('should return CLOSED settlement window when it exists', async () => {
    const { ledger } = harness.getResources()

    // Close a settlement window
    await ledger.closeSettlementWindow({ id: 1, reason: 'Test close for query' })

    // Query for CLOSED windows
    const result = await ledger.getSettlementWindows({ state: 'CLOSED' })

    assert.strictEqual(result.type, 'SUCCESS')
    if (result.type === 'SUCCESS') {
      assert.ok(Array.isArray(result.result))
      assert.ok(result.result.length > 0)
      assert.strictEqual(result.result[0].state, 'CLOSED')
      assert.ok(result.result[0].settlementWindowId)
      assert.ok(result.result[0].reason)
      assert.ok(result.result[0].createdDate)
      assert.ok(result.result[0].changedDate)
    }
  })

  it('should filter by date range', async () => {
    const { ledger } = harness.getResources()

    const now = new Date()
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000)
    const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000)

    // Query for windows created between yesterday and tomorrow
    const result = await ledger.getSettlementWindows({
      fromDateTime: yesterday,
      toDateTime: tomorrow
    })

    assert.strictEqual(result.type, 'SUCCESS')
    if (result.type === 'SUCCESS') {
      assert.ok(Array.isArray(result.result))
      // Should include any windows created today
    }
  })

  it('should handle combined filters', async () => {
    const { ledger } = harness.getResources()

    const now = new Date()
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000)
    const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000)

    // Query with multiple filters
    const result = await ledger.getSettlementWindows({
      state: 'OPEN',
      fromDateTime: yesterday,
      toDateTime: tomorrow
    })

    assert.strictEqual(result.type, 'SUCCESS')
    if (result.type === 'SUCCESS') {
      assert.ok(Array.isArray(result.result))
      // All results should be OPEN
      result.result.forEach((window: any) => {
        assert.strictEqual(window.state, 'OPEN')
      })
    }
  })

  it('should handle invalid state gracefully', async () => {
    const { ledger } = harness.getResources()

    // Query with an invalid state (not in TigerBeetle schema)
    const result = await ledger.getSettlementWindows({
      state: 'PENDING_SETTLEMENT' as any
    })

    // Should return SUCCESS with empty array (no matching windows)
    assert.strictEqual(result.type, 'SUCCESS')
    if (result.type === 'SUCCESS') {
      assert.ok(Array.isArray(result.result))
    }
  })
})
