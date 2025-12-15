import { describe, it } from "node:test";
import Helper from "./TigerBeetleLedgerHelper";
import assert from "assert";

describe('TigerBeetleLedgerHelper', () => {

  describe('validateFulfilmentAndCondition', () => {
    it('validates that the fulfilment correctly matches the condition', () => {
      // Arrange
      const fulfilment = 'V-IalzIzy-zxy0SrlY1Ku2OE9aS4KgGZ0W-Zq5_BeC0'
      const condition = 'GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA'

      // Act
      const result = Helper.validateFulfilmentAndCondition(fulfilment, condition)

      // Assert
      assert(result)
      assert.equal(result.type, 'PASS')
    })
  })

  describe('id mapping', () => {
    it('maps from a mojaloop uuid to a tigerbeetle bigint id and back again', () => {
      const source = `4f73c4b8-6f4a-4321-a3eb-a972d0caab69`
      const tigerBeetleId = Helper.fromMojaloopId(source)
      assert(tigerBeetleId === 105610115770446691108652388357673495401n)

      const mojaloopId = Helper.toMojaloopId(tigerBeetleId)
      assert(source === mojaloopId)
    })
  })
})