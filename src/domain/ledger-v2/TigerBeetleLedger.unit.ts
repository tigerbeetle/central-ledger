import { describe, it } from "node:test";
import TigerBeetleLedger from "./TigerBeetleLedger";
import assert from "assert";

describe('TigerBeetleLedger', () => {

  describe('validateFulfilmentAndCondition', () => {
    it('validates that the fulfilment correctly matches the condition', () => {
      // Arrange
      const fulfilment = 'V-IalzIzy-zxy0SrlY1Ku2OE9aS4KgGZ0W-Zq5_BeC0'
      const condition = 'GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA'

      // Act
      const result = TigerBeetleLedger.validateFulfilmentAndCondition(fulfilment, condition)

      // Assert
      assert(result)
      assert.equal(result.type, 'PASS')
    })
  })
})