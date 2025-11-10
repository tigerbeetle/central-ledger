import { describe, it } from "node:test";
import { checkSnapshotString, SnapshotMismatch, SnapshotResultType } from "./snapshot";
import assert from "assert";

describe('snapshot testing test', () => {


  describe('checkSnapshotString', () => {
    it('fails as expected on mismatching input', () => {
      // Arrange
      const actual = JSON.stringify({
        "abc": 123,
        "def": new Date(),
        "HIJ": 'aaa'
      }, null, 2)
      const snapshot = JSON.stringify({
        "abc": 123,
        "def": ':ignore',
        "HIJ": 'bbb'
      }, null, 2)

      // Act
      const result = checkSnapshotString(actual, snapshot)

      // Assert
      assert.equal(result.type, SnapshotResultType.MISMATCH)
      // lol we need a snapshot test to test the snapshot test diff
      console.log('diff is', (result as SnapshotMismatch<String>).diff)
    })

    it('passes as expected on matching input', () => {
      // Arrange
      const actual = JSON.stringify({
        "abc": 123,
        "def": 456
      }, null, 2)
      const snapshot = JSON.stringify({
        "abc": 123,
        "def": 456,
      }, null, 2)

      // Act
      const result = checkSnapshotString(actual, snapshot)

      // Assert
      assert.equal(result.type, SnapshotResultType.MATCH)
    })

    it('fails if something mismatches before the :ignore', () => {
      // Arrange
      const actual = JSON.stringify({
        "abc": 123,
        "defg": '456'
      }, null, 2)
      const snapshot = JSON.stringify({
        "abc": 123,
        "def": ":ignore",
      }, null, 2)

      // Act
      const result = checkSnapshotString(actual, snapshot)

      // Assert
      assert.equal(result.type, SnapshotResultType.MISMATCH)
      console.log('diff is', (result as SnapshotMismatch<String>).diff)
    })
  })
})