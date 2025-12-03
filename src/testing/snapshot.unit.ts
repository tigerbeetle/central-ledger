import { describe, it } from "node:test";
import { checkSnapshotObject, checkSnapshotString, SnapshotMismatch, SnapshotResultType } from "./snapshot";
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

    it('can match :string to be any string', () => {
      // Arrange
      const actual = `{
        "abc": 123,
        "defg": "hello"
      }`.replaceAll('   ', '')
      const snapshot = `{
        "abc": 123,
        "defg": ":string"
      }`.replaceAll('   ', '')
  
      // Act
      const result = checkSnapshotString(actual, snapshot)


      // Assert
      assert.equal(result.type, SnapshotResultType.MATCH)
    })

    it('fails to match a number to :string', () => {
      // Arrange
      const actual = `{
        "abc": 123,
        "defg": 100
      }`.replaceAll('   ', '')
      const snapshot = `{
        "abc": 123,
        "defg": ":string"
      }`.replaceAll('   ', '')
  
      // Act
      const result = checkSnapshotString(actual, snapshot)

      // Assert
      console.log('diff is', (result as SnapshotMismatch<String>).diff)
      assert.equal(result.type, SnapshotResultType.MISMATCH)
    })

    it('can match :integer to be any integer', () => {
      // Arrange
      const actual = `{
        "abc": 123,
        "defg": "hello"
      }`.replaceAll('   ', '')
      const snapshot = `{
        "abc": ":integer",
        "defg": ":string"
      }`.replaceAll('   ', '')
  
      // Act
      const result = checkSnapshotString(actual, snapshot)


      // Assert
      console.log('diff is', (result as SnapshotMismatch<String>).diff)
      assert.equal(result.type, SnapshotResultType.MATCH)
    })

    it('fails to match a string to :integer', () => {
      // Arrange
      const actual = `{
        "abc": '123',
        "defg": 100
      }`.replaceAll('   ', '')
      const snapshot = `{
        "abc": :integer,
        "defg": 100
      }`.replaceAll('   ', '')
  
      // Act
      const result = checkSnapshotString(actual, snapshot)

      // Assert
      console.log('diff is', (result as SnapshotMismatch<String>).diff)
      assert.equal(result.type, SnapshotResultType.MISMATCH)
    })
  })

  describe('checkSnapshotObject', () => {
    it('can match :string to be any string', () => {
      // Arrange
      const actual = {
        "abc": 123,
        "defg": "hello"
      }
      const snapshot = {
        "abc": 123,
        "defg": ':string'
      }
  
      // Act
      const result = checkSnapshotObject(actual, snapshot)


      // Assert
      console.log('diff is', (result as SnapshotMismatch<String>).diff)
      assert.equal(result.type, SnapshotResultType.MATCH)

    })
  })
})