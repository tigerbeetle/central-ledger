/*****
 License
 --------------
 Copyright © 2020-2024 Mojaloop Foundation
 The Mojaloop files are made available by the Mojaloop Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Mojaloop Foundation for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * TigerBeetle
 - Lewis Daly <lewis@tigerbeetle.com>
 --------------

 ******/

import { describe, it } from 'node:test'
import assert from 'node:assert'
import {
  assertString,
  assertNumber,
  assertBoolean,
  assertStringOrNull,
  assertProxyCacheConfig,
  defaultTo,
  stringToBoolean,
  convertBigIntToNumber,
  safeStringToNumber,
  assertRange,
  assertNestedFields
} from './util'

describe('config/util', () => {

  describe('assertString', () => {
    it('accepts a valid string', () => {
      assert.doesNotThrow(() => assertString('hello'))
      assert.doesNotThrow(() => assertString(''))
    })

    it('throws for non-string values', () => {
      assert.throws(() => assertString(123), /expected 'string', instead found number/)
      assert.throws(() => assertString(null), /expected 'string', instead found object/)
      assert.throws(() => assertString(undefined), /expected 'string', instead found undefined/)
      assert.throws(() => assertString({}), /expected 'string', instead found object/)
      assert.throws(() => assertString([]), /expected 'string', instead found object/)
      assert.throws(() => assertString(true), /expected 'string', instead found boolean/)
    })
  })

  describe('assertNumber', () => {
    it('accepts valid numbers', () => {
      assert.doesNotThrow(() => assertNumber(123))
      assert.doesNotThrow(() => assertNumber(0))
      assert.doesNotThrow(() => assertNumber(-42))
      assert.doesNotThrow(() => assertNumber(3.14))
    })

    it('throws for non-number values', () => {
      assert.throws(() => assertNumber('123'), /expected 'number', instead found string/)
      assert.throws(() => assertNumber(null), /expected 'number', instead found object/)
      assert.throws(() => assertNumber(undefined), /expected 'number', instead found undefined/)
      assert.throws(() => assertNumber({}), /expected 'number', instead found object/)
    })

    it('throws for NaN', () => {
      assert.throws(() => assertNumber(NaN), /expected 'number', instead found NaN/)
    })
  })

  describe('assertBoolean', () => {
    it('accepts valid booleans', () => {
      assert.doesNotThrow(() => assertBoolean(true))
      assert.doesNotThrow(() => assertBoolean(false))
    })

    it('throws for non-boolean values', () => {
      assert.throws(() => assertBoolean('true'), /expected 'boolean', instead found string/)
      assert.throws(() => assertBoolean(1), /expected 'boolean', instead found number/)
      assert.throws(() => assertBoolean(0), /expected 'boolean', instead found number/)
      assert.throws(() => assertBoolean(null), /expected 'boolean', instead found object/)
      assert.throws(() => assertBoolean(undefined), /expected 'boolean', instead found undefined/)
    })
  })

  describe('assertStringOrNull', () => {
    it('accepts valid strings', () => {
      assert.doesNotThrow(() => assertStringOrNull('hello'))
      assert.doesNotThrow(() => assertStringOrNull(''))
    })

    it('accepts null', () => {
      assert.doesNotThrow(() => assertStringOrNull(null))
    })

    it('throws for non-string, non-null values', () => {
      assert.throws(() => assertStringOrNull(123), /expected 'string', instead found number/)
      assert.throws(() => assertStringOrNull(undefined), /expected 'string', instead found undefined/)
      assert.throws(() => assertStringOrNull({}), /expected 'string', instead found object/)
    })
  })

  describe('assertProxyCacheConfig', () => {
    it('accepts truthy values', () => {
      assert.doesNotThrow(() => assertProxyCacheConfig({ enabled: true }))
      assert.doesNotThrow(() => assertProxyCacheConfig('config'))
      assert.doesNotThrow(() => assertProxyCacheConfig(1))
    })

    it('throws for falsy values', () => {
      assert.throws(() => assertProxyCacheConfig(null))
      assert.throws(() => assertProxyCacheConfig(undefined))
      assert.throws(() => assertProxyCacheConfig(0))
      assert.throws(() => assertProxyCacheConfig(''))
    })
  })

  describe('defaultTo', () => {
    it('returns the input if defined', () => {
      assert.strictEqual(defaultTo('value', 'default'), 'value')
      assert.strictEqual(defaultTo(42, 0), 42)
      assert.strictEqual(defaultTo(false, true), false)
    })

    it('returns the default if input is undefined', () => {
      assert.strictEqual(defaultTo(undefined, 'default'), 'default')
      assert.strictEqual(defaultTo(undefined, 42), 42)
      assert.strictEqual(defaultTo(undefined, true), true)
    })

    it('throws if input type does not match default type', () => {
      assert.throws(() => defaultTo('string', 123))
      assert.throws(() => defaultTo(123, 'string'))
    })
  })

  describe('stringToBoolean', () => {
    it('converts "true" to true (case insensitive)', () => {
      assert.strictEqual(stringToBoolean('true'), true)
      assert.strictEqual(stringToBoolean('TRUE'), true)
      assert.strictEqual(stringToBoolean('True'), true)
    })

    it('converts "false" to false (case insensitive)', () => {
      assert.strictEqual(stringToBoolean('false'), false)
      assert.strictEqual(stringToBoolean('FALSE'), false)
      assert.strictEqual(stringToBoolean('False'), false)
    })

    it('throws for invalid input', () => {
      assert.throws(() => stringToBoolean('yes'), /unknown input/)
      assert.throws(() => stringToBoolean('no'), /unknown input/)
      assert.throws(() => stringToBoolean('1'), /unknown input/)
      assert.throws(() => stringToBoolean('0'), /unknown input/)
      assert.throws(() => stringToBoolean(''), /unknown input/)
    })
  })

  describe('convertBigIntToNumber', () => {
    it('converts bigint within safe range', () => {
      assert.strictEqual(convertBigIntToNumber(BigInt(123)), 123)
      assert.strictEqual(convertBigIntToNumber(BigInt(0)), 0)
      assert.strictEqual(convertBigIntToNumber(BigInt(-42)), -42)
      assert.strictEqual(convertBigIntToNumber(BigInt(Number.MAX_SAFE_INTEGER)), Number.MAX_SAFE_INTEGER)
      assert.strictEqual(convertBigIntToNumber(BigInt(Number.MIN_SAFE_INTEGER)), Number.MIN_SAFE_INTEGER)
    })

    it('throws for bigint outside safe range', () => {
      assert.throws(
        () => convertBigIntToNumber(BigInt(Number.MAX_SAFE_INTEGER) + BigInt(1)),
        /input is outside of safe range/
      )
      assert.throws(
        () => convertBigIntToNumber(BigInt(Number.MIN_SAFE_INTEGER) - BigInt(1)),
        /input is outside of safe range/
      )
    })
  })

  describe('safeStringToNumber', () => {
    it('converts valid number strings', () => {
      assert.strictEqual(safeStringToNumber('123'), 123)
      assert.strictEqual(safeStringToNumber('0'), 0)
      assert.strictEqual(safeStringToNumber('-42'), -42)
      assert.strictEqual(safeStringToNumber('3.14'), 3.14)
      assert.strictEqual(safeStringToNumber('  123  '), 123)
    })

    it('throws for invalid number strings', () => {
      assert.throws(() => safeStringToNumber(''), /Invalid number string/)
      assert.throws(() => safeStringToNumber('abc'), /Invalid number string/)
      assert.throws(() => safeStringToNumber('12abc'), /Invalid number string/)
      assert.throws(() => safeStringToNumber('   '), /Invalid number string/)
    })

    it('throws for infinity', () => {
      assert.throws(() => safeStringToNumber('Infinity'), /Number out of range/)
      assert.throws(() => safeStringToNumber('-Infinity'), /Number out of range/)
    })
  })

  describe('assertRange', () => {
    it('accepts numbers within range', () => {
      assert.doesNotThrow(() => assertRange(5, 0, 10))
      assert.doesNotThrow(() => assertRange(0, 0, 10))
      assert.doesNotThrow(() => assertRange(10, 0, 10))
      assert.doesNotThrow(() => assertRange(-5, -10, 0))
    })

    it('throws for numbers outside range', () => {
      assert.throws(() => assertRange(-1, 0, 10), /valid range/)
      assert.throws(() => assertRange(11, 0, 10), /valid range/)
    })

    it('throws for non-numbers', () => {
      assert.throws(() => assertRange('5' as any, 0, 10))
    })

    it('throws for invalid range arguments', () => {
      assert.throws(() => assertRange(5, 10, 0), /expected maxInclusive > minInclusive/)
    })
  })

  describe('assertNestedFields', () => {
    it('passes for existing top-level field', () => {
      const config = { DATABASE: { HOST: 'localhost' } }
      assert.doesNotThrow(() => assertNestedFields(config, 'DATABASE'))
    })

    it('passes for existing nested fields', () => {
      const config = {
        HANDLERS: {
          API: {
            DISABLED: false
          }
        }
      }
      assert.doesNotThrow(() => assertNestedFields(config, 'HANDLERS'))
      assert.doesNotThrow(() => assertNestedFields(config, 'HANDLERS.API'))
      assert.doesNotThrow(() => assertNestedFields(config, 'HANDLERS.API.DISABLED'))
    })

    it('throws for missing top-level field', () => {
      const config = { DATABASE: {} }
      assert.throws(() => assertNestedFields(config, 'MISSING'), /expected `MISSING` to be defined/)
    })

    it('throws for missing nested field', () => {
      const config = { HANDLERS: {} }
      assert.throws(() => assertNestedFields(config, 'HANDLERS.API'), /expected `HANDLERS.API` to be defined/)
    })

    it('throws for deeply missing nested field', () => {
      const config = { HANDLERS: { API: {} } }
      assert.throws(
        () => assertNestedFields(config, 'HANDLERS.API.MISSING'),
        /expected `HANDLERS.API.MISSING` to be defined/
      )
    })
  })
})
