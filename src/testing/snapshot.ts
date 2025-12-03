import assert from "assert"
import { Ignore } from "glob"

const BG_YELLOW = '\x1b[43m'
const BLACK = '\x1b[30m'
const RESET = '\x1b[0m'
const BG_RED = '\x1b[41m'

export enum SnapshotResultType {
  MATCH = 'MATCH',
  MISMATCH = 'MISMATCH',
}

export interface SnapshotMatch<T> {
  type: SnapshotResultType.MATCH
  actual: T
  snapshot: T

}

export interface SnapshotMismatch<T> {
  type: SnapshotResultType.MISMATCH
  actual: T
  snapshot: T
  diff: string
}

export type SnapshotResult<T> = SnapshotMatch<T> | SnapshotMismatch<T>

/**
 * Stringify an object with consistent key ordering for stable comparison
 */
function sortedStringify(obj: any): string {
  return JSON.stringify(obj, (key, value) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      // Sort object keys for consistent ordering
      return Object.keys(value).sort().reduce((sorted: any, key: string) => {
        sorted[key] = value[key];
        return sorted;
      }, {});
    }
    return value;
  }, 2);
}

export function unwrapSnapshot<T>(result: SnapshotResult<T>): void {
  if (result.type === SnapshotResultType.MATCH) {
    return
  }

  console.log(`snapshot mismatch:\n${result.diff}`)
  throw new Error(`snapshot mismatch`)
}

export function checkSnapshotObject(actual: object, snapshot: object): SnapshotResult<object> {
  const actualString = sortedStringify(actual);
  const snapshotString = sortedStringify(snapshot);

  const stringResult = checkSnapshotString(actualString, snapshotString);

  if (stringResult.type === SnapshotResultType.MATCH) {
    return {
      type: SnapshotResultType.MATCH,
      actual,
      snapshot
    };
  }

  return {
    type: SnapshotResultType.MISMATCH,
    actual,
    snapshot,
    diff: stringResult.diff
  };
}

enum SpecialToken {
  IGNORE = 'IGNORE',
  MATCH_STRING = 'MATCH_STRING',
  MATCH_INTEGER = 'MATCH_INTEGER',
  MATCH_DATE = 'MATCH_DATE',
  MATCH_BIGINT = 'MATCH_BIGINT',
  NONE = 'NONE',
}

/**
 * Search the line for special tokens.
 */
const matchSpecialToken = (line: string): { 
  token: SpecialToken,
  index: number 
} => {

  const matchers: Array<[string, SpecialToken]> = [
    ['\":ignore', SpecialToken.IGNORE],
    ['\':ignore', SpecialToken.IGNORE],
    [':ignore', SpecialToken.IGNORE],
    ['\":string', SpecialToken.MATCH_STRING],
    ['\':string', SpecialToken.MATCH_STRING],
    [':string', SpecialToken.MATCH_STRING],
    ['":integer', SpecialToken.MATCH_INTEGER],
    ['\':integer', SpecialToken.MATCH_INTEGER],
    [':integer', SpecialToken.MATCH_INTEGER],
    ['":date', SpecialToken.MATCH_DATE],
    ['\':date', SpecialToken.MATCH_DATE],
    [':date', SpecialToken.MATCH_DATE],
    [':bigint', SpecialToken.MATCH_BIGINT],
  ]

  for (const matcher of matchers) {
    let index = line.indexOf(matcher[0])
    if (index > -1) {
      return {
        token: matcher[1],
        index
      }
    }
  }

  // catch all - no matches
  return {
    token: SpecialToken.NONE,
    index: 0
  }
}

export function checkSnapshotString(actual: string, snapshot: string): SnapshotResult<string> {
  assert(actual)
  assert(typeof actual === 'string')
  assert(snapshot)
  assert(typeof snapshot === 'string')

  const actualLines = actual.split('\n')
  const snapshotLines = snapshot.split('\n')
  const maxLines = Math.max(actualLines.length, snapshotLines.length)
  const mismatchedLines: Array<number> = []
  let maxColumnLengthLeft = 0
  let match = true

  for (let lineIdx = 0; lineIdx < maxLines; lineIdx++) {
    const left = actualLines[lineIdx] || ''
    const right = snapshotLines[lineIdx] || ''
    if (left.length > maxColumnLengthLeft) {
      maxColumnLengthLeft = left.length
    }

    if ((!left && right) || (left && !right)) {
      mismatchedLines.push(lineIdx)
      continue
    }

    const specialToken = matchSpecialToken(right)
    switch (specialToken.token) {
      /**
       * We ignore the rest of the line after the index of the `:ignore`
       */
      case SpecialToken.IGNORE: {
        // if we found a skip token, then only match the line up to the token
        if (left.length < specialToken.index) {
          mismatchedLines.push(lineIdx)
          continue;
        }

        const leftTruncated = left.substring(0, specialToken.index)
        const rightTruncated = right.substring(0, specialToken.index)
        assert(leftTruncated.length === rightTruncated.length)
        if (leftTruncated !== rightTruncated) {
          mismatchedLines.push(lineIdx)
          continue;
        }
        break;
      }
      /**
       * We expect the left side to be a string
       */
      case SpecialToken.MATCH_STRING: {
        if (left.length < specialToken.index) {
          mismatchedLines.push(lineIdx)
          continue;
        }

        // make sure the left side is a string
        const leftCandidate = left.substring(specialToken.index)
          .replace(',', '') // Workaround for trailing commas
        if (!leftCandidate || leftCandidate.length === 0) {
          mismatchedLines.push(lineIdx)
          continue
        }

        // Parse and see what it might be
        try {
          const leftParsed = JSON.parse(leftCandidate)
          if (typeof leftParsed !== 'string') {
            mismatchedLines.push(lineIdx)
          }

        } catch (err) {
          mismatchedLines.push(lineIdx)
        }

        break;
      }
      /**
       * We expect the left side to be a string
       */
      case SpecialToken.MATCH_INTEGER: {
        if (left.length < specialToken.index) {
          mismatchedLines.push(lineIdx)
          continue;
        }

        // make sure the left side is a string somehow?!
        const leftCandidate = left.substring(specialToken.index)
          .replace(',', '') // Strip off trailing commas, a little hacky but it works!
        if (!leftCandidate || leftCandidate.length === 0) {
          mismatchedLines.push(lineIdx)
          continue
        }

        // Parse and see what it might be
        try {
          const leftParsed = JSON.parse(leftCandidate)
          if (typeof leftParsed !== 'number') {
            mismatchedLines.push(lineIdx)
          }

        } catch (err) {
          mismatchedLines.push(lineIdx)
        }

        break;
      }
      case SpecialToken.NONE: {
        if (left !== right) {
          mismatchedLines.push(lineIdx)
          continue;
        }
        break;
      }
      default: {
        throw new Error(`${specialToken.token} not yet implemented!`)
      }
    }
  }

  if (mismatchedLines.length === 0) {
    return {
      type: SnapshotResultType.MATCH,
      actual: actual,
      snapshot: snapshot
    }
  }

  let diff = `${RESET}\n`
  diff += `${'Actual:'.padEnd(maxColumnLengthLeft)} | Snapshot:\n`
  for (let index = 0; index < maxLines; index++) {
    const left = actualLines[index] || ''
    const right = snapshotLines[index] || ''
    if (mismatchedLines.indexOf(index) < 0) {
      diff += `${left.padEnd(maxColumnLengthLeft)} | ${right}\n`
      continue
    }
    diff += `${BG_YELLOW}${left.padEnd(maxColumnLengthLeft)} | ${right}${RESET}\n`
  }


  return {
    type: SnapshotResultType.MISMATCH,
    actual: actual,
    snapshot: snapshot,
    diff
  }
}