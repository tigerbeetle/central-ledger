import assert from "assert"

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

  console.log(`snapshot check failed:\n${result.diff}`)
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
    const left = actualLines[lineIdx]
    const right = snapshotLines[lineIdx]
    if (left.length > maxColumnLengthLeft) {
      maxColumnLengthLeft = left.length
    }

    if ((!left && right) || (right && !left)) {
      mismatchedLines.push(lineIdx)
      continue
    }

    const skipTokenIdx = right.indexOf(':ignore')
    if (skipTokenIdx === -1) {
      if (left !== right) {
        mismatchedLines.push(lineIdx)
        continue;
      }
    }
    // if we found a skip token, then only match the line up to the token
    if (left.length < skipTokenIdx) {
      mismatchedLines.push(lineIdx)
      continue;
    }

    const leftTruncated = left.substring(0, skipTokenIdx)
    const rightTruncated = right.substring(0, skipTokenIdx)
    assert(leftTruncated.length === rightTruncated.length)
    if (leftTruncated !== rightTruncated) {
      mismatchedLines.push(lineIdx)
      continue;
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