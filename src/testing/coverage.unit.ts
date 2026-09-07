import { describe, it } from "node:test";
import Coverage from "./coverage";
import path from "node:path";
import assert from "node:assert";

describe('coverage', () => {
  it('checks the coverage for a file', { expectFailure: true }, async (context) => {
    const coverage = new Coverage([
      'src/testing/util.ts',
      'src/testing/kafka.ts'
    ])
    const filename = path.basename(__filename)
    assert(filename)
    const pathBase = `.fuzz_output/${filename}/${context.name.replaceAll(' ', '_')}/coverage`

    await coverage.start()

    // Dynamic import AFTER coverage starts - this ensures V8 sees all functions
    const util = await import("./util")

    // Run some code.
    util.futureDate(100, 'ms', new Date())

    await coverage.stopAndReport(pathBase)
  })
})