import { describe, it } from "node:test";
import Coverage from "./coverage";
import path from "node:path";
import assert from "node:assert";
import  fs from "node:fs";
import { futureDate } from "./util";

describe('coverage', () => {
  it('checks the coverage for a file', async (context) => {
    const coverage = new Coverage([
      'src/testing/util.ts'
    ])
    const filename = path.basename(__filename)
    assert(filename)
    const pathBase = `.fuzz_output/${filename}/${context.name.replaceAll(' ', '_')}/coverage`

    await coverage.start()

    // Run some code.
    futureDate(100, 'ms', new Date())
    futureDate(100, 's', new Date())
    futureDate(100, 'm', new Date())

    await coverage.stopAndReport(pathBase)
  })
})