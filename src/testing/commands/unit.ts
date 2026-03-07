/*****
 License
 --------------
 Copyright © 2020-2026 Mojaloop Foundation
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

import assert from 'node:assert'
import path from 'node:path'
import { spawn, spawnSync } from 'node:child_process'
import { ResultUnitTest, RunTaskUnit, RunTaskCoverage } from '../types'
import { findFiles, convertToXunit } from '../util'
import { mergeTapStreams } from '../tap-stream'
import { PROJECT_ROOT, NYC_BIN, TAPE_BIN } from '../constants'

export const usageUnit = `Usage:
  ./testing/run.ts unit [options]

Options:
  --type=<type>          Test type to run.
                         - tape:   Run legacy tape tests only.
                         - native: Run native Node.js tests only.
                         - both:   Run both tape and native tests (default).

  --output=<format>      Output format.
                         - default: Standard TAP output (default).
                         - xunit:   Generate xunit XML report.

  --outputPath=<path>    Path for xunit output file (required when --output=xunit).

  --help, -h             Show this help message.

Examples:
  # Run all unit tests.
  ./testing/run.ts unit

  # Run only tape tests.
  ./testing/run.ts unit --type=tape

  # Run only native Node.js tests.
  ./testing/run.ts unit --type=native

  # Generate xunit XML report.
  ./testing/run.ts unit --output=xunit --outputPath=./test/results/xunit.xml

`

/**
 * @function runUnitTests
 * @description Run the unit tests based on the RunTaskUnit settings.
 */
export async function runUnitTests(task: RunTaskUnit): Promise<ResultUnitTest> {
  let results: ResultUnitTest
  switch (task.type) {
    case 'TAPE':
      console.log('==== Running Legacy (Tape) unit tests ====')
      results = await runUnitTestsTape()
      break
    case 'NATIVE':
      console.log('==== Running New (Native) unit tests ====')
      results = await runUnitTestsNative()
      break
    case 'BOTH': {
      console.log('==== Running Legacy (Tape) unit tests ====')
      const resultsTape = await runUnitTestsTape()
      assert(resultsTape.exitCode !== null, 'Encountered unknown error when runUnitTestsTape().')
      console.log('==== Running New (Native) unit tests ====')
      const resultsNative = await runUnitTestsNative()
      assert(resultsNative.exitCode !== null, 'Encountered unknown error when runUnitTestsNative().')

      const outputMerged = mergeTapStreams(resultsTape.output, resultsNative.output)
      const exitCodeMerged = [resultsTape, resultsNative].reduce((acc, result) => {
        assert(result.exitCode !== null)
        return acc > 0 ? acc : result.exitCode
      }, 0)

      console.log('==== Merged test results ====')
      const summaryText = outputMerged.split('\n')
        .filter(line => line.match(/^#/))
        .join('\n')
      console.log(summaryText)

      results = {
        output: outputMerged,
        exitCode: exitCodeMerged
      }
      break
    }
  }

  if (task.output === 'XUNIT') {
    assert(task.outputPath, 'expected outputPath to be defined')

    // Export to xunit.
    await convertToXunit(results.output, task.outputPath)
  }

  return results
}

/**
 * @function runCoverage
 * @description Run the unit tests while collecting coverage, and produce the coverage report.
 *
 * For BOTH: uses --silent and --no-clean to accumulate coverage, then nyc report.
 * See: https://github.com/istanbuljs/nyc#combining-reports-from-multiple-runs
 */
export async function runCoverage(task: RunTaskCoverage): Promise<void> {
  switch (task.type) {
    case 'TAPE':
      runCoverageTape({ silent: false, noClean: false })
      break
    case 'NATIVE':
      runCoverageNative({ silent: false, noClean: false })
      break
    case 'BOTH':
      // Run both with --silent, second with --no-clean to accumulate.
      runCoverageTape({ silent: true, noClean: false })
      runCoverageNative({ silent: true, noClean: true })
      // Generate combined report.
      spawnSync(NYC_BIN, ['report', '--reporter=lcov', '--reporter=text-summary'], {
        cwd: PROJECT_ROOT,
        stdio: 'inherit'
      })
      break
  }

  // Check coverage thresholds unless --only-report was specified.
  if (!task.onlyReport) {
    const checkResult = spawnSync(NYC_BIN, ['check-coverage'], {
      cwd: PROJECT_ROOT,
      stdio: 'inherit'
    })
    if (checkResult.status !== 0) {
      process.exit(checkResult.status ?? 1)
    }
  }
}

type NycOptions = {
  silent: boolean
  noClean: boolean
}

/**
 * @function runCoverageTape
 * @description Run legacy tape tests under nyc coverage.
 */
function runCoverageTape(opts: NycOptions): void {
  const testFiles = findFiles(
    path.join(PROJECT_ROOT, 'test/unit'),
    '**/*.test.js'
  ).map(file => path.join(PROJECT_ROOT, 'test/unit', file))

  if (testFiles.length === 0) {
    console.warn(`runCoverageTape() - no test files found.`)
    return
  }

  const nycArgs: string[] = []
  if (opts.silent) nycArgs.push('--silent')
  if (opts.noClean) nycArgs.push('--no-clean')
  if (!opts.silent) nycArgs.push('--reporter=lcov', '--reporter=text-summary')

  const args = [...nycArgs, '--', TAPE_BIN, ...testFiles]
  const result = spawnSync(NYC_BIN, args, {
    cwd: PROJECT_ROOT,
    stdio: 'inherit',
    env: {
      ...process.env,
      NODE_OPTIONS: '-r ts-node/register'
    }
  })

  if (result.error) {
    console.error('Failed to run tape tests with coverage:', result.error.message)
    process.exit(1)
  }
}

/**
 * @function runCoverageNative
 * @description Run native Node.js tests under nyc coverage.
 */
function runCoverageNative(opts: NycOptions): void {
  const testFiles = findFiles(
    path.join(PROJECT_ROOT, 'src'),
    '**/*.unit.ts'
  ).map(f => path.join(PROJECT_ROOT, 'src', f))

  if (testFiles.length === 0) {
    console.warn(`runCoverageNative() - no test files found.`)
    return
  }

  const nycArgs: string[] = []
  if (opts.silent) nycArgs.push('--silent')
  if (opts.noClean) nycArgs.push('--no-clean')
  if (!opts.silent) nycArgs.push('--reporter=lcov', '--reporter=text-summary')

  const args = [
    ...nycArgs,
    '--',
    process.execPath,
    '--require', 'ts-node/register',
    '--test',
    '--test-reporter=tap',
    ...testFiles
  ]
  const result = spawnSync(NYC_BIN, args, {
    cwd: PROJECT_ROOT,
    stdio: 'inherit',
    env: process.env
  })

  if (result.error) {
    console.error('Failed to run native tests with coverage:', result.error.message)
    process.exit(1)
  }
}

/**
 * @function runUnitTestsTape
 * @description Run the legacy unit tests written with tape.
 */
async function runUnitTestsTape(): Promise<ResultUnitTest> {
  return new Promise((resolve) => {
    const testFiles = findFiles(
      path.join(PROJECT_ROOT, 'test/unit'),
      '**/*.test.js'
    ).map(file => path.join(PROJECT_ROOT, 'test/unit', file))

    if (testFiles.length === 0) {
      console.warn(`runUnitTestsTape() - no test files found.`)
      resolve({ output: '', exitCode: 0 })
      return
    }

    // Run node directly with tape module to allow debugging.
    const tapeEntry = path.join(PROJECT_ROOT, 'node_modules/tape/bin/tape')
    const proc = spawn(process.execPath, [
      '-r', 'ts-node/register',
      '--inspect',
      tapeEntry,
      ...testFiles
    ], {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        NODE_OPTIONS: '-r ts-node/register'
      },
      stdio: ['inherit', 'pipe', 'pipe']
    })

    let output = ''
    proc.stdout.on('data', (data: Buffer) => {
      const chunk = data.toString()
      output += chunk
      process.stdout.write(chunk)
    })

    proc.stderr.on('data', (data: Buffer) => {
      process.stderr.write(data)
    })

    proc.on('close', (code) => {
      resolve({ output, exitCode: code })
    })

    proc.on('error', (err) => {
      console.error('Failed to run legacy tests:', err.message)
      resolve({ output: '', exitCode: 1 })
    })
  })
}

/**
 * @function runUnitTestsNative
 * @description Run the unit tests written with the native nodejs test suite.
 */
async function runUnitTestsNative(): Promise<ResultUnitTest> {
  return new Promise((resolve) => {
    const testFiles = findFiles(
      path.join(PROJECT_ROOT, 'src'),
      '**/*.unit.ts'
    ).map(f => path.join(PROJECT_ROOT, 'src', f))

    if (testFiles.length === 0) {
      resolve({ output: '', exitCode: 0 })
      return
    }

    const proc = spawn(process.execPath, [
      '--require', 'ts-node/register',
      '--test',
      '--test-reporter=tap',
      ...testFiles
    ], {
      cwd: PROJECT_ROOT,
      env: process.env,
      stdio: ['inherit', 'pipe', 'pipe']
    })

    let output = ''
    proc.stdout.on('data', (data: Buffer) => {
      const chunk = data.toString()
      output += chunk
      process.stdout.write(chunk)
    })

    proc.stderr.on('data', (data: Buffer) => {
      process.stderr.write(data)
    })

    proc.on('close', (code) => {
      resolve({ output, exitCode: code })
    })

    proc.on('error', (err) => {
      console.error('Failed to run native tests:', err.message)
      resolve({ output: '', exitCode: 1 })
    })
  })
}
