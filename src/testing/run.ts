import assert from 'node:assert'
import { RunTask, RunTaskUnit, RunTaskCoverage, RunTaskFunctional, TagTask } from './types'
import { runUnitTests, runCoverage } from './commands/unit'
import { runFunctionalTests } from './commands/functional'

/**
 * @file run.ts
 * @description Single entrypoint for running unit tests, coverage checks, integration tests, and functional tests.
 */

async function main() {
  try {
    const task = parseOptions(process.argv.slice(2), process.env)
    switch (task.tag) {
      case 'TEST_UNIT': {
        const result = await runUnitTests(task)
        process.exit(result.exitCode)
      }
      case 'TEST_COVERAGE':
        await runCoverage(task)
        if (task.onlyReport) {
          process.exit(0)
        }
        break
      case 'TEST_FUNCTIONAL': {
        const exitCode = await runFunctionalTests(task)
        process.exit(exitCode)
      }
    }
  } catch (err: any) {
    console.log('Error:', err.message)
    console.log(usage)
  }
}

const parseUnitTestOptions = (args: Array<string>): Omit<RunTaskUnit, 'tag'> => {
  let type = 'BOTH' as RunTaskUnit['type']
  let output = 'DEFAULT' as RunTaskUnit['output']
  let outputPath = undefined
  args.forEach(arg => {
    const matchType = arg.match(/--type=(.*)$/)
    if (matchType) {
      assert(matchType.length >= 2)
      switch (matchType[1]) {
        case 'tape': type = 'TAPE'; return
        case 'native': type = 'NATIVE'; return
        case 'both': type = 'BOTH'; return
        default: {
          throw new Error(`Invalid --type=${matchType[1]}, expected: tape | native | both`)
        }
      }
    }

    const matchOutput = arg.match(/^--output=(.*)$/)
    if (matchOutput) {
      assert(matchOutput.length >= 2)
      switch (matchOutput[1]) {
        case 'default': output = 'DEFAULT'; return
        case 'xunit': output = 'XUNIT'; return
        default: {
          throw new Error(`Invalid --output=${matchOutput[1]}, expected: default | xunit`)
        }
      }
    }

    const matchOutputPath = arg.match(/^--outputPath=(.*)$/)
    if (matchOutputPath) {
      assert(matchOutputPath.length >= 2)
      outputPath = matchOutputPath[1]
      assert(typeof outputPath === 'string')
      return
    }

    throw new Error(`unhandled arg: ${arg}`)
  })

  // Validate options.
  if (output === 'XUNIT' && !outputPath) {
    throw new Error('Validation error:\n    Required: `--outputPath` when `--output=xunit`.')
  }

  return { type, output, outputPath }
}

const parseCoverageOptions = (args: Array<string>): Omit<RunTaskCoverage, 'tag'> => {
  let type = 'BOTH' as RunTaskUnit['type']
  let onlyReport = false
  args.forEach(arg => {
    const matchType = arg.match(/--type=(.*)$/)
    if (matchType) {
      assert(matchType.length >= 2)
      switch (matchType[1]) {
        case 'tape': type = 'TAPE'; return
        case 'native': type = 'NATIVE'; return
        case 'both': type = 'BOTH'; return
        default: {
          throw new Error(`Invalid --type=${matchType[1]}, expected: tape | native | both .`)
        }
      }
    }

    if (arg.match(/^--only-report$/)) {
      onlyReport = true
      return
    }

    throw new Error(`unhandled arg: ${arg}. Supported args for coverage are:\n  --type=[tape|native|both]\n  --only-report .`)
  })

  return {
    type,
    onlyReport
  }
}

function parseOptions(args: Array<string>, _env: NodeJS.ProcessEnv): RunTask {
  assert(args.length > 0, 'expected at least one arg.')
  const taskCommand = args.shift()
  let tag: TagTask

  switch (taskCommand) {
    case 'unit': {
      tag = 'TEST_UNIT'
      const options = parseUnitTestOptions(args)

      return {
        tag,
        ...options,
      }
    }
    case 'coverage': {
      tag = 'TEST_COVERAGE'
      const options = parseCoverageOptions(args)

      return {
        tag,
        ...options
      }
    }
    case 'integration': {
      throw new Error(`'${taskCommand}' not implemented.`)
    }
    case 'functional': {
      tag = 'TEST_FUNCTIONAL'
      const quiet = args.includes('--quiet')

      return {
        tag,
        quiet
      }
    }
    default: {
      throw new Error(`'${taskCommand}' not found.`)
    }
  }
}

const usage = `
Usage:

./testing/run.ts [unit | coverage | integration | functional]\n\n\
  'unit'          : Run the unit tests.
  'coverage'      : Run the unit tests then check coverage.
  'integration'   : *Preview - not yet implemented* Run the integration tests.
  'functional'    : Run the functional tests using the ml-core-test-harness.


  Examples:

  # Run the unit tests.
  ./testing/run.ts unit

  # Run only the the legacy tape tests.
  ./testing/run.ts unit --type=tape

  # Run all the tests, outputting xunit
  ./testing/run.ts unit --output=xunit

  # Run the unit tests then check for coverage (will exit != 0 if it fails.)
  ./testing/run.ts coverage

  # Run coverage report only (don't check thresholds)
  ./testing/run.ts coverage --only-report

  # Run the functional tests
  ./testing/run.ts functional

  # Run the functional tests in quiet mode
  ./testing/run.ts functional --quiet

`

main().catch((error) => {
  console.error(error)
  process.exit(1)
})