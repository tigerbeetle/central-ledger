import assert from 'node:assert'
import { RunTask, RunTaskUnit, RunTaskCoverage, RunTaskIntegration, TagTask } from './types'
import { runUnitTests, runCoverage, usageUnit } from './commands/unit'
import { runFunctionalTests } from './commands/functional'
import { runIntegrationTests, usageIntegration } from './commands/integration'

/**
 * @file run.ts
 * @description Single entrypoint for running unit tests, coverage checks, integration tests and 
 *   functional tests.
 */
async function main() {
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
    case 'TEST_INTEGRATION': {
      const exitCode = await runIntegrationTests(task)
      process.exit(exitCode)
    }
  }
}

function parseOptions(args: Array<string>, _env: NodeJS.ProcessEnv): RunTask {
  if (args.length === 0 || (args.length === 1 && (args[0] === '--help' || args[0] === '-h'))) {
    console.log(usage)
    process.exit(0)
  }

  assert(args.length > 0, 'expected at least one arg.')
  const taskCommand = args.shift()
  let tag: TagTask

  switch (taskCommand) {
    case 'unit': {
      return {
        tag: 'TEST_UNIT',
        ...parseUnitTestOptions(args)
      }
    }
    case 'coverage': {
      return {
        tag: 'TEST_COVERAGE',
        ...parseCoverageOptions(args)
      }
    }
    case 'integration': {
      return {
        tag: 'TEST_INTEGRATION',
        ...parseIntegrationOptions(args)
      }
    }
    case 'functional': {
      const quiet = args.includes('--quiet')
      return {
        tag: 'TEST_FUNCTIONAL',
        quiet
      }
    }
    default: {
      console.error(`Error: '${taskCommand}' not found.`)
      console.log('\n' + usage)
      process.exit(1)
    }
  }
}

const parseUnitTestOptions = (args: Array<string>): Omit<RunTaskUnit, 'tag'> => {
  if (args.includes('--help') || args.includes('-h')) {
    console.log(usageUnit)
    process.exit(0)
  }

  try {
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
  } catch (err: any) {
    console.error('Error:', err.message)
    console.log('\n' + usageUnit)
    process.exit(1)
  }
}

const parseIntegrationOptions = (args: Array<string>): Omit<RunTaskIntegration, 'tag'> => {
  if (args.includes('--help') || args.includes('-h')) {
    console.log(usageIntegration)
    process.exit(0)
  }

  try {
    let type = 'BOTH' as RunTaskIntegration['type']
    let output = 'DEFAULT' as RunTaskIntegration['output']
    let outputPath = undefined
    let skipDocker = false
    let skipShutdown = false

    args.forEach(arg => {
      const matchType = arg.match(/--type=(.*)$/)
      if (matchType) {
        assert(matchType.length >= 2)
        switch (matchType[1]) {
          case 'standard': type = 'STANDARD'; return
          case 'override': type = 'OVERRIDE'; return
          case 'both': type = 'BOTH'; return
          default: {
            throw new Error(`Invalid --type=${matchType[1]}, expected: standard | override | both`)
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
        return
      }

      if (arg === '--skip-docker') {
        skipDocker = true
        return
      }

      if (arg === '--skip-shutdown') {
        skipShutdown = true
        return
      }

      throw new Error(`unhandled arg: ${arg}`)
    })

    // Validate options.
    if (output === 'XUNIT' && !outputPath) {
      throw new Error('Validation error:\n    Required: `--outputPath` when `--output=xunit`.')
    }

    return {
      type,
      output,
      outputPath,
      skipDocker,
      skipShutdown
    }
  } catch (err: any) {
    console.error('Error:', err.message)
    console.log('\n' + usageIntegration)
    process.exit(1)
  }
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

const usage = `Usage:

./testing/run.ts [unit | coverage | integration | functional]\n\n\
  'unit'          : Run the unit tests.
  'coverage'      : Run the unit tests then check coverage.
  'integration'   : Run the integration tests (requires Docker services).
  'functional'    : Run the functional tests using the ml-core-test-harness.

  Examples:

  # Run the unit tests.
  ./testing/run.ts unit

  # Run the unit tests then check for coverage (will exit != 0 if it fails.)
  ./testing/run.ts coverage

  # Run coverage report only (don't check thresholds)
  ./testing/run.ts coverage --only-report

  # Run the integration tests (starts Docker services automatically)
  ./testing/run.ts integration

  # Run the functional tests
  ./testing/run.ts functional

  # Run the functional tests in quiet mode
  ./testing/run.ts functional --quiet

`

main().catch((error) => {
  console.error(error)
  process.exit(1)
})