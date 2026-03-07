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
import { spawn, ChildProcess } from 'node:child_process'
import { RunTaskIntegration, ResultUnitTest } from '../types'
import { findFiles, convertToXunit, $, ensurePortFree } from '../util'
import { mergeTapStreams } from '../tap-stream'
import { PROJECT_ROOT, TAPE_BIN, HEALTH_URL, HEALTH_RETRIES, HEALTH_RETRY_DELAY_MS, colors } from '../constants'

export const usageIntegration = `Usage:
  ./testing/run.ts integration [options]

Options:
  --type=<type>          Test type to run.
                         - standard: Run standard integration tests (test/integration/).
                         - override: Run override integration tests with batch handler (test/integration-override/).
                         - both:     Run both standard and override tests (default).

  --output=<format>      Output format.
                         - default: Standard TAP output (default).
                         - xunit:   Generate xunit XML report.

  --outputPath=<path>    Path for xunit output file (required when --output=xunit).

  --skip-docker          Skip starting Docker services (assumes they're already running).

  --skip-shutdown        Skip shutting down Docker services after tests complete.

  --help, -h             Show this help message.

Examples:
  # Run all integration tests (standard + override).
  ./testing/run.ts integration

  # Run only standard integration tests.
  ./testing/run.ts integration --type=standard

  # Run only override integration tests (with batch handler).
  ./testing/run.ts integration --type=override

  # Run tests with Docker already running.
  ./testing/run.ts integration --skip-docker

  # Run tests and leave Docker running for debugging.
  ./testing/run.ts integration --skip-shutdown

  # Generate xunit XML report.
  ./testing/run.ts integration --output=xunit --outputPath=./test/results/xunit-integration.xml

`

/**
 * Track running service processes for cleanup.
 */
interface ServiceProcesses {
  api?: ChildProcess
  handler?: ChildProcess
}

let serviceProcesses: ServiceProcesses = {}

/**
 * Start the central-ledger API service
 */
async function startApiService(logFile: string, envOverrides: Record<string, string> = {}): Promise<ChildProcess> {
  console.log(`${colors.cyan}==>${colors.reset} Starting central-ledger API service`)

  return new Promise((resolve, reject) => {
    const proc = spawn('npx', ['ts-node', './src/api/index.ts'], {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        LOG_LEVEL: 'error',  // Reduce log verbosity for integration tests
        ...envOverrides
      },
      stdio: ['ignore', 'pipe', 'pipe']
    })

    // Write logs to file
    const logPath = path.join(PROJECT_ROOT, logFile)
    const logStream = require('fs').createWriteStream(logPath, { flags: 'w' })

    if (proc.stdout) {
      proc.stdout.pipe(logStream)
    }
    if (proc.stderr) {
      proc.stderr.pipe(logStream)
    }

    proc.on('error', (err) => {
      reject(err)
    })

    // Give it a moment to start before resolving
    setTimeout(() => {
      if (proc.exitCode !== null) {
        reject(new Error(`Service exited immediately with code ${proc.exitCode}`))
      } else {
        console.log(`${colors.green}✓${colors.reset} API service started (PID: ${proc.pid})`)
        resolve(proc)
      }
    }, 1000)
  })
}

/**
 * Start the batch handler service
 */
async function startBatchHandler(logFile: string, envOverrides: Record<string, string> = {}): Promise<ChildProcess> {
  console.log(`${colors.cyan}==>${colors.reset} Starting batch handler service`)

  return new Promise((resolve, reject) => {
    const proc = spawn('npx', ['ts-node', './src/handlers/index.js', 'handler', '--positionbatch'], {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        LOG_LEVEL: 'error',  // Reduce log verbosity for integration tests
        CLEDG_HANDLERS__API__DISABLED: 'true',
        ...envOverrides
      },
      stdio: ['ignore', 'pipe', 'pipe']
    })

    // Write logs to file
    const logPath = path.join(PROJECT_ROOT, logFile)
    const logStream = require('fs').createWriteStream(logPath, { flags: 'w' })

    if (proc.stdout) {
      proc.stdout.pipe(logStream)
    }
    if (proc.stderr) {
      proc.stderr.pipe(logStream)
    }

    proc.on('error', (err) => {
      reject(err)
    })

    // Give it a moment to start before resolving
    setTimeout(() => {
      if (proc.exitCode !== null) {
        reject(new Error(`Batch handler exited immediately with code ${proc.exitCode}`))
      } else {
        console.log(`${colors.green}✓${colors.reset} Batch handler started (PID: ${proc.pid})`)
        resolve(proc)
      }
    }, 1000)
  })
}

/**
 * Wait for the service to be healthy
 */
async function waitForServiceHealth(): Promise<void> {
  console.log(`${colors.cyan}==>${colors.reset} Waiting for service to be healthy...`)

  for (let i = 0; i < HEALTH_RETRIES; i++) {
    try {
      const response = await fetch(HEALTH_URL)
      if (response.status === 200) {
        console.log(`${colors.green}✓${colors.reset} Service is healthy`)
        return
      } else {
        console.log(`Response: ${response.status}. Retrying...`)
      }
    } catch (err) {
      console.log(`Health check failed. Retrying...`)
    }

    await new Promise(resolve => setTimeout(resolve, HEALTH_RETRY_DELAY_MS))
  }

  throw new Error(`Service failed to become healthy after ${HEALTH_RETRIES} attempts`)
}

/**
 * Stop a service process gracefully, then force kill if needed
 */
async function stopService(proc: ChildProcess, name: string): Promise<void> {
  if (!proc.pid) {
    console.log(`${colors.yellow}Warning: ${name} has no PID, skipping stop${colors.reset}`)
    return
  }

  console.log(`${colors.cyan}==>${colors.reset} Stopping ${name} (PID: ${proc.pid})`)

  try {
    // Try graceful kill
    proc.kill('SIGTERM')

    // Wait a bit for graceful shutdown
    await new Promise(resolve => setTimeout(resolve, 3000))

    // Force kill if still running
    if (proc.exitCode === null) {
      console.log(`${colors.yellow}Forcing kill of ${name}${colors.reset}`)
      proc.kill('SIGKILL')
    }

    console.log(`${colors.green}✓${colors.reset} ${name} stopped`)
  } catch (err: any) {
    console.log(`${colors.yellow}Warning: Error stopping ${name}: ${err.message}${colors.reset}`)
  }

  // Also try to kill anything on port 3001 as fallback
  $('kill -9 $(lsof -t -i:3001) 2>/dev/null || true', { stdio: 'ignore' }).nothrow()
}

/**
 * Stop all running services
 */
async function stopAllServices(): Promise<void> {
  if (serviceProcesses.api) {
    await stopService(serviceProcesses.api, 'API service')
    serviceProcesses.api = undefined
  }

  if (serviceProcesses.handler) {
    await stopService(serviceProcesses.handler, 'Batch handler')
    serviceProcesses.handler = undefined
  }
}

/**
 * Wait for debugging before shutting down (on test failure/error)
 */
async function waitBeforeShutdown(): Promise<void> {
  console.log(`${colors.yellow}⏱${colors.reset}  Tests failed. Waiting 5 minutes before shutting down Docker services...`)
  console.log(`${colors.cyan}==>${colors.reset} This allows you to inspect Docker logs and service state for debugging.`)
  console.log(`${colors.cyan}==>${colors.reset} Press Ctrl+C to skip the wait and shutdown immediately.`)

  const waitDuration = 5 * 60 * 1000 // 5 minutes
  await new Promise(resolve => setTimeout(resolve, waitDuration))
}

/**
 * Run integration tests with Docker services
 */
export async function runIntegrationTests(task: RunTaskIntegration): Promise<number> {
  console.log(`\n${colors.bold}--=== Running Integration Tests ===--${colors.reset}\n`)
  console.log(`${colors.cyan}==>${colors.reset} Configuration:`)
  console.log(`    Type:            ${task.type}`)
  console.log(`    Output:          ${task.output}`)
  console.log(`    Skip Docker:     ${task.skipDocker}`)
  console.log(`    Skip Shutdown:   ${task.skipShutdown}`)
  console.log('')

  let dockerStarted = false

  // Register SIGINT handler for graceful teardown
  const sigintHandler = async () => {
    console.log(`\n${colors.yellow}Received SIGINT, tearing down...${colors.reset}`)
    await stopAllServices()
    if (dockerStarted && !task.skipShutdown) {
      await shutdownDocker()
    }
    process.exit(1)
  }
  process.on('SIGINT', sigintHandler)

  try {
    // Ensure port 3001 is free before starting
    await ensurePortFree(3001)

    // Start Docker services if needed
    if (!task.skipDocker) {
      await startDocker()
      dockerStarted = true

      // Wait for Docker services to be healthy
      console.log(`${colors.cyan}==>${colors.reset} Waiting for Docker services to be healthy...`)
      await waitForDocker()

      // Run database migrations
      console.log(`${colors.cyan}==>${colors.reset} Running database migrations`)
      await runMigrations()
    }

    // Run tests based on type
    let result: ResultUnitTest
    switch (task.type) {
      case 'STANDARD':
        console.log(`${colors.cyan}==>${colors.reset} Running standard integration tests`)
        result = await runIntegrationTestsStandard()
        break
      case 'OVERRIDE':
        console.log(`${colors.cyan}==>${colors.reset} Running override integration tests`)
        result = await runIntegrationTestsOverride()
        break
      case 'BOTH': {
        console.log(`${colors.cyan}==>${colors.reset} Running standard integration tests`)
        const resultStandard = await runIntegrationTestsStandard()
        assert(resultStandard.exitCode !== null, 'Encountered unknown error when running standard tests.')

        // Wait between test runs for Kafka rebalancing
        console.log(`${colors.cyan}==>${colors.reset} Waiting 15s before starting override tests...`)
        await new Promise(resolve => setTimeout(resolve, 15000))

        console.log(`${colors.cyan}==>${colors.reset} Running override integration tests`)
        const resultOverride = await runIntegrationTestsOverride()
        assert(resultOverride.exitCode !== null, 'Encountered unknown error when running override tests.')

        const outputMerged = mergeTapStreams(resultStandard.output, resultOverride.output)
        const exitCodeMerged = [resultStandard, resultOverride].reduce((acc, result) => {
          assert(result.exitCode !== null)
          return acc > 0 ? acc : result.exitCode
        }, 0)

        console.log('==== Merged test results ====')
        const summaryText = outputMerged.split('\n')
          .filter(line => line.match(/^#/))
          .join('\n')
        console.log(summaryText)

        result = {
          output: outputMerged,
          exitCode: exitCodeMerged
        }
        break
      }
    }

    // Output xunit if requested
    if (task.output === 'XUNIT') {
      assert(task.outputPath, 'expected outputPath to be defined for xunit output')
      await convertToXunit(result.output, task.outputPath)
    }

    const statusColor = result.exitCode === 0 ? colors.green : colors.red
    const statusText = result.exitCode === 0 ? 'PASS' : 'FAIL'
    console.log(`\n${colors.cyan}==>${colors.reset} Integration tests: ${statusColor}${statusText}${colors.reset} (exit code: ${result.exitCode})`)

    // If tests failed, wait 5 minutes before shutting down (for debugging)
    if (result.exitCode !== 0 && dockerStarted && !task.skipShutdown) {
      await waitBeforeShutdown()
    }

    // Shutdown Docker services if needed
    if (dockerStarted && !task.skipShutdown) {
      await shutdownDocker()
    }

    process.off('SIGINT', sigintHandler)
    return result.exitCode ?? 1
  } catch (err: any) {
    console.error(`${colors.red}Error:${colors.reset}`, err.message)

    await stopAllServices()

    // On error, also wait before shutting down Docker (for debugging)
    if (dockerStarted && !task.skipShutdown) {
      await waitBeforeShutdown()
      await shutdownDocker()
    }

    process.off('SIGINT', sigintHandler)
    return 1
  }
}

/**
 * Wait for Docker services to be healthy
 */
async function waitForDocker(): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn('node', ['scripts/_wait4_all.js'], {
      cwd: PROJECT_ROOT,
      stdio: ['ignore', 'pipe', 'pipe']
    })

    let stdout = ''
    let stderr = ''

    if (proc.stdout) {
      proc.stdout.on('data', (data: Buffer) => {
        const text = data.toString()
        stdout += text
        // Show progress to user
        if (text.includes('Still waiting')) {
          process.stdout.write('.')
        }
      })
    }

    if (proc.stderr) {
      proc.stderr.on('data', (data: Buffer) => {
        stderr += data.toString()
      })
    }

    proc.on('close', (code) => {
      if (code === 0) {
        console.log(`\n${colors.green}✓${colors.reset} Docker services are healthy`)
        resolve()
      } else {
        console.error(`${colors.red}✗${colors.reset} Docker services failed to become healthy`)
        if (stderr.trim()) {
          console.error(stderr)
        }
        if (stdout.trim()) {
          console.log(stdout)
        }
        reject(new Error(`Docker services failed to become healthy (exit code: ${code})`))
      }
    })

    proc.on('error', (err) => {
      reject(err)
    })
  })
}

/**
 * Run database migrations
 */
async function runMigrations(): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn('npm', ['run', 'migrate'], {
      cwd: PROJECT_ROOT,
      stdio: ['ignore', 'pipe', 'pipe']
    })

    let stdout = ''
    let stderr = ''

    if (proc.stdout) {
      proc.stdout.on('data', (data: Buffer) => {
        stdout += data.toString()
      })
    }

    if (proc.stderr) {
      proc.stderr.on('data', (data: Buffer) => {
        stderr += data.toString()
      })
    }

    proc.on('close', (code) => {
      if (code === 0) {
        console.log(`${colors.green}✓${colors.reset} Database migrations completed`)
        resolve()
      } else {
        console.error(`${colors.red}✗${colors.reset} Database migrations failed with exit code: ${code}`)
        if (stderr.trim()) {
          console.error(stderr)
        }
        if (stdout.trim()) {
          console.log(stdout)
        }
        reject(new Error(`Database migrations failed with exit code: ${code}`))
      }
    })

    proc.on('error', (err) => {
      reject(err)
    })
  })
}

/**
 * Start Docker services for integration tests
 */
async function startDocker(): Promise<void> {
  console.log(`${colors.cyan}==>${colors.reset} Starting Docker services (mysql, kafka, redis-cluster, objstore)`)

  return new Promise((resolve, reject) => {
    const proc = spawn('docker', [
      'compose',
      'up',
      '-d',
      'mysql',
      'kafka',
      'init-kafka',
      'redis-node-0',
      'redis-node-1',
      'redis-node-2',
      'redis-node-3',
      'redis-node-4',
      'redis-node-5',
      'objstore'
    ], {
      cwd: PROJECT_ROOT,
      stdio: ['ignore', 'pipe', 'pipe'] // Suppress verbose Docker output
    })

    let stdout = ''
    let stderr = ''

    if (proc.stdout) {
      proc.stdout.on('data', (data: Buffer) => {
        stdout += data.toString()
      })
    }

    if (proc.stderr) {
      proc.stderr.on('data', (data: Buffer) => {
        stderr += data.toString()
      })
    }

    proc.on('close', (code) => {
      if (code === 0) {
        console.log(`${colors.green}✓${colors.reset} Docker services started`)
        resolve()
      } else {
        console.error(`${colors.red}✗${colors.reset} Docker start failed with exit code: ${code}`)
        if (stderr.trim()) {
          console.error(stderr)
        }
        reject(new Error(`Docker start failed with exit code: ${code}`))
      }
    })

    proc.on('error', (err) => {
      reject(err)
    })
  })
}

/**
 * Shutdown Docker services
 */
async function shutdownDocker(): Promise<void> {
  console.log(`${colors.cyan}==>${colors.reset} Shutting down Docker services`)

  return new Promise((resolve) => {
    const proc = spawn('docker', [
      'compose',
      'down',
      '-v'
    ], {
      cwd: PROJECT_ROOT,
      stdio: ['ignore', 'pipe', 'pipe'] // Suppress verbose Docker output
    })

    let errorOutput = ''

    // Capture stderr in case of real errors
    if (proc.stderr) {
      proc.stderr.on('data', (data: Buffer) => {
        errorOutput += data.toString()
      })
    }

    proc.on('close', (code) => {
      // Only show errors if Docker shutdown actually failed
      if (code !== 0 && errorOutput.trim()) {
        console.error(`${colors.yellow}Warning: Docker shutdown had issues:${colors.reset}`)
        console.error(errorOutput)
      }
      resolve()
    })

    proc.on('error', () => {
      // Ignore errors during shutdown
      resolve()
    })
  })
}

/**
 * Run standard integration tests (test/integration/)
 */
async function runIntegrationTestsStandard(): Promise<ResultUnitTest> {
  try {
    // Create test results directory if it doesn't exist
    const testResultsDir = path.join(PROJECT_ROOT, 'test/results')
    require('fs').mkdirSync(testResultsDir, { recursive: true })

    // Start the API service
    const apiProc = await startApiService('./test/results/cl-service.log')
    serviceProcesses.api = apiProc

    // Wait for service to be healthy
    await waitForServiceHealth()

    // Wait for Kafka rebalancing
    console.log(`${colors.cyan}==>${colors.reset} Waiting 15s for Kafka rebalancing...`)
    await new Promise(resolve => setTimeout(resolve, 15000))

    // Run the tests
    const result = await runTapeTests('test/integration')

    // Stop the service
    await stopAllServices()

    return result
  } catch (err: any) {
    console.error(`${colors.red}Error in runIntegrationTestsStandard:${colors.reset}`, err.message)
    await stopAllServices()
    return { output: '', exitCode: 1 }
  }
}

/**
 * Helper function to run tape tests from a directory
 */
async function runTapeTests(testDir: string): Promise<ResultUnitTest> {
  return new Promise((resolve) => {
    const testFiles = findFiles(
      path.join(PROJECT_ROOT, testDir),
      '**/*.test.js'
    ).map(file => path.join(PROJECT_ROOT, testDir, file))

    if (testFiles.length === 0) {
      console.warn(`runTapeTests(${testDir}) - no test files found.`)
      resolve({ output: '', exitCode: 0 })
      return
    }

    const proc = spawn(process.execPath, [
      TAPE_BIN,
      '-r', 'ts-node/register',
      ...testFiles
    ], {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        LOG_LEVEL: 'error',  // Reduce log verbosity for test execution
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
      console.error('Failed to run tape tests:', err.message)
      resolve({ output: '', exitCode: 1 })
    })
  })
}

/**
 * Run override integration tests (test/integration-override/)
 */
async function runIntegrationTestsOverride(): Promise<ResultUnitTest> {
  try {
    // Create test results directory if it doesn't exist
    const testResultsDir = path.join(PROJECT_ROOT, 'test/results')
    require('fs').mkdirSync(testResultsDir, { recursive: true })

    // Environment variable overrides for Kafka topic mapping
    const envOverrides = {
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__PREPARE': 'topic-transfer-position-batch',
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__COMMIT': 'topic-transfer-position-batch',
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__RESERVE': 'topic-transfer-position-batch',
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__TIMEOUT_RESERVED': 'topic-transfer-position-batch',
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__FX_TIMEOUT_RESERVED': 'topic-transfer-position-batch',
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__ABORT': 'topic-transfer-position-batch',
      'CLEDG_KAFKA__EVENT_TYPE_ACTION_TOPIC_MAP__POSITION__FX_ABORT': 'topic-transfer-position-batch'
    }

    // Start the API service with environment overrides
    const apiProc = await startApiService('./test/results/cl-service-override.log', envOverrides)
    serviceProcesses.api = apiProc

    // Start the batch handler with the same environment overrides
    const handlerProc = await startBatchHandler('./test/results/cl-batch-handler.log', envOverrides)
    serviceProcesses.handler = handlerProc

    // Wait for service to be healthy
    await waitForServiceHealth()

    // Wait for Kafka rebalancing
    console.log(`${colors.cyan}==>${colors.reset} Waiting 15s for Kafka rebalancing...`)
    await new Promise(resolve => setTimeout(resolve, 15000))

    // Run the tests
    const result = await runTapeTests('test/integration-override')

    // Stop all services
    await stopAllServices()

    return result
  } catch (err: any) {
    console.error(`${colors.red}Error in runIntegrationTestsOverride:${colors.reset}`, err.message)
    await stopAllServices()
    return { output: '', exitCode: 1 }
  }
}
