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

import { spawn, spawnSync } from 'node:child_process'
import { readFile, writeFile, mkdir, rm } from 'node:fs/promises'
import path from 'node:path'
import { RunTaskFunctional } from '../types'

const PROJECT_ROOT = path.resolve(__dirname, '../../..')

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m'
}

interface FunctionalTestConfig {
  centralLedgerVersion: string
  testHarnessVersion: string
  testHarnessGit: string
  testHarnessDir: string
  skipShutdown: boolean
  quiet: boolean
}

/**
 * Build configuration from task options and environment variables
 */
function buildConfig(task: RunTaskFunctional): FunctionalTestConfig {
  return {
    centralLedgerVersion: process.env.CENTRAL_LEDGER_VERSION || 'local',
    testHarnessVersion: process.env.ML_CORE_TEST_HARNESS_VERSION || 'v2.12.0',
    testHarnessGit: process.env.ML_CORE_TEST_HARNESS_GIT || 'https://github.com/mojaloop/ml-core-test-harness.git',
    testHarnessDir: process.env.ML_CORE_TEST_HARNESS_DIR || '/tmp/ml-core-test-harness',
    skipShutdown: process.env.ML_CORE_TEST_SKIP_SHUTDOWN === 'true',
    quiet: task.quiet
  }
}

/**
 * Run a shell command and return the result
 */
function runCommand(command: string, args: string[], options: { cwd?: string, quiet?: boolean } = {}): Promise<number> {
  return new Promise((resolve, reject) => {
    const stdio = options.quiet ? 'ignore' : 'inherit'
    const proc = spawn(command, args, {
      cwd: options.cwd || PROJECT_ROOT,
      stdio,
      shell: true
    })

    proc.on('close', (code) => {
      resolve(code ?? 1)
    })

    proc.on('error', (err) => {
      reject(err)
    })
  })
}

/**
 * Clone the test harness repository
 */
async function cloneTestHarness(config: FunctionalTestConfig): Promise<void> {
  // Clean up any existing directory first
  try {
    await rm(config.testHarnessDir, { recursive: true, force: true })
  } catch (err) {
    // Ignore errors if directory doesn't exist
  }

  console.log(`${colors.cyan}==>${colors.reset} Cloning ${config.testHarnessGit}:${config.testHarnessVersion} into ${config.testHarnessDir}`)

  const exitCode = await runCommand('git', [
    'clone',
    '--depth', '1',
    '--branch', config.testHarnessVersion,
    config.testHarnessGit,
    config.testHarnessDir
  ], { quiet: config.quiet })

  if (exitCode !== 0) {
    throw new Error(`Failed to clone test harness (exit code: ${exitCode})`)
  }
}

/**
 * Patch docker-compose.yml for optimizations
 */
async function patchDockerCompose(config: FunctionalTestConfig): Promise<void> {
  const composeFile = path.join(config.testHarnessDir, 'docker-compose.yml')

  console.log(`${colors.cyan}==>${colors.reset} Patching docker-compose.yml`)

  // Read the file
  let content = await readFile(composeFile, 'utf-8')

  // Replace central-ledger image with the locally built version
  const localImage = `central-ledger:${config.centralLedgerVersion}`
  content = content.replace(
    /^(\s*)(image:\s*)mojaloop\/central-ledger:[^\s]+$/gm,
    `$1$2${localImage}\n$1pull_policy: never`
  )

  console.log(`${colors.cyan}==>${colors.reset} Using local central-ledger image: ${localImage}`)

  // Replace central-ledger specific entrypoints with dist/ (TypeScript build output)
  content = content
    .replace(/(central-ledger\.js.*?)node src\/api\//g, '$1node dist/api/')
    .replace(/(central-ledger\.js.*?)node src\/handlers\//g, '$1node dist/handlers/')

  console.log(`${colors.cyan}==>${colors.reset} Patched central-ledger entrypoints: src/* -> dist/*`)

  // Optimize health check intervals for faster local testing
  content = content
    .replace(/interval:\s*30s/g, 'interval: 10s')
    .replace(/interval:\s*15s/g, 'interval: 8s')
    .replace(/start_period:\s*40s/g, 'start_period: 15s')
    .replace(/start_period:\s*30s/g, 'start_period: 15s')

  console.log(`${colors.cyan}==>${colors.reset} Optimized health check intervals`)

  // Write back
  await writeFile(composeFile, content, 'utf-8')
}

/**
 * Optimize wait4 configuration for faster startup
 */
async function patchWait4Config(config: FunctionalTestConfig): Promise<void> {
  const wait4ConfigFile = path.join(config.testHarnessDir, 'docker/wait4/wait4.config.js')
  console.log(`${colors.cyan}==>${colors.reset} Optimizing wait4 config`)

  const content = await readFile(wait4ConfigFile, 'utf-8')
  const patched = content.replace(/waitMs:\s*10000/g, 'waitMs: 2000')

  await writeFile(wait4ConfigFile, patched, 'utf-8')
}

/**
 * Copy configuration files to test harness
 */
async function copyConfigs(config: FunctionalTestConfig): Promise<void> {
  const srcDir = path.join(PROJECT_ROOT, 'docker/config-modifier/configs')
  const destDir = path.join(config.testHarnessDir, 'docker/config-modifier/configs')
  console.log(`${colors.cyan}==>${colors.reset} Copying configs from ${srcDir} to ${destDir}`)

  const exitCode = await runCommand('cp', ['-rf', `${srcDir}/*`, destDir], { quiet: config.quiet })

  if (exitCode !== 0) {
    throw new Error(`Failed to copy configs (exit code: ${exitCode})`)
  }
}

/**
 * Start docker compose with all required profiles
 */
async function startDockerCompose(config: FunctionalTestConfig): Promise<void> {
  console.log(`${colors.cyan}==>${colors.reset} Starting Docker compose`)

  const exitCode = await runCommand('docker', [
    'compose',
    '--project-name', 'ttk-func',
    '--ansi', 'never',
    '--profile', 'testing-toolkit',
    '--profile', 'fx',
    '--profile', 'ttk-provisioning-fx',
    '--profile', 'ttk-fx-tests',
    'up', '-d'
  ], { cwd: config.testHarnessDir, quiet: config.quiet })

  if (exitCode !== 0) {
    throw new Error(`Failed to start Docker compose (exit code: ${exitCode})`)
  }
}

/**
 * Wait for a container to exit and return its exit code
 */
async function waitForContainer(containerName: string, pollIntervalMs: number = 5000): Promise<number> {
  console.log(`${colors.cyan}==>${colors.reset} Waiting for ${containerName} to complete...`)

  return new Promise((resolve) => {
    const checkStatus = () => {
      const proc = spawnSync('docker', ['inspect', '--format={{.State.Status}}', containerName], {
        encoding: 'utf-8'
      })

      const status = proc.stdout.trim().replace(/'/g, '')

      if (status === 'exited') {
        const exitCodeProc = spawnSync('docker', ['inspect', '--format={{.State.ExitCode}}', containerName], {
          encoding: 'utf-8'
        })
        const exitCode = parseInt(exitCodeProc.stdout.trim(), 10)

        const statusColor = exitCode === 0 ? colors.green : colors.red
        const statusText = exitCode === 0 ? 'PASS' : 'FAIL'
        console.log(`${colors.cyan}==>${colors.reset} Container ${containerName}: ${statusColor}${statusText}${colors.reset}`)

        resolve(exitCode)
        return
      }

      setTimeout(checkStatus, pollIntervalMs)
    }

    checkStatus()
  })
}

/**
 * Parse TTK summary from logs
 */
function parseTtkSummary(logs: string): Record<string, string> | null {
  const summary: Record<string, string> = {}

  const lines = logs.split('\n')
  for (const line of lines) {
    const match = line.match(/│\s*([^│]+?)\s*│\s*([^│]+?)\s*│/)
    if (match && match[1] !== 'SUMMARY') {
      const key = match[1].trim()
      const value = match[2].trim()
      if (key && value) {
        summary[key] = value
      }
    }
  }

  return Object.keys(summary).length > 0 ? summary : null
}

/**
 * Print TTK summary in a nice table format
 */
function printTtkSummary(summary: Record<string, string>): void {
  const keys = Object.keys(summary)
  if (keys.length === 0) return

  const keyWidth = Math.max(...keys.map(k => k.length), 17)
  const valueWidth = Math.max(...Object.values(summary).map(v => v.length), 29)

  const headerWidth = keyWidth + valueWidth + 5
  const summaryText = 'SUMMARY'
  const padding = Math.floor((headerWidth - summaryText.length) / 2)
  const headerTop = `┌${'─'.repeat(headerWidth)}┐`
  const headerLine = `│${' '.repeat(padding)}${colors.bold}${summaryText}${colors.reset}${' '.repeat(headerWidth - padding - summaryText.length)}│`
  const topBorder = `┌${'─'.repeat(keyWidth + 2)}┬${'─'.repeat(valueWidth + 2)}┐`
  const headerSep = `├${'─'.repeat(keyWidth + 2)}┼${'─'.repeat(valueWidth + 2)}┤`
  const bottomBorder = `└${'─'.repeat(keyWidth + 2)}┴${'─'.repeat(valueWidth + 2)}┘`

  console.log(headerTop)
  console.log(headerLine)
  console.log(topBorder.replace('┌', '├').replace('┐', '┤'))

  let first = true
  for (const [key, value] of Object.entries(summary)) {
    if (!first) {
      console.log(headerSep)
    }
    first = false

    let displayValue = value
    if (key === 'Failed assertions' && value !== '0') {
      displayValue = `${colors.red}${value}${colors.reset}`
    } else if (key === 'Passed percentage') {
      const pct = parseFloat(value)
      if (pct === 100) {
        displayValue = `${colors.green}${value}${colors.reset}`
      } else if (pct < 100) {
        displayValue = `${colors.yellow}${value}${colors.reset}`
      }
    }

    console.log(`│ ${key.padEnd(keyWidth)} │ ${displayValue.padEnd(valueWidth + (displayValue.length - value.length))} │`)
  }

  console.log(bottomBorder)
}

/**
 * Collect logs from containers
 */
async function collectLogs(config: FunctionalTestConfig): Promise<void> {
  const reportsDir = path.join(config.testHarnessDir, 'reports')
  console.log(`${colors.cyan}==>${colors.reset} Collecting logs to ${reportsDir}`)

  await mkdir(reportsDir, { recursive: true })

  // Get provisioning logs
  const provProc = spawnSync('docker', ['logs', 'ttk-func-ttk-provisioning-fx-1'], {
    encoding: 'utf-8'
  })
  await writeFile(path.join(reportsDir, 'ttk-provisioning-console.log'), provProc.stdout + provProc.stderr, 'utf-8')

  // Get test logs
  const testProc = spawnSync('docker', ['logs', 'ttk-func-ttk-fx-tests-1'], {
    encoding: 'utf-8'
  })
  const testLogs = testProc.stdout + testProc.stderr
  await writeFile(path.join(reportsDir, 'ttk-tests-console.log'), testLogs, 'utf-8')

  // Parse and print summary
  const summary = parseTtkSummary(testLogs)
  if (summary) {
    console.log('')
    printTtkSummary(summary)
  } else {
    console.log(`\n${colors.bold}=== Test Output ===${colors.reset}\n`)
    console.log(testLogs)
  }
}

/**
 * Shutdown docker compose
 */
async function shutdownDockerCompose(config: FunctionalTestConfig): Promise<void> {
  console.log(`${colors.cyan}==>${colors.reset} Shutting down Docker compose`)

  await runCommand('docker', [
    'compose',
    '--project-name', 'ttk-func',
    '--ansi', 'never',
    '--profile', 'testing-toolkit',
    '--profile', 'fx',
    '--profile', 'ttk-provisioning-fx',
    '--profile', 'ttk-fx-tests',
    'down', '-v'
  ], { cwd: config.testHarnessDir, quiet: config.quiet })
}

/**
 * Cleanup cloned repository
 */
async function cleanup(config: FunctionalTestConfig): Promise<void> {
  console.log(`${colors.cyan}==>${colors.reset} Cleaning up ${config.testHarnessDir}`)

  await rm(config.testHarnessDir, { recursive: true, force: true })
}

/**
 * Main function to run functional tests
 */
export async function runFunctionalTests(task: RunTaskFunctional): Promise<number> {
  const config = buildConfig(task)

  console.log(`\n${colors.bold}--=== Running Functional Test Runner ===--${colors.reset}\n`)
  console.log(`${colors.cyan}==>${colors.reset} Configuration:`)
  console.log(`    Central Ledger Version: ${config.centralLedgerVersion}`)
  console.log(`    Test Harness Version:   ${config.testHarnessVersion}`)
  console.log(`    Test Harness Dir:       ${config.testHarnessDir}`)
  console.log(`    Skip Shutdown:          ${config.skipShutdown}`)
  console.log(`    Quiet:                  ${config.quiet}`)
  console.log('')

  // Register SIGINT handler for graceful teardown
  const sigintHandler = async () => {
    console.log(`\n${colors.yellow}Received SIGINT, tearing down...${colors.reset}`)
    await shutdownDockerCompose(config).catch(() => {})
    await cleanup(config).catch(() => {})
    process.exit(1)
  }
  process.on('SIGINT', sigintHandler)

  let exitCode = 1
  try {
    await cloneTestHarness(config)
    await patchDockerCompose(config)
    await patchWait4Config(config)
    await copyConfigs(config)
    await startDockerCompose(config)

    exitCode = await waitForContainer('ttk-func-ttk-fx-tests-1')

    await collectLogs(config)

    if (!config.skipShutdown) {
      await shutdownDockerCompose(config)
      await cleanup(config)
    } else {
      console.log(`\n${colors.yellow}==>${colors.reset} Skipping shutdown (containers still running)`)
      console.log(`    You can debug with:`)
      console.log(`    docker ps --filter "name=ttk-func"`)
      console.log(`    docker logs ttk-func-ttk-fx-tests-1`)
    }

    const statusColor = exitCode === 0 ? colors.green : colors.red
    const statusText = exitCode === 0 ? 'PASS' : 'FAIL'
    console.log(`\n${colors.cyan}==>${colors.reset} Functional tests: ${statusColor}${statusText}${colors.reset} (exit code: ${exitCode})`)
    return exitCode
  } catch (err: any) {
    console.error(`${colors.red}Error:${colors.reset}`, err.message)

    if (!config.skipShutdown) {
      await shutdownDockerCompose(config).catch(() => {})
      await cleanup(config).catch(() => {})
    }
    return 1
  } finally {
    process.off('SIGINT', sigintHandler)
  }
}
