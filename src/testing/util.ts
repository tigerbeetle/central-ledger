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
import { spawn as nodeSpawn, execSync, type ExecSyncOptions, type SpawnOptions } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { PROJECT_ROOT, TAP_XUNIT_BIN, colors } from './constants'

export interface ShellResult {
  success: boolean
  stdout: string
  stderr: string
  exitCode: number | null
}

export interface SpawnResult {
  exitCode: number
}

export interface SpawnCommandOptions {
  cwd?: string
  quiet?: boolean
  env?: NodeJS.ProcessEnv
}

/**
 * @function $
 * @description Shell command utility similar to Bun's $.
 *
 * @example
 * ```typescript
 * // Execute synchronously without throwing
 * const result = $('lsof -ti:3001').nothrow()
 * if (result.success) {
 *   console.log('PIDs:', result.stdout)
 * }
 *
 * // Spawn long-running command with streaming output
 * const exitCode = await $('docker', ['compose', 'up', '-d']).spawn()
 * if (exitCode !== 0) {
 *   throw new Error('Docker failed')
 * }
 * ```
 */
export function $(command: string, args?: string[] | ExecSyncOptions, options?: ExecSyncOptions | SpawnCommandOptions) {
  // Handle overloads: $(cmd, execOptions) or $(cmd, args, spawnOptions)
  const isArgsArray = Array.isArray(args)
  const execOptions = isArgsArray ? (options as ExecSyncOptions) : (args as ExecSyncOptions)
  const spawnArgs = isArgsArray ? args : []
  const spawnOptions = isArgsArray ? (options as SpawnCommandOptions) : {}

  return {
    /**
     * Execute command synchronously without throwing errors.
     */
    nothrow(): ShellResult {
      try {
        const stdout = execSync(command, {
          encoding: 'utf8',
          ...execOptions
        })
        return {
          success: true,
          stdout: stdout?.toString() || '',
          stderr: '',
          exitCode: 0
        }
      } catch (err: any) {
        return {
          success: false,
          stdout: err.stdout?.toString() || '',
          stderr: err.stderr?.toString() || '',
          exitCode: err.status ?? null
        }
      }
    },

    /**
     * Spawn command asynchronously with streaming output.
     * Returns a Promise that resolves with the exit code.
     */
    spawn(): Promise<number> {
      return new Promise((resolve, reject) => {
        const stdio = spawnOptions.quiet ? 'ignore' : 'inherit'
        const proc = nodeSpawn(command, spawnArgs, {
          cwd: spawnOptions.cwd || PROJECT_ROOT,
          stdio,
          shell: true,
          env: spawnOptions.env || process.env
        })

        proc.on('close', (code) => {
          resolve(code ?? 1)
        })

        proc.on('error', (err) => {
          reject(err)
        })
      })
    }
  }
}

/**
 * @function ensurePortFree
 * @description Check if a port is in use and kill any process using it.
 *
 * @example
 * ```typescript
 * // Ensure port 3001 is free before starting a service
 * await ensurePortFree(3001)
 * ```
 */
export async function ensurePortFree(port: number): Promise<void> {
  // Check if anything is using the port.
  const result = $(`lsof -ti:${port}`, { stdio: ['pipe', 'pipe', 'ignore'] }).nothrow()

  // lsof returns exit code 1 if no process found - this is fine.
  if (!result.success && result.exitCode === 1) {
    // Port is free.
    return
  }

  // If we got an unexpected error, throw it.
  if (!result.success) {
    throw new Error(`Failed to check port ${port}: ${result.stderr}`)
  }

  const pids: Array<string> = result.stdout.trim().split('\n').filter(Boolean)

  if (pids.length > 0) {
    console.log(`${colors.yellow}Warning: Port ${port} is in use by PID(s): ${pids.join(', ')}${colors.reset}`)
    console.log(`${colors.cyan}==>${colors.reset} Killing process(es) on port ${port}...`)

    pids.forEach((pid: string) => {
      $(`kill -9 ${pid}`, { stdio: 'ignore' }).nothrow()
    })

    // Wait a moment for the port to be released.
    await new Promise(resolve => setTimeout(resolve, 1000))
    console.log(`${colors.green}✓${colors.reset} Port ${port} is now free`)
  }
}

/**
 * @function enumeratePaths
 * @description Iterate through a nested object and return the paths as a list of `|` delimited path
 *   strings.
 * @example
 *  
 * enumeratePaths({a:{b:{c: 123}}}) => ['a', 'a|b', 'a|b|c']
 */
export function enumeratePaths(input: any): Array<string> {
  const paths: Array<string> = []
  const _enumerateNode = (input: any, path: string) => {
    if (input === null || input === undefined) {
      paths.push(path.replace(/\|$/, ''))
      return
    }
    if (typeof input === 'string'
      || typeof input === 'number'
      || typeof input === 'boolean'
      || typeof input === 'bigint'
    ) {
      paths.push(path.replace(/\|$/, ''))
      return
    }

    assert(typeof input === 'object')

    for (const leaf of Object.keys(input)) {
      const node = input[leaf]
      paths.push(path.replace(/\|$/, ''))
      _enumerateNode(node, `${path}${leaf}|`)
    }
    return []
  }

  _enumerateNode(input, '')

  // Deduplicate the intermediate paths.
  return Object.keys(paths.reduce((acc: Record<string, true>, curr) => {
    if (curr === '') return acc
    acc[curr] = true
    return acc
  }, {}))
}

/**
 * @function deleteAtPath
 * @description Delete an element from a complex object. Replaces the object in place.
 * @param path: `|` delimited path string
 */
export function deleteAtPath(input: any, path: string): void {
  const pathComponents = path.split('|')
  assert(pathComponents.length > 0)
  for (let pathComponent of pathComponents) {
    if (pathComponent === pathComponents.at(-1)) {
      delete input[pathComponent]
      return
    }
    input = input[pathComponent]
  }
}

/**
 * @function replaceAtPath
 * @description Replace an element with a new value from a complex object. Replaces the object in 
 *  place.
 * @param path: `|` delimited path string
 */
export function replaceAtPath(input: any, path: string, newValue: any): void {
  const pathComponents = path.split('|')
  assert(pathComponents.length > 0)
  for (let pathComponent of pathComponents) {
    if (pathComponent === pathComponents.at(-1)) {
      input[pathComponent] = newValue
      return
    }
    input = input[pathComponent]
  }
}

/**
 * @function findFiles
 * @description Find all files matching a glob pattern.
 */
export function findFiles(baseDir: string, pattern: string): string[] {
  const results: string[] = []

  // Convert glob pattern to regex.
  const regexPattern = pattern
    .replaceAll('.', String.raw`\.`)
    .replaceAll('**', '{{DOUBLESTAR}}')
    .replaceAll('*', String.raw`[^/]*`)
    .replaceAll('{{DOUBLESTAR}}', '.*')

  const regex = new RegExp(`^${regexPattern}$`)

  function walkDir(dir: string, relativePath: string = '') {
    try {
      const entries = fs.readdirSync(dir, { withFileTypes: true })

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name)
        const relPath = relativePath ? `${relativePath}/${entry.name}` : entry.name

        if (entry.isDirectory()) {
          if (entry.name !== 'node_modules' && entry.name !== '.git') {
            walkDir(fullPath, relPath)
          }
        } else if (entry.isFile()) {
          if (regex.test(relPath)) {
            results.push(relPath)
          }
        }
      }
    } catch (err: any) {
      // Ignore permission errors.
      console.error('findFiles() - ignoring err', err.message)
    }
  }

  walkDir(baseDir)
  return results
}

/**
 * @function convertToXunit
 * @description Convert TAP output to xunit XML format.
 */
export async function convertToXunit(output: string, outputFile: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn(TAP_XUNIT_BIN, [], {
      cwd: PROJECT_ROOT,
      stdio: ['pipe', 'pipe', 'pipe']
    })

    let xml = ''
    let stderr = ''

    proc.stdout.on('data', (data: Buffer) => {
      xml += data.toString()
    })

    proc.stderr.on('data', (data: Buffer) => {
      stderr += data.toString()
    })

    proc.on('close', (code) => {
      if (code !== 0 || !xml) {
        console.warn('Warning: Could not generate xunit report:', stderr)
        reject(stderr)
        return
      }
      // Ensure directory exists.
      const dir = path.dirname(outputFile)
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true })
      }
      fs.writeFileSync(outputFile, xml)
      console.log(`\nXUnit report written to: ${outputFile}`)
      resolve()
    })

    proc.on('error', () => {
      console.warn('Warning: tap-xunit not available')
      resolve()
    })

    proc.stdin.write(output)
    proc.stdin.end()
  })
}