
import assert from 'node:assert'
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { PROJECT_ROOT } from './run'

/**
 * @function enumeratePaths
 * @description Iterate through a nested object and return the paths as a list of path strings.
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

  // deduplicate the intermediate paths
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
 * Find all files matching a glob pattern
 * Simple implementation that handles basic patterns
 * @param baseDir - Base directory to search in
 * @param pattern - Glob pattern (supports ** and *)
 * @returns Array of matching file paths
 */
export function findFiles(baseDir: string, pattern: string): string[] {
  const results: string[] = []

  // Convert glob pattern to regex
  // Important: escape dots BEFORE converting wildcards
  const regexPattern = pattern
    .replace(/\./g, '\\.')           // Escape dots first
    .replace(/\*\*/g, '{{DOUBLESTAR}}')
    .replace(/\*/g, '[^/]*')
    .replace(/{{DOUBLESTAR}}/g, '.*')

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
    } catch (err) {
      // Ignore permission errors
    }
  }

  walkDir(baseDir)
  return results
}

/**
 * @function converToXunit
 * @description Convert TAP output to xunit XML format. Requires `tap-xunit`.
 */
export async function convertToXunit(output: string, outputFile: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn('npx', ['tap-xunit'], {
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
      // Ensure directory exists
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