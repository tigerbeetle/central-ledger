
import assert from 'node:assert'

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