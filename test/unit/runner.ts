import { glob } from "glob";
import path from "path";
import assert  from "assert";

/**
 * @file runner.ts
 * @description A simple test runner that allows us to run tests with both .ts and .js, but not have
 *   to compile first. We should extend this if we want to be able to combine running the legacy 
 *   tape tests with native nodejs tests
 */
const testSearchGlob = process.argv.slice(2)[0]
assert(testSearchGlob, 'expected a search path')

// Match all test files
// const testFiles = glob.sync(path.join(process.cwd(), "./test/unit/**/*.test.*"));
const testFiles = glob.sync(path.join(process.cwd(), testSearchGlob));

console.log(`Found ${testFiles.length} test files matching: ${testSearchGlob}`)


// TODO(LD): I've disabled these tests for now. They break the other tests because of the global
// nature of all mocks etc. Before Merging in any PRs, we should first address these test problems
const skippedTests = [
  '/test/unit/lib/cache.test.js',
  '/test/unit/handlers/index.test.js',
  '/test/unit/handlers/positions/handler.test.js',
  '/test/unit/domain/fx/cyril.test.js',
]

console.warn(`Ignoring the following tests since they create dependency chaos:`)
console.warn(`  - ${skippedTests.join('\n  -  ')}`)

const skippedTestsFullPath = skippedTests.map(shortPath => path.join(process.cwd(), shortPath))
const filteredTestFiles: Array<string> = []
testFiles.forEach(file => {
  // console.log('file is', file)
  if (skippedTestsFullPath.indexOf(file) === -1) {
    filteredTestFiles.push(file)
  } 
})

console.log(`Running ${filteredTestFiles.length} after removing skipped tests`)

for (const file of filteredTestFiles) {
  require(path.resolve(file));
}