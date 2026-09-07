import { Session } from 'node:inspector/promises'
import path from "node:path";
import assert from "node:assert";
import fs from "node:fs";

import v8toIstanbul from 'v8-to-istanbul';
import convertSourceMap from 'convert-source-map';

const libCoverage = require('istanbul-lib-coverage')
const libReport = require('istanbul-lib-report')
const reports = require('istanbul-reports')

export default class Coverage {
  private session = new Session()

  constructor(private filePatterns: Array<string>) { }

  async start() {
    this.session.connect()
    await this.session.post('Profiler.enable')
    await this.session.post('Debugger.enable')
    await this.session.post('Profiler.startPreciseCoverage', {
      callCount: true,
      detailed: true
    })
  }

  async stopAndReport(dir: string) {
    fs.mkdirSync(dir, { recursive: true });
    // const pathA = `${dir}/traceA.txt`

    const { result } = await this.session.post('Profiler.takePreciseCoverage')
    await this.session.post('Profiler.stopPreciseCoverage')
    this.session.disconnect()


    const filtered = result.filter(entry =>
      this.filePatterns.some(pattern => entry.url.includes(pattern))
    )

    for (const entry of filtered) {
      const pathIn = entry.url.replace('file://', '')

      // Debug: Log the raw V8 coverage data
      console.log('=== Raw URL:', entry.url)
      console.log('=== pathIn:', pathIn)
      console.log('Number of functions:', entry.functions.length)
      for (const fn of entry.functions) {
        console.log(`  Function: ${fn.functionName || '(anonymous)'}, ranges: ${fn.ranges.length}`)
        for (const range of fn.ranges) {
          console.log(`    Range: ${range.startOffset}-${range.endOffset}, count=${range.count}`)
        }
      }

      const converter = v8toIstanbul(pathIn)
      await converter.load()
      converter.applyCoverage(entry.functions)

      const istanbul = converter.toIstanbul()
      console.log('=== Istanbul output ===')
      console.log(JSON.stringify(istanbul, null, 2))
      const coverageMap = libCoverage.createCoverageMap(istanbul)
      const context = libReport.createContext({
        dir,
        coverageMap,
      })

      const reportHtml = reports.create('html', {})
      reportHtml.execute(context)
      const reportHtmlPath = path.join(dir, path.basename(pathIn) + '.html')
      console.log(`Open the coverage report at:\n\t${reportHtmlPath}`)


      // Assert thresholds.
      const summary = coverageMap.getCoverageSummary()
      const coverageReport = `${pathIn}\n`
        + `   statements: ${summary.statements.pct}%\n`
        + `   branches:   ${summary.branches.pct}%\n`
        + `   functions:  ${summary.functions.pct}%\n`
        + `   lines:      ${summary.lines.pct}%\n`
      if (
        summary.statements.pct < 100 ||
        summary.branches.pct < 100 ||
        summary.functions.pct < 100 ||
        summary.lines.pct < 100
      ) {
        console.log(`Coverage check failed:\n${coverageReport}`)
        throw new Error(``)
      } else {
        console.log(`Coverage check passed:\n${coverageReport}`)
      }
    }
  }

  async stopAndReportOld(dir: string) {
    fs.mkdirSync(dir, { recursive: true });
    // const pathA = `${dir}/traceA.txt`

    const { result } = await this.session.post('Profiler.takePreciseCoverage')
    await this.session.post('Profiler.stopPreciseCoverage')
    this.session.disconnect()

    const filtered = result.filter(entry =>
      this.filePatterns.some(pattern => entry.url.includes(pattern))
    )

    // const offsetToLineColumn = (lines: Array<string>, offset: number): {line: number, column:number} => {
    //   const lineLength = lines.length
    //   const column = lines[lines.length]
    // }

    // Look up the start of each line.
    const lineStarts = (source: string) => {
      const starts = [0]
      for (let idx = 0; idx < source.length; idx++) {
        if (source[idx] === '\n') {
          starts.push(idx + 1)
        }
      }
      return starts
    }

    const offsetToLine = (starts: Array<number>, offset: number) => {
      let lower = 0
      let upper = starts.length - 1

      // Binary search for offset.
      while (lower < upper) {
        const middle = Math.ceil((lower + upper) / 2)
        if (starts[middle] <= offset) {
          lower = middle
        } else {
          upper = middle - 1
        }
      }
      return lower + 1
    }

    for (const entry of filtered) {
      const pathIn = entry.url.replace('file://', '')
      const source = fs.readFileSync(pathIn, 'utf-8')
      const totalBytes = source.length

      const starts = lineStarts(source)

      let coveredBytes = 0
      const uncoveredRanges: Array<string> = []

      for (const fn of entry.functions) {
        for (const range of fn.ranges) {
          const start = offsetToLine(starts, range.startOffset)
          const end = offsetToLine(starts, range.endOffset)
          console.log(`${fn.functionName || '<anonymous>'}: lines ${start}-${end}, count=${range.count}`)
        }
        // We should convert to a range to lines.
        // for (const range of fn.ranges) {
        //   if (range.count > 0) {
        //     coveredBytes += range.endOffset - range.startOffset
        //   } else {
        //     const snippet = source.slice(range.startOffset, range.startOffset + 50)
        //     uncoveredRanges.push(snippet.split('\n')[0])
        //   }
        // }
      }

      // Append a prefix to each line.
      let modified = ''
      // Each line can be fully covered, partially covered, or not covered
      source.split('\n').forEach(line => {
        modified += '  |  '
        modified += line
        modified += '\n'
      })

      const fileName = path.basename(pathIn) + 'coverage'
      const pathOut = path.join(dir, fileName)
      fs.writeFileSync(pathOut, modified)

      console.log(`Open the coverage report at:\n\t${pathOut}`)

      // console.log(`--- ${path} ---`)
      // console.log(`Coverage: ${Math.round(coveredBytes/totalBytes * 100)}%`)
      // // TODO: make this better at picking up functions
      // if (uncoveredRanges.length > 0) {
      //   console.log('Uncovered:')
      //   uncoveredRanges.slice(0, 10).forEach(r => console.log(`    - ${r.trim()}`))
      // }
    }
  }
}