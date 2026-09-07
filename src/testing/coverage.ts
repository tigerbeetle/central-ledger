import { Session } from 'node:inspector/promises'
import path from "node:path";
import assert from "node:assert";
import fs from "node:fs";

import v8toIstanbul from 'v8-to-istanbul';

const libCoverage = require('istanbul-lib-coverage')
const libReport = require('istanbul-lib-report')
const reports = require('istanbul-reports')
const convertSourceMap = require('convert-source-map')

/**
 * An inline code coverage checker which uses v8toIstanbul to produce html code coverage reports.
 */
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

    const filtered = result.filter(entry =>
      this.filePatterns.some(pattern => entry.url.includes(pattern))
    )

    // Get the source maps for any transpiled ts.
    const scriptSources = new Map<string, { source: string; sourceMap?: any }>()
    for (const entry of filtered) {
      if (entry.scriptId) {
        try {
          const scriptSource = await this.session.post('Debugger.getScriptSource', {
            scriptId: entry.scriptId
          })
          const transpiledSource = (scriptSource as any).scriptSource

          // Extract inline source map if present
          const sourceMap = convertSourceMap.fromSource(transpiledSource)

          scriptSources.set(entry.scriptId, {
            source: transpiledSource,
            sourceMap: sourceMap || undefined
          })
        } catch (err) {
          console.warn('Could not get script source for', entry.url, ':', err)
        }
      }
    }

    this.session.disconnect()

    for (const entry of filtered) {
      const pathIn = entry.url.replace('file://', '')

      // Get the actual transpiled source from V8 using scriptId
      // This is critical for TypeScript - V8 has the transpiled JS, not the TS source
      const scriptData = entry.scriptId ? scriptSources.get(entry.scriptId) : undefined
      const transpiledSource = scriptData?.source
      const sourceMapData = scriptData?.sourceMap

      // Pass the transpiled source and source map to v8-to-istanbul
      // This is critical for TypeScript - the byte offsets from V8 are for the
      // transpiled JS, not the original TS source
      const originalSource = fs.readFileSync(pathIn, 'utf-8')
      const sources = (transpiledSource && sourceMapData) ? {
        source: transpiledSource,
        originalSource,
        sourceMap: sourceMapData
      } : transpiledSource ? {
        source: transpiledSource
      } : undefined

      const converter = v8toIstanbul(pathIn, 0, sources)
      await converter.load()
      converter.applyCoverage(entry.functions)

      const istanbul = converter.toIstanbul()
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
        throw new Error(`Coverage check failed.\n${coverageReport}`)
      } else {
        console.log(`Coverage check passed:\n${coverageReport}`)
      }
    }
  }
}