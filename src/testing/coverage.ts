import { Session } from 'node:inspector/promises'
import { readFileSync } from 'node:fs'

export default class Coverage {
  private session = new Session()

  constructor(private filePatterns: Array<string>) {}

  async start() {
    this.session.connect()
    await this.session.post('Profiler.enable')
    await this.session.post('Profiler.startPreciseCoverage', {
      callCount: true,
      detailed: true
    })
  }

  async stopAndReport() {
    const { result } = await this.session.post('Profiler.takePreciseCoverage')
    await this.session.post('Profiler.stopPreciseCoverage')
    this.session.disconnect()

    const filtered = result.filter(entry => 
      this.filePatterns.some(pattern => entry.url.includes(pattern))
    )

    for (const entry of filtered) {
      const path = entry.url.replace('file://', '')
      const source = readFileSync(path, 'utf-8')
      const totalBytes = source.length

      let coveredBytes = 0
      const uncoveredRanges: Array<string> = []

      for (const fn of entry.functions) {
        for (const range of fn.ranges) {
          if (range.count > 0) {
            coveredBytes += range.endOffset - range.startOffset
          } else {
            const snippet = source.slice(range.startOffset, range.startOffset + 50)
            uncoveredRanges.push(snippet.split('\n')[0])
          }
        }
      }

      console.log(`--- ${path} ---`)
      console.log(`Coverage: ${Math.round(coveredBytes/totalBytes * 100)}%`)
      // TODO: make this better at picking up functions
      if (uncoveredRanges.length > 0) {
        console.log('Uncovered:')
        uncoveredRanges.slice(0, 10).forEach(r => console.log(`    - ${r.trim()}`))
      }
    }
  }
}