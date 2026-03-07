import path from 'node:path'

export const PROJECT_ROOT = path.resolve(__dirname, '../../..')

export const NYC_BIN = path.join(PROJECT_ROOT, 'node_modules/.bin/nyc')
export const TAPE_BIN = path.join(PROJECT_ROOT, 'node_modules/.bin/tape')
export const TAP_XUNIT_BIN = path.join(PROJECT_ROOT, 'node_modules/.bin/tap-xunit')

export const HEALTH_URL = 'http://localhost:3001/health'
export const HEALTH_RETRIES = 30
export const HEALTH_RETRY_DELAY_MS = 1000

export const colors = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m'
}
