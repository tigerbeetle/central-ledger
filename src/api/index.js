'use strict'

process.env.UV_THREADPOOL_SIZE = 12

import Config from '../shared/config'
import Routes from './routes'
import { initialize } from '../shared/setup'
import { plugin as MetricsPlugin } from '@mojaloop/central-services-metrics'

export default initialize({
  service: 'api',
  port: Config.PORT,
  modules: [Routes, !Config.INSTRUMENTATION_METRICS_DISABLED && MetricsPlugin].filter(Boolean),
  runMigrations: Config.RUN_MIGRATIONS,
  runHandlers: !Config.HANDLERS_DISABLED
})
