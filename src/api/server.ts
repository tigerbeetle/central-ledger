'use strict'

import Config from '../shared/config'
import Routes from './routes'
import { initialize } from '../shared/setup'
import { plugin as MetricsPlugin } from '@mojaloop/central-services-metrics'
import Migrator from '../lib/migrator'


const server = {
  run: () => {
    return initialize({
      service: 'api',
      port: Config.PORT,
      modules: [Routes, !Config.INSTRUMENTATION_METRICS_DISABLED && MetricsPlugin].filter(Boolean),
      runMigrations: Config.RUN_MIGRATIONS,
      runHandlers: !Config.HANDLERS_DISABLED,
      // TODO: specify which handlers to run in config
      // handlers: [
      //   { type: 'prepare' },
      //   { type: 'position' },
      //   { type: 'fulfil' },
      //   { type: 'timeout' },
      //   { type: 'admin' },
      //   { type: 'get' },
      // ]
    });
  },
  // run only the migrations
  migrate: () => {
    console.log('server - migrator.migrate()')
    return Migrator.migrate()
  }
}

export default server