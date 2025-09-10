'use strict'

import Config from '../shared/config'
import Routes from './routes'
import { HandlerType, initialize, Service } from '../shared/setup-new'
import { plugin as MetricsPlugin } from '@mojaloop/central-services-metrics'
import Migrator from '../lib/migrator'


const server = {
  run: () => {
    return initialize({
      config: Config,
      service: Service.api,
      modules: [Routes, !Config.INSTRUMENTATION_METRICS_DISABLED && MetricsPlugin].filter(Boolean),
      // TODO: specify which handlers to run in config
      handlerTypes: [
        // HandlerType.prepare,
        HandlerType.fusedprepare,
        HandlerType.position,
        // HandlerType.fulfil,
        HandlerType.fusedfulfil,
        HandlerType.timeout,
        HandlerType.admin,
        HandlerType.get,
      ]
    });
  },
  migrate: () => {
    return Migrator.migrate()
  }
}

export default server