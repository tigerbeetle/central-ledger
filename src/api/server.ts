'use strict'

import Config from '../shared/config'
import AdminRoutes from './routes'
import SettlementRoutes from '../settlement/api/routes'
import { HandlerType, initialize, Service } from '../shared/setup-new'
import { plugin as MetricsPlugin } from '@mojaloop/central-services-metrics'
import Migrator from '../lib/migrator'


const server = {
  run: () => {
    return initialize({
      config: Config,
      service: Service.api,
      modules: [
        AdminRoutes, 
        SettlementRoutes,
        !Config.INSTRUMENTATION_METRICS_DISABLED && MetricsPlugin
      ].filter(Boolean),
      handlerTypes: [
        HandlerType.fusedprepare,
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