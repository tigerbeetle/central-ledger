import Metrics from '@mojaloop/central-services-metrics';
import Hapi, { Plugin } from '@hapi/hapi';

const ObjStoreDb = require('@mojaloop/object-store-lib').Db
import MongoUriBuilder from 'mongo-uri-builder';
import ErrorHandler from '@mojaloop/central-services-error-handling';

const Logger = require('../shared/logger').logger
import { ApplicationConfig } from "./config";
import assert from 'assert';
import Cache from '../lib/cache';

import Migrator from '../lib/migrator';
import Db from '../lib/db';
import EnumCached from '../lib/enumCached';
import RegisterHandlers from '../handlers/register';
import ParticipantCached from '../models/participant/participantCached';
import ParticipantCurrencyCached from '../models/participant/participantCurrencyCached';
import ParticipantLimitCached from '../models/participant/participantLimitCached';
import externalParticipantCached from '../models/participant/externalParticipantCached';
import BatchPositionModelCached from '../models/position/batchCached';
import ProxyCache from '../lib/proxyCache';
import Provisioner from './provisioner';
import handlers from 'src/handlers';
import Plugins from './plugins';


export interface Initialized {
  server: undefined | Hapi.Server<Hapi.ServerApplicationState>,
  handlers: undefined | Array<unknown>,
  proxyCache: undefined | unknown,
  mongoClient: undefined | unknown
}

export enum Service {
  api = 'api',
  admin = 'admin',
  handler = 'handler'
}

export enum HandlerType {
  prepare = 'prepare',
  position = 'position',
  positionbatch = 'positionbatch',
  fulfil = 'fulfil',
  timeout = 'timeout',
  admin = 'admin',
  get = 'get',
  bulkprepare = 'bulkprepare',
  bulkfulfil = 'bulkfulfil',
  bulkprocessing = 'bulkprocessing',
  bulkget = 'bulkget',
}

async function initializeMongoDB(config: ApplicationConfig): Promise<unknown> {
  assert.equal(config.MONGODB_DISABLED, false)

  try {
    if (config.MONGODB_DEBUG) {
      Logger.isWarnEnabled && Logger.warn('Enabling debug for Mongoose...')
      // TODO: dependency inject
      ObjStoreDb.Mongoose.set('debug', config.MONGODB_DEBUG) // enable debug
    }
    const connectionString = MongoUriBuilder({
      username: encodeURIComponent(config.MONGODB_USER),
      password: encodeURIComponent(config.MONGODB_PASSWORD),
      host: config.MONGODB_HOST,
      port: config.MONGODB_PORT,
      database: config.MONGODB_DATABASE
    })

    return await ObjStoreDb.connect(connectionString)
  } catch (err) {
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

async function initializeCache(): Promise<void> {
  // TODO: dependency inject!
  await EnumCached.initialize()
  await ParticipantCached.initialize()
  await ParticipantCurrencyCached.initialize()
  await ParticipantLimitCached.initialize()
  await BatchPositionModelCached.initialize()
  // all cached models initialize-methods are SYNC!!
  externalParticipantCached.initialize()
  await Cache.initCache()
}

/**
 * @function Initialize the Hapi server at port with modules
 */
async function initializeServer(port: number, modules: Array<Plugin<any>>): Promise<Hapi.Server<Hapi.ServerApplicationState>> {
  return (async () => {
    const server = await new Hapi.Server({
      port,
      routes: {
        validate: {
          options: ErrorHandler.validateRoutes(),
          failAction: async (request, h, err) => {
            throw ErrorHandler.Factory.reformatFSPIOPError(err, ErrorHandler.Enums.FSPIOPErrorCodes.MALFORMED_SYNTAX)
          }
        }
      }
    })

    await Plugins.registerPlugins(server)
    await server.register(modules)
    await server.start()
    Logger.isInfoEnabled && Logger.info(`Server running at: ${server.info.uri}`)
    return server
  })()
}

/**
 * @function initializeHandlers
 * @description Set up all of the kafka handlers
 */
async function initializeHandlers(handlers: Array<HandlerType>): Promise<unknown> {
  const registeredHandlers = {
    connection: {},
    register: {},
    ext: {},
    start: new Date(),
    info: {},
    handlers
  }

  for (const handlerType of handlers) {
    Logger.isInfoEnabled && Logger.info(`Handler Setup - Registering ${JSON.stringify(handlerType)}!`)
    switch (handlerType) {
      case 'prepare': {
        await RegisterHandlers.transfers.registerPrepareHandler()
        break
      }
      case 'position': {
        await RegisterHandlers.positions.registerPositionHandler()
        break
      }
      case 'positionbatch': {
        await RegisterHandlers.positionsBatch.registerPositionHandler()
        break
      }
      case 'fulfil': {
        await RegisterHandlers.transfers.registerFulfilHandler()
        break
      }
      case 'timeout': {
        await RegisterHandlers.timeouts.registerTimeoutHandler()
        break
      }
      case 'admin': {
        await RegisterHandlers.admin.registerAdminHandlers()
        break
      }
      case 'get': {
        await RegisterHandlers.transfers.registerGetHandler()
        break
      }
      case 'bulkprepare': {
        await RegisterHandlers.bulk.registerBulkPrepareHandler()
        break
      }
      case 'bulkfulfil': {
        await RegisterHandlers.bulk.registerBulkFulfilHandler()
        break
      }
      case 'bulkprocessing': {
        await RegisterHandlers.bulk.registerBulkProcessingHandler()
        break
      }
      case 'bulkget': {
        await RegisterHandlers.bulk.registerBulkGetHandler()
        break
      }
      default: {
        const error = `Handler Setup - ${JSON.stringify(handlerType)} is not a valid handler to register!`
        Logger.isErrorEnabled && Logger.error(error)
        throw new Error(error)
      }
    }
  }

  return registeredHandlers
}

export async function initialize({
  config,
  service,
  modules,
  handlers
}: { config: ApplicationConfig, service: Service, modules: Array<Plugin<any>>, handlers: Array<HandlerType> }): Promise<Initialized> {
  try {
    if (!config.INSTRUMENTATION_METRICS_DISABLED) {
      Metrics.setup(config.INSTRUMENTATION_METRICS_CONFIG)
    }

    if (config.RUN_MIGRATIONS) {
      // WARNING: need to dependency inject
      await Migrator.migrate()
    }

    // WARNING: need to dependency inject
    await Db.connect(config.DATABASE)
    const dbLoadedTables = Db._tables ? Db._tables.length : -1
    Logger.debug(`DB.connect loaded '${dbLoadedTables}' tables!`)

    let mongoClient
    if (config.MONGODB_DISABLED === false) {
      mongoClient = await initializeMongoDB(config)
    }

    await initializeCache()
    let proxyCache
    if (config.PROXY_CACHE_CONFIG.enabled) {
      proxyCache = await ProxyCache.connect()
    }

    let server
    switch (service) {
      case Service.api:
      case Service.admin: {
        server = await initializeServer(config.PORT, modules)
        break
      }
      case Service.handler: {
        // Special case - when we're running in `handler` mode, we can still run an api
        if (config.HANDLERS_API_DISABLED === false) {
          server = await initializeServer(config.PORT, modules)
        }
        break
      }
      default: {
        Logger.isErrorEnabled && Logger.error(`No valid service type ${service} found!`)
        throw ErrorHandler.Factory.createInternalServerFSPIOPError(`No valid service type ${service} found!`)
      }
    }

    const registeredHandlers = await initializeHandlers(handlers)

    // Provision from scratch on first start, or update provisioning to match static config
    if (config.EXPERIMENTAL.PROVISIONING.enabled) {
      const provisioner = new Provisioner(config.EXPERIMENTAL.PROVISIONING)
      await provisioner.run();
    }

    return {
      server,
      handlers: [registeredHandlers],
      proxyCache,
      mongoClient,
    }
  } catch (err) {
    Logger.isErrorEnabled && Logger.error(`setup.initialize() - error while initializing ${err}`)

    await Db.disconnect()
    // TODO: 
    if (config.PROXY_CACHE_CONFIG?.enabled) {
      await ProxyCache.disconnect()
    }
    process.exit(1)
  }

}