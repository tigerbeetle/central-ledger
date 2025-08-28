import Hapi, { Plugin } from '@hapi/hapi';
import ErrorHandler from '@mojaloop/central-services-error-handling';
import Metrics from '@mojaloop/central-services-metrics';
import assert from 'assert';
import MongoUriBuilder from 'mongo-uri-builder';
import Cache from '../lib/cache';
import { ApplicationConfig } from "./config";

const ObjStoreDb = require('@mojaloop/object-store-lib').Db

const Logger = require('../shared/logger').logger

import { Enum, Util } from '@mojaloop/central-services-shared';
import { Kafka } from '@mojaloop/central-services-stream';
import RegisterHandlers from '../handlers/register';
import { registerFulfilHandler_new, registerPrepareHandler_new, registerGetHandler_new } from '../handlers/transfers/register';
import { registerPositionHandler_new } from '../handlers/positions/register';
import { registerTimeoutHandler_new } from '../handlers/timeouts/register';
import { TimeoutScheduler } from '../messaging/jobs/TimeoutScheduler';
import Db from '../lib/db';
import EnumCached from '../lib/enumCached';
import Migrator from '../lib/migrator';
import ProxyCache from '../lib/proxyCache';
import externalParticipantCached from '../models/participant/externalParticipantCached';
import ParticipantCached from '../models/participant/participantCached';
import ParticipantCurrencyCached from '../models/participant/participantCurrencyCached';
import ParticipantLimitCached from '../models/participant/participantLimitCached';
import BatchPositionModelCached from '../models/position/batchCached';
import Plugins from './plugins';
import Provisioner from './provisioner';


const USE_NEW_HANDLERS = true

export interface Initialized {
  server: undefined | Hapi.Server<Hapi.ServerApplicationState>,
  handlers: undefined | Array<unknown>,
  // handlersV2: undefined | HandlerClients,
  proxyCache: undefined | unknown,
  mongoClient: undefined | unknown,
  consumers: undefined | Consumers,
  producers: undefined | Producers,
  timeoutScheduler: undefined | TimeoutScheduler,
}

export interface Consumers {
  prepare: Kafka.Consumer
  position: Kafka.Consumer
  fulfil: Kafka.Consumer
  get: Kafka.Consumer

  // add other consumers here
  // admin
}

export interface Producers {
  notification: Kafka.Producer
  position: Kafka.Producer
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

async function createConsumers(config: ApplicationConfig): Promise<Consumers> {
  const KafkaUtil = Util.Kafka;
  const TEMPLATE = config.KAFKA_CONFIG.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE
  const { TRANSFER, POSITION, FULFIL } = Enum.Events.Event.Type;
  const { PREPARE, GET } = Enum.Events.Event.Action;

  // Build topic names
  const topicNamePrepare = KafkaUtil.transformGeneralTopicName(TEMPLATE, TRANSFER, PREPARE);
  const topicNamePosition = KafkaUtil.transformGeneralTopicName(TEMPLATE, POSITION, PREPARE);
  const topicNameFulfil = KafkaUtil.transformGeneralTopicName(TEMPLATE, TRANSFER, FULFIL);
  const topicNameGet = KafkaUtil.transformGeneralTopicName(TEMPLATE, TRANSFER, GET);

  // Resolve Config
  const configPrepare = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.CONSUMER,
    TRANSFER.toUpperCase(),
    PREPARE.toUpperCase()
  );
  (configPrepare as any).rdkafkaConf['client.id'] = topicNamePrepare;

  const configPosition = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.CONSUMER,
    TRANSFER.toUpperCase(),
    Enum.Events.Event.Action.POSITION.toUpperCase(),
  );
  (configPosition as any).rdkafkaConf['client.id'] = topicNamePosition;
  
  const configFulfil = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.CONSUMER,
    TRANSFER.toUpperCase(),
    FULFIL.toUpperCase(),
  );
  (configFulfil as any).rdkafkaConf['client.id'] = topicNameFulfil;

  const configGet = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.CONSUMER,
    TRANSFER.toUpperCase(),
    GET.toUpperCase(),
  );
  (configGet as any).rdkafkaConf['client.id'] = topicNameGet;

  const consumerPrepare = new Kafka.Consumer([topicNamePrepare], configPrepare);
  const consumerPosition = new Kafka.Consumer([topicNamePosition], configPosition);
  const consumerFulfil = new Kafka.Consumer([topicNameFulfil], configFulfil);
  const consumerGet = new Kafka.Consumer([topicNameGet], configGet);

  // Connect consumers
  await consumerPrepare.connect()
  await consumerPosition.connect()
  await consumerFulfil.connect()
  await consumerGet.connect()

  Logger.info('createConsumers() - created and connected');
  return {
    prepare: consumerPrepare,
    position: consumerPosition,
    fulfil: consumerFulfil,
    get: consumerGet,
  }
}

async function createProducers(config: ApplicationConfig): Promise<Producers> {
  const KafkaUtil = Util.Kafka;

  Logger.isInfoEnabled && Logger.info('Creating shared Kafka producers...');

  // Create position producer
  const positionProducerConfig = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.PRODUCER,
    Enum.Events.Event.Type.TRANSFER.toUpperCase(),
    Enum.Events.Event.Type.POSITION.toUpperCase(),
  );
  const positionProducer = new Kafka.Producer(positionProducerConfig);

  // Create notification producer
  const notificationProducerConfig = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.PRODUCER,
    Enum.Events.Event.Type.NOTIFICATION.toUpperCase(),
    Enum.Events.Event.Action.EVENT.toUpperCase()
  );
  const notificationProducer = new Kafka.Producer(notificationProducerConfig);

  // Connect producers
  await positionProducer.connect();
  await notificationProducer.connect();

  Logger.info('createProducers() - created and connected');
  return {
    position: positionProducer,
    notification: notificationProducer
  }
}

async function initializeHandlersV2(
  config: ApplicationConfig,
  handlerTypes: Array<HandlerType>,
  consumers: Consumers,
  producers: Producers
): Promise<{ timeoutScheduler?: TimeoutScheduler }> {
  let timeoutScheduler: TimeoutScheduler | undefined;

  for (const handlerType of handlerTypes) {
    Logger.info(`HandlerV2 Setup - Registering ${handlerType}`)

    switch (handlerType) {
      case HandlerType.prepare: {
        assert(consumers.prepare)
        assert(producers.position)
        assert(producers.notification)
        await registerPrepareHandler_new(config, consumers.prepare, producers.position, producers.notification)
        break;
      }
      case HandlerType.position: {
        assert(consumers.position)
        assert(producers.notification)
        await registerPositionHandler_new(config, consumers.position, producers.notification)
        break;
      }
      case HandlerType.fulfil: {
        assert(consumers.fulfil)
        assert(producers.position)
        assert(producers.notification)
        await registerFulfilHandler_new(config, consumers.fulfil, producers.position, producers.notification)
        break;
      }
      case HandlerType.timeout: {
        assert(producers.position)
        assert(producers.notification)
        timeoutScheduler = await registerTimeoutHandler_new(config, producers.notification, producers.position)
        break;
      }
      case HandlerType.get: {
        assert(consumers.get)
        assert(producers.notification)
        await registerGetHandler_new(config, consumers.get, producers.notification)
        break;
      }
      case HandlerType.admin: {
        
        break;
      }
      default: {
        Logger.error(`initializeHandlersV2 - unsupported v2 handler: ${handlerType}. Please check your config and restart the service.`)
        throw new Error(`initializeHandlersV2 - unsupported v2 handler: ${handlerType}`)
      }
    }
  }

  return { timeoutScheduler };
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
    Logger.isInfoEnabled && Logger.info(`Handler Setup - Registering ${handlerType}`)
    switch (handlerType) {
      case 'prepare': {
        if (!USE_NEW_HANDLERS) {
          await RegisterHandlers.transfers.registerPrepareHandler()
        }
        break
      }
      case 'position': {
        if (!USE_NEW_HANDLERS) {
          await RegisterHandlers.positions.registerPositionHandler()
        }
        break
      }
      case 'positionbatch': {
        await RegisterHandlers.positionsBatch.registerPositionHandler()
        break
      }
      case 'fulfil': {
        if (!USE_NEW_HANDLERS) {
          await RegisterHandlers.transfers.registerFulfilHandler()
        }
        break
      }
      case 'timeout': {
        if (!USE_NEW_HANDLERS) {
          await RegisterHandlers.timeouts.registerTimeoutHandler()
        }
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
  handlerTypes
}: { config: ApplicationConfig, service: Service, modules: Array<Plugin<any>>, handlerTypes: Array<HandlerType> }): Promise<Initialized> {

  let consumers: Consumers
  let producers: Producers

  try {
    if (!config.INSTRUMENTATION_METRICS_DISABLED) {
      Metrics.setup(config.INSTRUMENTATION_METRICS_CONFIG)
    }

    if (config.RUN_MIGRATIONS) {
      // TODO(LD): inject dependency
      await Migrator.migrate()
    }

    // TODO(LD): inject dependency
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

    // TODO: we need to be able to initialize the message handlers and api separately

    // Initialize legacy handlers
    const legacyHandlers = await initializeHandlers(handlerTypes)

    // Initialize new V2 handlers with dependency injection
    consumers = await createConsumers(config)
    producers = await createProducers(config)
    let timeoutScheduler: TimeoutScheduler | undefined
    // TODO: rename handlers here to handlerTypes or something
    if (USE_NEW_HANDLERS) {
      const v2Handlers = await initializeHandlersV2(config, handlerTypes, consumers, producers)
      timeoutScheduler = v2Handlers.timeoutScheduler
    }

    // Provision from scratch on first start, or update provisioning to match static config
    if (config.EXPERIMENTAL.PROVISIONING.enabled) {
      const provisioner = new Provisioner(config.EXPERIMENTAL.PROVISIONING)
      await provisioner.run();
    }

    return {
      server,
      handlers: [legacyHandlers],
      consumers: consumers,
      producers: producers,
      proxyCache,
      mongoClient,
      timeoutScheduler,
    }
  } catch (err) {
    Logger.isErrorEnabled && Logger.error(`setup.initialize() - error while initializing ${err}`)

    await Db.disconnect()

    // TODO(LD): Improve the cleanup and disconnection of kafka consumers/handlers
    if (consumers) {
      if (consumers.prepare) {
        consumers.prepare.disconnect()
      }
      if (consumers.position) {
        consumers.position.disconnect()
      }
      if (consumers.fulfil) {
        consumers.fulfil.disconnect()
      }
      if (consumers.get) {
        consumers.get.disconnect()
      }
    }

    if (producers) {
      if (producers.position) {
        producers.position.disconnect()
      }

      if (producers.notification) {
        producers.notification.disconnect()
      }
    }

    // timeoutScheduler is scoped to the main try block
    // if (timeoutScheduler) {
    //   await timeoutScheduler.stop()
    // }


    if (config.PROXY_CACHE_CONFIG?.enabled) {
      await ProxyCache.disconnect()
    }
    process.exit(1)
  }

}