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
import LegacyCompatibleLedger, { LegacyCompatibleLedgerDependencies } from '../domain/ledger-v2/LegacyCompatibleLedger';
import TigerBeetleLedger, { TigerBeetleLedgerDependencies } from '../domain/ledger-v2/TigerBeetleLedger';
import { PersistedMetadataStore } from '../domain/ledger-v2/PersistedMetadataStore';
import { TransferBatcher } from '../domain/ledger-v2/TransferBatcher';
import { createClient } from 'tigerbeetle-node';
import {
  registerAdminHandlerV2,
  registerFulfilHandlerV2,
  registerFusedFulfilHandler,
  registerFusedPrepareHandler,
  registerGetHandlerV2,
  registerPositionHandlerV2,
  registerPrepareHandlerV2,
  registerTimeoutHandlerV2
} from '../handlers-v2/register';
import RegisterHandlers from '../handlers/register';
import Db from '../lib/db';
import EnumCached from '../lib/enumCached';
import Migrator from '../lib/migrator';
import ProxyCache from '../lib/proxyCache';
import { TimeoutScheduler } from '../messaging/jobs/TimeoutScheduler';
import externalParticipantCached from '../models/participant/externalParticipantCached';
import ParticipantCached from '../models/participant/participantCached';
import ParticipantCurrencyCached from '../models/participant/participantCurrencyCached';
import ParticipantLimitCached from '../models/participant/participantLimitCached';
import BatchPositionModelCached from '../models/position/batchCached';
import SettlementModelCached from '../models/settlement/settlementModelCached';

import Plugins from './plugins';
import Provisioner, { ProvisionerDependencies } from './provisioner';
import { getAccountByNameAndCurrency } from 'src/domain/participant';
import { Ledger } from 'src/domain/ledger-v2/Ledger';
import { logger } from './logger';

// Extend Hapi's ServerApplicationState to include our ledger
declare module '@hapi/hapi' {
  interface ServerApplicationState {
    ledger: Ledger;
  }
}


const USE_NEW_HANDLERS = true

export interface Initialized {
  server: undefined | Hapi.Server<Hapi.ServerApplicationState>,
  handlers: undefined | Array<unknown>,
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
  admin: Kafka.Consumer
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
  fusedprepare = 'fusedprepare',
  position = 'position',
  positionbatch = 'positionbatch',
  fulfil = 'fulfil',
  fusedfulfil = 'fusedfulfil',
  timeout = 'timeout',
  admin = 'admin',
  get = 'get',
  bulkprepare = 'bulkprepare',
  bulkfulfil = 'bulkfulfil',
  bulkprocessing = 'bulkprocessing',
  bulkget = 'bulkget',
}

// export as a list for js to use
export const JS_HANDLER_TYPES = Object.values(HandlerType);

export async function initialize({
  config,
  service,
  modules,
  handlerTypes
}: { config: ApplicationConfig, service: Service, modules: Array<Plugin<any>>, handlerTypes: Array<HandlerType> }): Promise<Initialized> {
  logger.debug('setup.initialize()')

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

    // TODO: we need to be able to initialize the message handlers and api separately
    // in a better fashion
    // ledger
    // TODO(LD): pass in Db instead  relying on global here.
    const ledger = initializeLedger(config)

    let server
    switch (service) {
      case Service.api:
      case Service.admin: {
        server = await initializeServer(config.PORT, modules, ledger)
        break
      }
      case Service.handler: {
        // Special case - when we're running in `handler` mode, we can still run an api
        if (config.HANDLERS_API_DISABLED === false) {
          server = await initializeServer(config.PORT, modules, ledger)
        }
        break
      }
      default: {
        Logger.isErrorEnabled && Logger.error(`No valid service type ${service} found!`)
        throw ErrorHandler.Factory.createInternalServerFSPIOPError(`No valid service type ${service} found!`)
      }
    }

    // TODO(LD): type
    let legacyHandlers: undefined | unknown
    let timeoutScheduler: TimeoutScheduler | undefined

    if (config.HANDLERS.DISABLED === false) {
      // Initialize legacy handlers
      legacyHandlers = await initializeHandlers(handlerTypes)
      // Initialize new V2 handlers with dependency injection
      consumers = await createConsumers(config)
      producers = await createProducers(config)

      // TODO: rename handlers here to handlerTypes or something
      if (USE_NEW_HANDLERS) {
        const v2Handlers = await initializeHandlersV2(
          config, handlerTypes, consumers, producers, ledger
        )
        timeoutScheduler = v2Handlers.timeoutScheduler
      }
    } else {
      logger.warn('config.HANDLERS.DISABLED === true, skipping running of handlers')
    }

    // Provision from scratch on first start, or update provisioning to match static config
    if (config.EXPERIMENTAL.PROVISIONING.enabled) {
      const provisionerDependencies: ProvisionerDependencies = {
        ledger,
      }
      const provisioner = new Provisioner(config.EXPERIMENTAL.PROVISIONING, provisionerDependencies)
      await provisioner.run();
    }

    return {
      server,
      handlers: [
        legacyHandlers
      ],
      consumers: consumers,
      producers: producers,
      proxyCache,
      mongoClient,
      timeoutScheduler,
    }
  } catch (err) {
    
    Logger.error(`setup.initialize() - error while initializing ${err}`, { stack: err.stack })

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
      if (consumers.admin) {
        consumers.admin.disconnect()
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

    if (config.PROXY_CACHE_CONFIG?.enabled) {
      await ProxyCache.disconnect()
    }
    process.exit(1)
  }
}

function initializeLedger(config: ApplicationConfig): Ledger {
  // TODO: Configure the ledgers to run side-by-side
  switch (config.EXPERIMENTAL.LEDGER.PRIMARY) {
    case 'SQL': return initializeLegacyCompatibleLedger(config)
    case 'TIGERBEETLE': return initializeTigerBeetleLedger(config)
    default:
      throw new Error(`initializeLedger uknnown ledger type: ${config.EXPERIMENTAL.LEDGER.PRIMARY}`)
  }
}

function initializeTigerBeetleLedger(config: ApplicationConfig): TigerBeetleLedger {
  const client = createClient({
    cluster_id: config.EXPERIMENTAL.TIGERBEETLE.CLUSTER_ID,
    replica_addresses: config.EXPERIMENTAL.TIGERBEETLE.ADDRESS
  })
  const metadataStore = new PersistedMetadataStore(Db.getKnex())
  const transferBatcher = new TransferBatcher(
    client,
    8000,
    25  // batch interval ms - TODO: make configurable
  )

  const tigerBeetleDeps: TigerBeetleLedgerDependencies = {
    config,
    client,
    metadataStore,
    transferBatcher,
    participantService: require('../domain/participant')
  }
  return new TigerBeetleLedger(tigerBeetleDeps)
}

function initializeLegacyCompatibleLedger(config: ApplicationConfig): LegacyCompatibleLedger {
  // Existing business logic modules
  const Validator = require('../handlers/transfers/validator')
  const TransferService = require('../domain/transfer/index')
  const Participant = require('../domain/participant')
  const participantFacade = require('../models/participant/facade')
  const ProxyCache = require('../lib/proxyCache')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const createRemittanceEntity = require('../handlers/transfers/createRemittanceEntity')
  const TransferObjectTransform = require('../domain/transfer/transform')
  const PositionService = require('../domain/position')
  const prepareModule = require('../handlers/transfers/prepare')
  const TimeoutService = require('../domain/timeout')


  const deps: LegacyCompatibleLedgerDependencies = {
    config,
    lifecycle: {
      participantsHandler: require('../api/participants/handler'),
      participantService: require('../domain/participant'),
      participantFacade: require('../models/participant/facade'),
      transferService: require('../domain/transfer'),
      enums: undefined, // Will be initialized separately
      settlementModelDomain: require('../domain/settlement'),
    },
    clearing: {
      // Validation functions (flattened from validator)
      validatePrepare: Validator.validatePrepare,
      validateParticipantByName: Validator.validateParticipantByName,
      validatePositionAccountByNameAndCurrency: Validator.validatePositionAccountByNameAndCurrency,
      validateParticipantTransferId: Validator.validateParticipantTransferId,
      validateFulfilCondition: Validator.validateFulfilCondition,
      validationReasons: Validator.reasons,

      // Transfer service functions (flattened from transferService)
      handlePayeeResponse: TransferService.handlePayeeResponse,
      getTransferById: TransferService.getById,
      getTransferInfoToChangePosition: TransferService.getTransferInfoToChangePosition,
      getTransferFulfilmentDuplicateCheck: TransferService.getTransferFulfilmentDuplicateCheck,
      saveTransferFulfilmentDuplicateCheck: TransferService.saveTransferFulfilmentDuplicateCheck,
      getTransferErrorDuplicateCheck: TransferService.getTransferErrorDuplicateCheck,
      saveTransferErrorDuplicateCheck: TransferService.saveTransferErrorDuplicateCheck,

      // Utility functions (flattened from nested objects)
      transformTransferToFulfil: TransferObjectTransform.toFulfil,
      duplicateCheckComparator: Comparators.duplicateCheckComparator,

      // Existing top-level functions
      checkDuplication: prepareModule.checkDuplication,
      savePreparedRequest: prepareModule.savePreparedRequest,
      calculatePreparePositionsBatch: PositionService.calculatePreparePositionsBatch,
      changeParticipantPosition: PositionService.changeParticipantPosition,
      getAccountByNameAndCurrency: Participant.getAccountByNameAndCurrency,
      getByIDAndCurrency: participantFacade.getByIDAndCurrency,
      timeoutService: TimeoutService,
    }
  }
  return new LegacyCompatibleLedger(deps)
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
  await SettlementModelCached.initialize();
  
  // all cached models initialize-methods are SYNC!!
  externalParticipantCached.initialize()
  await Cache.initCache()
}

/**
 * @function Initialize the Hapi server at port with modules
 */
async function initializeServer(port: number, modules: Array<Plugin<any>>, ledger: Ledger): Promise<Hapi.Server<Hapi.ServerApplicationState>> {
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

    // Pass through ledger in app state
    server.app.ledger = ledger

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
  const { TRANSFER, POSITION, FULFIL, ADMIN } = Enum.Events.Event.Type;
  const { PREPARE, GET } = Enum.Events.Event.Action;

  // Build topic names
  const topicNamePrepare = KafkaUtil.transformGeneralTopicName(TEMPLATE, TRANSFER, PREPARE);
  const topicNamePosition = KafkaUtil.transformGeneralTopicName(TEMPLATE, POSITION, PREPARE);
  const topicNameFulfil = KafkaUtil.transformGeneralTopicName(TEMPLATE, TRANSFER, FULFIL);
  const topicNameGet = KafkaUtil.transformGeneralTopicName(TEMPLATE, TRANSFER, GET);
  const topicNameAdmin = KafkaUtil.transformGeneralTopicName(TEMPLATE, ADMIN, Enum.Events.Event.Action.TRANSFER);

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

  const configAdmin = KafkaUtil.getKafkaConfig(
    config.KAFKA_CONFIG,
    Enum.Kafka.Config.CONSUMER,
    ADMIN.toUpperCase(),
    Enum.Events.Event.Action.TRANSFER.toUpperCase(),
  );
  (configAdmin as any).rdkafkaConf['client.id'] = topicNameAdmin;

  const consumerPrepare = new Kafka.Consumer([topicNamePrepare], configPrepare);
  const consumerPosition = new Kafka.Consumer([topicNamePosition], configPosition);
  const consumerFulfil = new Kafka.Consumer([topicNameFulfil], configFulfil);
  const consumerGet = new Kafka.Consumer([topicNameGet], configGet);
  const consumerAdmin = new Kafka.Consumer([topicNameAdmin], configAdmin);

  // Connect consumers
  await consumerPrepare.connect()
  await consumerPosition.connect()
  await consumerFulfil.connect()
  await consumerGet.connect()
  await consumerAdmin.connect()

  Logger.info('createConsumers() - created and connected');
  return {
    prepare: consumerPrepare,
    position: consumerPosition,
    fulfil: consumerFulfil,
    get: consumerGet,
    admin: consumerAdmin,
  }
}

async function createProducers(config: ApplicationConfig): Promise<Producers> {
  const KafkaUtil = Util.Kafka;

  Logger.isInfoEnabled && Logger.info('createProducers() - Creating shared Kafka producers');

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
  producers: Producers,
  ledger: Ledger
): Promise<{ timeoutScheduler?: TimeoutScheduler }> {
  let timeoutScheduler: TimeoutScheduler | undefined;

  for (const handlerType of handlerTypes) {
    Logger.info(`HandlerV2 Setup - Registering ${handlerType}`)

    switch (handlerType) {
      case HandlerType.prepare: {
        assert(consumers.prepare)
        assert(producers.position)
        assert(producers.notification)
        await registerPrepareHandlerV2(config, consumers.prepare, producers.position, producers.notification)
        break;
      }
      case HandlerType.fusedprepare: {
        assert(consumers.prepare)
        assert(producers.position)
        assert(producers.notification)
        await registerFusedPrepareHandler(config, consumers.prepare, producers.position, producers.notification, ledger)
        break;
      }
      case HandlerType.position: {
        assert(consumers.position)
        assert(producers.notification)
        await registerPositionHandlerV2(config, consumers.position, producers.notification, producers.position)
        break;
      }
      case HandlerType.fulfil: {
        assert(consumers.fulfil)
        assert(producers.position)
        assert(producers.notification)
        await registerFulfilHandlerV2(config, consumers.fulfil, producers.position, producers.notification)
        break;
      }
      case HandlerType.fusedfulfil: {
        assert(consumers.prepare)
        assert(producers.position)
        assert(producers.notification)
        await registerFusedFulfilHandler(config, consumers.fulfil, producers.position, producers.notification, ledger)
        break;
      }
      case HandlerType.timeout: {
        assert(producers.position)
        assert(producers.notification)
        timeoutScheduler = await registerTimeoutHandlerV2(config, producers.notification, producers.position, ledger)
        break;
      }
      case HandlerType.get: {
        assert(consumers.get)
        assert(producers.notification)
        await registerGetHandlerV2(config, consumers.get, producers.notification, ledger)
        break;
      }
      case HandlerType.admin: {
        assert(consumers.admin)
        await registerAdminHandlerV2(config, consumers.admin)
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
        if (!USE_NEW_HANDLERS) {
          await RegisterHandlers.admin.registerAdminHandlers()
        }
        break
      }
      case 'get': {
        if (!USE_NEW_HANDLERS) {
          await RegisterHandlers.transfers.registerGetHandler()
        }
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
      // ignore newer handlers
      case 'fusedprepare':
      case 'fusedfulfil': {
        break;
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