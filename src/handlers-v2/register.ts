import { Util } from '@mojaloop/central-services-shared'
import { Kafka } from '@mojaloop/central-services-stream'
import { ApplicationConfig } from 'src/shared/config'
import { TimeoutScheduler, TimeoutSchedulerConfig } from '../messaging/jobs/TimeoutScheduler'
import { MessageCommitter } from '../messaging/MessageCommitter'
import { NotificationProducer } from '../messaging/producers/NotificationProducer'
import { PositionProducer } from '../messaging/producers/PositionProducer'
import { INotificationProducer, IPositionProducer } from '../messaging/types'
import { logger } from '../shared/logger'
import { AdminHandler, AdminHandlerDependencies } from './AdminHandler'
import { FulfilHandler, FulfilHandlerDependencies } from './FulfilHandler'
import { GetHandler, GetHandlerDependencies } from './GetHandler'
import { PositionHandler, PositionHandlerDependencies } from './PositionHandler'
import { PrepareHandler, PrepareHandlerDependencies } from './PrepareHandler'
import { TimeoutHandler, TimeoutHandlerDependencies } from './TimeoutHandler'
const { createLock } = require('../lib/distLock');

const rethrow = Util.rethrow

export const createAdminHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
) => {
  // Import existing business logic modules
  const TransferService = require('../domain/transfer/index')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const Db = require('../lib/db')

  const dependencies: AdminHandlerDependencies = {
    committer: new MessageCommitter(consumer),
    config,
    transferService: TransferService,
    comparators: Comparators,
    db: Db
  }

  const handler = new AdminHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}

export const registerAdminHandlerV2 = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
): Promise<void> => {
  try {
    logger.debug(`registerAdminHandlerV2 registering`)

    const handleMessage = createAdminHandler(config, consumer)
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerAdminHandlerV2' })
  }
}

export const createPositionHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
  positionProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const TransferService = require('../domain/transfer/index')
  const PositionService = require('../domain/position')
  const participantFacade = require('../models/participant/facade')
  const SettlementModelCached = require('../models/settlement/settlementModelCached')
  const TransferObjectTransform = require('../domain/transfer/transform')

  // Import Kafka utilities for settlement notifications
  const KafkaUtil = Util.Kafka

  const dependencies: PositionHandlerDependencies = {
    notificationProducer: new NotificationProducer(notificationProducer, config),
    committer: new MessageCommitter(consumer),
    config,
    transferService: TransferService,
    positionService: PositionService,
    participantFacade,
    settlementModelCached: SettlementModelCached,
    transferObjectTransform: TransferObjectTransform,
    kafkaUtil: KafkaUtil,
    positionProducer: new PositionProducer(positionProducer, config)
  }

  const handler = new PositionHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}

export const registerPositionHandlerV2 = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
  positionProducer: Kafka.Producer,
): Promise<void> => {
  try {
    logger.debug(`registerPositionHandlerV2 registering`)

    // Initialize settlement model cache (required by position handler)
    const SettlementModelCached = require('../models/settlement/settlementModelCached')
    await SettlementModelCached.initialize()

    // Create the position handler function
    const handleMessage = createPositionHandler(
      config, consumer, notificationProducer, positionProducer
    )
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPositionHandlerV2' })
  }
}

export function createTimeoutHandler(
  config: any,
  notificationProducer: INotificationProducer,
  positionProducer: IPositionProducer
): TimeoutHandler {
  // TODO(LD): inject the timeout service
  const TimeoutService = require('../domain/timeout');

  // Initialize distributed lock if enabled
  let distLock;
  const distLockEnabled = config.HANDLERS_TIMEOUT_DIST_LOCK_ENABLED === true;
  if (distLockEnabled) {
    distLock = createLock(config.HANDLERS_TIMEOUT.DIST_LOCK, logger);
  }

  const deps: TimeoutHandlerDependencies = {
    notificationProducer,
    positionProducer,
    config,
    timeoutService: TimeoutService,
    distLock
  };

  return new TimeoutHandler(deps);
}

export async function registerTimeoutHandlerV2(
  config: any,
  notificationProducer: Kafka.Producer,
  positionProducer: Kafka.Producer
): Promise<TimeoutScheduler> {
  try {
    if (config.HANDLERS_TIMEOUT_DISABLED) {
      logger.info('Timeout handler is disabled');
      throw new Error('Timeout handler is disabled');
    }

    // Create wrapped producers
    const wrappedNotificationProducer = new NotificationProducer(notificationProducer, config);
    const wrappedPositionProducer = new PositionProducer(positionProducer, config);

    // Create timeout handler
    const timeoutHandler = createTimeoutHandler(config, wrappedNotificationProducer, wrappedPositionProducer);

    // Create scheduler config
    const schedulerConfig: TimeoutSchedulerConfig = {
      cronTime: config.HANDLERS_TIMEOUT_TIMEXP,
      timeZone: config.HANDLERS_TIMEOUT_TIMEZONE,
      onTick: async () => {
        try {
          const result = await timeoutHandler.processTimeouts();
          logger.debug('Timeout processing completed', result);
          return result;
        } catch (err) {
          logger.error('Error in timeout processing:', err);
          throw err;
        }
      }
    };

    // Create and start scheduler
    const timeoutScheduler = TimeoutScheduler.createWithSignalHandlers(schedulerConfig);
    await timeoutScheduler.start();

    logger.info('registerTimeoutHandlerV2 completed successfully');
    return timeoutScheduler;
  } catch (err) {
    logger.error('Error in registerTimeoutHandlerV2:', err);
    throw err;
  }
}

export const createPrepareHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProducer: Kafka.Producer,
  notificationProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const Validator = require('../handlers/transfers/validator')
  const TransferService = require('../domain/transfer/index')
  const ProxyCache = require('../lib/proxyCache')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const createRemittanceEntity = require('../handlers/transfers/createRemittanceEntity')
  const TransferObjectTransform = require('../domain/transfer/transform')

  // Import business logic functions from prepare.js
  const prepareModule = require('../handlers/transfers/prepare')

  const dependencies: PrepareHandlerDependencies = {
    positionProducer: new PositionProducer(positionProducer, config),
    notificationProducer: new NotificationProducer(notificationProducer, config),
    committer: new MessageCommitter(consumer),
    config,
    validator: Validator,
    transferService: TransferService,
    proxyCache: ProxyCache,
    comparators: Comparators,
    createRemittanceEntity,
    transferObjectTransform: TransferObjectTransform,
    
    // Business logic functions from prepare.js
    checkDuplication: prepareModule.checkDuplication,
    savePreparedRequest: prepareModule.savePreparedRequest,
    definePositionParticipant: prepareModule.definePositionParticipant
  }

  const handler = new PrepareHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}

export const registerPrepareHandlerV2 = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProduer: Kafka.Producer,
  notificationProducer: Kafka.Producer
): Promise<void> => {
  try {
    logger.debug(`registerPrepareHandlerV2 registering`)

    const handleMessage = createPrepareHandler(
      config, consumer, positionProduer, notificationProducer
    )

    consumer.consume(handleMessage)
  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPrepareHandlerV2' })
  }
}

export const createFulfilHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProducer: Kafka.Producer,
  notificationProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const TransferService = require('../domain/transfer/index')
  const Validator = require('../handlers/transfers/validator')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const FxService = require('../domain/fx')
  const TransferObjectTransform = require('../domain/transfer/transform')
  const ParticipantFacade = require('../models/participant/facade')

  const dependencies: FulfilHandlerDependencies = {
    positionProducer: new PositionProducer(positionProducer, config),
    notificationProducer: new NotificationProducer(notificationProducer, config),
    committer: new MessageCommitter(consumer),
    config,
    transferService: TransferService,
    validator: Validator,
    comparators: Comparators,
    fxService: FxService,
    transferObjectTransform: TransferObjectTransform,
    participantFacade: ParticipantFacade
  }

  const handler = new FulfilHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}


export const registerFulfilHandlerV2 = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProducer: Kafka.Producer,
  notificationProducer: Kafka.Producer,
): Promise<void> => {
  try {
    logger.debug(`registerFulfilHandlerV2 registering`)

    const handleMessage = createFulfilHandler(config, consumer, positionProducer, notificationProducer)
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerFulfilHandlerV2' })
  }
}

export const createGetHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const Validator = require('../handlers/transfers/validator')
  const TransferService = require('../domain/transfer/index')
  const FxTransferModel = require('../models/fxTransfer/fxTransfer')
  const TransferObjectTransform = require('../domain/transfer/transform')

  const dependencies: GetHandlerDependencies = {
    notificationProducer: new NotificationProducer(notificationProducer, config),
    committer: new MessageCommitter(consumer),
    config,
    validator: Validator,
    transferService: TransferService,
    fxTransferModel: FxTransferModel,
    transferObjectTransform: TransferObjectTransform
  }

  const handler = new GetHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}

export const registerGetHandlerV2 = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
): Promise<void> => {
  try {
    logger.debug(`registerGetHandlerV2 registering`)

    const handleMessage = createGetHandler(config, consumer, notificationProducer)
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerGetHandlerV2' })
  }
}
