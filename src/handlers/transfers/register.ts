import { Enum, Util } from '@mojaloop/central-services-shared'
import { Kafka } from '@mojaloop/central-services-stream'
import { ApplicationConfig } from 'src/shared/config'
import { MessageCommitter } from '../../messaging/MessageCommitter'
import { NotificationProducer } from '../../messaging/producers/NotificationProducer'
import { PositionProducer } from '../../messaging/producers/PositionProducer'
import { logger } from '../../shared/logger'
import { PrepareHandler, PrepareHandlerDependencies } from './PrepareHandler'
import { FulfilHandler, FulfilHandlerDependencies } from './FulfilHandler'

const rethrow = Util.rethrow
const KafkaUtil = Util.Kafka
const TransferEventType = Enum.Events.Event.Type
const TransferEventAction = Enum.Events.Event.Action

export const createPrepareHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProducer: Kafka.Producer,
  notificationProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const Validator = require('./validator')
  const TransferService = require('../../domain/transfer/index')
  const ProxyCache = require('../../lib/proxyCache')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const createRemittanceEntity = require('./createRemittanceEntity')
  const TransferObjectTransform = require('../../domain/transfer/transform')

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
    transferObjectTransform: TransferObjectTransform
  }

  const handler = new PrepareHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}


export const createFulfilHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const TransferService = require('../../domain/transfer/index')
  const Validator = require('./validator')
  const Comparators = require('@mojaloop/central-services-shared').Util.Comparators
  const FxService = require('../../domain/fx')
  const TransferObjectTransform = require('../../domain/transfer/transform')

  const dependencies: FulfilHandlerDependencies = {
    notificationProducer: new NotificationProducer(notificationProducer, config),
    committer: new MessageCommitter(consumer),
    config,
    transferService: TransferService,
    validator: Validator,
    comparators: Comparators,
    fxService: FxService,
    transferObjectTransform: TransferObjectTransform
  }

  const handler = new FulfilHandler(dependencies)
  return (error: any, message: any) => handler.handle(error, message)
}

export const registerPrepareHandler_new = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProduer: Kafka.Producer,
  notificationProducer: Kafka.Producer
): Promise<void> => {
  try {
    logger.debug(`registerPrepareHandlerNew registering`)

    const handleMessage = createPrepareHandler(
      config, consumer, positionProduer, notificationProducer
    )
    
    consumer.consume(handleMessage)
  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPrepareHandlerNew' })
  }
}

export const registerFulfilHandler_new = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
): Promise<void> => {
  try {
    logger.debug(`registerFulfilHandler_new registering`)

    const handleMessage = createFulfilHandler(config, consumer, notificationProducer)
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerFulfilHandler_new' })
  }
}