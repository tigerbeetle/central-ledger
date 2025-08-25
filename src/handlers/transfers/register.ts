import { Enum, Util } from '@mojaloop/central-services-shared'
import { ApplicationConfig } from 'src/shared/config'
import { Kafka } from '@mojaloop/central-services-stream'
import { logger } from '../../shared/logger'
import { PrepareHandler, PrepareHandlerDependencies } from './PrepareHandler'
import { PositionProducer } from '../../messaging/producers/PositionProducer'
import { NotificationProducer } from '../../messaging/producers/NotificationProducer'
import { MessageCommitter } from '../../messaging/MessageCommitter'
import { Consumers } from 'src/shared/setup-new'

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

export interface PrepareHandlerClients {
  consumer: Kafka.Consumer;
  positionProducer: Kafka.Producer;
  notificationProducer: Kafka.Producer;
}

export const registerPrepareHandlerNew = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  positionProduer: Kafka.Producer,
  notificationProducer: Kafka.Producer
): Promise<void> => {
  try {
    logger.debug(`registerPrepareHandlerNew registering`)

    // Create and register handler with injected clients
    const handleMessage = createPrepareHandler(
      config, consumer, positionProduer, notificationProducer
    )
    
    consumer.consume(handleMessage)

    logger.info('registerPrepareHandlerNew registered successfully')
  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPrepareHandlerNew' })
  }
}


// Keep the old function for backward compatibility
export const registerPrepareHandler = async (config: ApplicationConfig, handler: (error: any, messages: any, consumer: Kafka.Consumer) => any) => {
  try {
    const { TRANSFER } = TransferEventType
    const { PREPARE } = TransferEventAction

    const topicName = KafkaUtil.transformGeneralTopicName(config.KAFKA_CONFIG.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE, TRANSFER, PREPARE)
    logger.debug(`registerPrepareHandler registering to topicName: '${topicName}'`)

    const consumerConfig = KafkaUtil.getKafkaConfig(config.KAFKA_CONFIG, Enum.Kafka.Config.CONSUMER, TRANSFER.toUpperCase(), PREPARE.toUpperCase());
    (consumerConfig as any).rdkafkaConf['client.id'] = topicName

    const consumer = new Kafka.Consumer([topicName], consumerConfig)
    await consumer.connect()

    consumer.consume((error, messages) => handler(error, messages, consumer))
  
    return consumer
  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPrepareHandler' })
  }
}