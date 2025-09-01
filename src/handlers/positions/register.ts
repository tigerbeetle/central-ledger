import { Enum, Util } from '@mojaloop/central-services-shared'
import { Kafka } from '@mojaloop/central-services-stream'
import { ApplicationConfig } from 'src/shared/config'
import { MessageCommitter } from '../../messaging/MessageCommitter'
import { NotificationProducer } from '../../messaging/producers/NotificationProducer'
import { PositionProducer } from '../../messaging/producers/PositionProducer'
import { logger } from '../../shared/logger'
import { PositionHandler, PositionHandlerDependencies } from './PositionHandler'

const rethrow = Util.rethrow

export const createPositionHandler = (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
  positionProducer: Kafka.Producer,
) => {
  // Import existing business logic modules
  const TransferService = require('../../domain/transfer/index')
  const PositionService = require('../../domain/position')
  const participantFacade = require('../../models/participant/facade')
  const SettlementModelCached = require('../../models/settlement/settlementModelCached')
  const TransferObjectTransform = require('../../domain/transfer/transform')
  
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

export const registerPositionHandler_new = async (
  config: ApplicationConfig,
  consumer: Kafka.Consumer,
  notificationProducer: Kafka.Producer,
  positionProducer: Kafka.Producer,
): Promise<void> => {
  try {
    logger.debug(`registerPositionHandler_new registering`)

    // Initialize settlement model cache (required by position handler)
    const SettlementModelCached = require('../../models/settlement/settlementModelCached')
    await SettlementModelCached.initialize()

    // Create the position handler function
    const handleMessage = createPositionHandler(
      config, consumer, notificationProducer, positionProducer
    )
    consumer.consume(handleMessage)

  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPrepareHandlerNew' })

    
  }
}