import { Enum, Util } from '@mojaloop/central-services-shared'
import { ApplicationConfig } from 'src/shared/config'
import { Kafka } from '@mojaloop/central-services-stream'
import { logger } from '../../shared/logger'

const rethrow = Util.rethrow
const KafkaUtil = Util.Kafka
const TransferEventType = Enum.Events.Event.Type
const TransferEventAction = Enum.Events.Event.Action


export const registerPrepareHandler = async (config: ApplicationConfig, handler: (error: any, messages: any) => any) => {
  try {
    const { TRANSFER } = TransferEventType
    const { PREPARE } = TransferEventAction

    const topicName = KafkaUtil.transformGeneralTopicName(config.KAFKA_CONFIG.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE.TEMPLATE, TRANSFER, PREPARE)
    logger.debug(`registerPrepareHandler registering to topicName: '${topicName}'`)

    // TODO(LD): improve typing
    const consumerConfig = KafkaUtil.getKafkaConfig(config.KAFKA_CONFIG, Enum.Kafka.Config.CONSUMER, TRANSFER.toUpperCase(), PREPARE.toUpperCase())
    // @ts-ignore
    consumerConfig.rdkafkaConf['client.id'] = topicName

    const consumer = new Kafka.Consumer([topicName], consumerConfig)
    await consumer.connect()

    consumer.consume(handler)
  
    return consumer
  } catch (err) {
    rethrow.rethrowAndCountFspiopError(err, { operation: 'registerPrepareHandler' })
  }
}