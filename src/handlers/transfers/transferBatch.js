const ledger = require('#src/domain/fast/ledger')
const Logger = require('@mojaloop/central-services-logger')
const { Enum, Util } = require('@mojaloop/central-services-shared')
const { Consumer, Producer } = require('@mojaloop/central-services-stream').Util
const decodePayload = Util.StreamingProtocol.decodePayload

const assert = require('assert')



const handlePrepares = async (error, messages) => {
  assert(messages)
  assert(Array.isArray(messages))
  assert(messages.length === 1, 'Expected only 1 message from Kafka')
  const message = messages[0]

  assert(message.value)
  assert(message.value.id)
  assert(message.value.content)
  assert(message.value.content.count)
  assert(message.value.content.batch)
  assert(message.value.metadata)

  const batchId = message.value.id

  if (error) {
    // need to understand these error conditions
    throw new Error(`Kafka Error: ${error}`)
  }

  console.log(`LD handlePrepares, handling batch of`, message.value.content.count)

  const prepares = message.value.content.batch
  const batch = await ledger.assemblePrepareBatch(prepares)
  const errors = await ledger.createTransfers(batch)

  if (errors.length > 0) {
    console.log(`WARN: ${errors.length} unhandled TigerBeetle errors - need to be handled`)
  }  

  // emit 1 notification message with fail/error buckets
  const messageProtocol = {
    content: {
      count: batch.length,
      // map of ids and error codes 
      failed: {},
    },
    id: batchId,
  }
  const topicConf = {
    topicName: 'notification-batch'
  }

  await Producer.produceMessage(messageProtocol, topicConf)
}

const handleFulfils = async (error, message) => {
  throw new Error('not implemented')
}

const registerHandlePreparesHandler = async () => {
  const topicName = `transfer-batch-prepare`
  // TODO: configure
  const consumeConfig = {
    config: {
      mode: 2,
      batchSize: 1,
      pollFrequency: 10,
      recursiveTimeout: 1,
      messageCharset: "utf8",
      messageAsJSON: true,
      sync: true,
      consumeTimeout: 1
    },
    rdkafkaConf: {
      "client.id": "cl-con-transfer-prepare",
      "group.id": "cl-group-transfer-prepare",
      "metadata.broker.list": "localhost:9192",
      "socket.keepalive.enable": true,
      "allow.auto.create.topics": true
    },
    topicConf: {
      "auto.offset.reset": "earliest"
    }
  }

  await Consumer.createHandler(topicName, consumeConfig, handlePrepares)
}

const registerHandleFulfilsHandler = async () => {
  throw new Error('not implemented!')

  const topicName = `transfer-batch-fulfil`
  // TODO: configure
  const consumeConfig = {
    config: {

    }

  }

  await Consumer.createHandler(topicName, consumeConfig, handlePrepares)
}


module.exports = {
  handlePrepares,
  handleFulfils,
  registerHandlePreparesHandler,
  registerHandleFulfilsHandler
}