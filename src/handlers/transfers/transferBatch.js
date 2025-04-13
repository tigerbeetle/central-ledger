const ledger = require('#src/domain/fast/ledger')
const Logger = require('@mojaloop/central-services-logger')
const { Enum, Util } = require('@mojaloop/central-services-shared')
const { Consumer, Producer } = require('@mojaloop/central-services-stream').Util
const decodePayload = Util.StreamingProtocol.decodePayload
const Validator = require('./validator')


const assert = require('assert')
const { fulfil } = require('./handler')
const { CreateTransferError } = require('tigerbeetle-node')
const config = require('#src/lib/config')


const _validatePreparesMessage = (message) => {
  try {
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.count)
    assert(message.value.content.batch)
    assert(message.value.metadata)
    assert(message.value.id)
  } catch (err) {
    throw err;
  }
}

const handlePrepares = async (error, messages) => {
  if (error) {
    // need to understand these error conditions
    throw new Error(`Kafka Error: ${error}`)
  }

  assert(messages)
  assert(Array.isArray(messages))
  assert(messages.length === 1, 'Expected only 1 message from Kafka')
  const message = messages[0]

  try {
    _validatePreparesMessage(message)
  } catch (err) {
    console.log('TODO: handle invalid message from kafka!')
    return;
  }
  
  const batchId = message.value.id

  console.log(`LD handlePrepares, handling batch of`, message.value.content.count)

  const prepares = message.value.content.batch
  const batch = await ledger.assemblePrepareBatch(prepares)
  const errors = await ledger.createTransfers(batch)

  for (const error of errors) {
    console.error(`Batch account at ${error.index} failed to create: ${CreateTransferError[error.result]}.`)
  }

  if (errors.length > 0) {
    console.log(`WARN: ${errors.length} unhandled TigerBeetle errors - need to be handled`)
  }  

  // emit 1 notification message with fail/error buckets
  const messageProtocol = {
    content: {
      count: batch.length,
      batch: prepares,
      // TODO: put in some proper metadata here
      metadata: prepares,

      // map of ids and error codes 
      failed: {},
    },
    metadata: {
      event: {
        type: 'notification',
        action: 'prepare',
      }
    },
    id: batchId,
  }
  const topicConf = {
    topicName: 'notification-batch'
  }

  await Producer.produceMessage(messageProtocol, topicConf)
}

const _validateFulfilsMessage = (message) => {
  try {
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.count)
    assert(message.value.content.batch)
    assert(message.value.metadata)
    assert(message.value.id)
  } catch (err) {
    throw err;
  }
}

const handleFulfils = async (error, messages) => {
  if (error) {
    // need to understand these error conditions
    throw new Error(`Kafka Error: ${error}`)
  }

  assert(messages)
  assert(Array.isArray(messages))
  assert(messages.length === 1, 'Expected only 1 message from Kafka')
  const message = messages[0]

  try {
    _validateFulfilsMessage(message)
  } catch (err) {
    console.log('TODO: handle invalid message from kafka!')
    return;
  }

  console.log(`LD handleFulfils, handling batch of`, message.value.content.count)

  const batchId = message.value.id


  const fulfils = message.value.content.batch
  // need a better name than metadata or context
  const metadata = message.value.content.metadata

  assert.equal(fulfils.length, metadata.length)

  // TODO: We need to load the prepareContext in here so that we can check that the condition
  // and fulfilment match one another. My plan is to have this already loaded in-memory from
  // a message that the ml-api-adapter broadcasts to each of the fulfil handlers.
  //
  // For now, we can 'make up' some dummy data and assume that the condition and fulfilment match
  const dummyPrepareContext = fulfils.map((fulfil, idx) => {
    const fulfilMetadata = metadata[idx]
    const transferId = fulfilMetadata.transferId
    // const headers = headerList[idx]
    return {
      transferId,
      payeeFsp: fulfilMetadata.payeeFsp,
      payerFsp: fulfilMetadata.payerFsp,
      amount: { amount: '1', currency: 'USD' },
      ilpPacket: 'DIICtgAAAAAAD0JAMjAyNDEyMDUxNjA4MDM5MDcYjF3nFyiGSaedeiWlO_87HCnJof_86Krj0lO8KjynIApnLm1vamFsb29wggJvZXlKeGRXOTBaVWxrSWpvaU1ERktSVUpUTmpsV1N6WkJSVUU0VkVkQlNrVXpXa0U1UlVnaUxDSjBjbUZ1YzJGamRHbHZia2xrSWpvaU1ERktSVUpUTmpsV1N6WkJSVUU0VkVkQlNrVXpXa0U1UlVvaUxDSjBjbUZ1YzJGamRHbHZibFI1Y0dVaU9uc2ljMk5sYm1GeWFXOGlPaUpVVWtGT1UwWkZVaUlzSW1sdWFYUnBZWFJ2Y2lJNklsQkJXVVZTSWl3aWFXNXBkR2xoZEc5eVZIbHdaU0k2SWtKVlUwbE9SVk5USW4wc0luQmhlV1ZsSWpwN0luQmhjblI1U1dSSmJtWnZJanA3SW5CaGNuUjVTV1JVZVhCbElqb2lUVk5KVTBST0lpd2ljR0Z5ZEhsSlpHVnVkR2xtYVdWeUlqb2lNamMzTVRNNE1ETTVNVElpTENKbWMzQkpaQ0k2SW5CaGVXVmxabk53SW4xOUxDSndZWGxsY2lJNmV5SndZWEowZVVsa1NXNW1ieUk2ZXlKd1lYSjBlVWxrVkhsd1pTSTZJazFUU1ZORVRpSXNJbkJoY25SNVNXUmxiblJwWm1sbGNpSTZJalEwTVRJek5EVTJOemc1SWl3aVpuTndTV1FpT2lKMFpYTjBhVzVuZEc5dmJHdHBkR1JtYzNBaWZYMHNJbVY0Y0dseVlYUnBiMjRpT2lJeU1ESTBMVEV5TFRBMVZERTJPakE0T2pBekxqa3dOMW9pTENKaGJXOTFiblFpT25zaVlXMXZkVzUwSWpvaU1UQXdJaXdpWTNWeWNtVnVZM2tpT2lKWVdGZ2lmWDA',
      condition: 'GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA',
      expiration: '2025-04-03T19:23:01.961Z'
    }
  })

  const validFulils = []
  const validPrepareContext = []

  dummyPrepareContext.forEach((context, idx) => {
    const fulfil = fulfils[idx]

    // We put in this call so that performance is mocked out appropriately
    const dummyFulfilment = 'V-IalzIzy-zxy0SrlY1Ku2OE9aS4KgGZ0W-Zq5_BeC0'
    const dummyCondition = 'GIxd5xcohkmnnXolpTv_OxwpyaH__Oiq49JTvCo8pyA'
    const isValid = Validator.validateFulfilCondition(dummyFulfilment, dummyCondition);

    // We wont just throw here, but instead separate out from the batch
    if (!isValid) {
      // TODO: add to an Errored list, with an error reason, 

      throw new Error(`condition and fulfillment don't match!`);
    }

    validFulils.push(fulfil)
    validPrepareContext.push(context)
  })

  const batch = await ledger.assembleFulfilBatch(validFulils, validPrepareContext)
  const errors = await ledger.createTransfers(batch)

  for (const error of errors) {
    console.error(`Batch account at ${error.index} failed to create: ${CreateTransferError[error.result]}.`)
  }

  if (errors.length > 0) {
    console.log(`WARN: ${errors.length} unhandled TigerBeetle errors - need to be handled`)
  }  

  // emit 1 notification message with fail/error buckets
  const messageProtocol = {
    content: {
      count: batch.length,
      batch: fulfils,
      metadata: metadata,

      // map of ids and error codes 
      failed: {},
    },
    metadata: {
      event: {
        type: 'notification',
        action: 'fulfil',
      }
    },
    id: batchId,
  }
  const topicConf = {
    topicName: 'notification-batch'
  }

  await Producer.produceMessage(messageProtocol, topicConf)
}

const registerHandlePreparesHandler = async () => {
  console.log('registerHandlePreparesHandler metadata.broker.list', config.DEFAULT_KAFKA_BROKER)

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
      "client.id": "transfer-batch-prepares",
      "group.id": "transfer-batch-prepares",
      "metadata.broker.list": config.DEFAULT_KAFKA_BROKER,
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
  console.log('registerHandleFulfilsHandler metadata.broker.list', config.DEFAULT_KAFKA_BROKER)

  const topicName = `transfer-batch-fulfil`
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
      "client.id": "transfer-batch-fulfils",
      "group.id": "transfer-batch-fulfils",
      "metadata.broker.list": config.DEFAULT_KAFKA_BROKER,
      "socket.keepalive.enable": true,
      "allow.auto.create.topics": true
    },
    topicConf: {
      "auto.offset.reset": "earliest"
    }
  }

  await Consumer.createHandler(topicName, consumeConfig, handleFulfils)
}


module.exports = {
  handlePrepares,
  handleFulfils,
  registerHandlePreparesHandler,
  registerHandleFulfilsHandler
}