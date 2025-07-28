import assert from 'assert'
import { KafkaConfig, KafkaConsumerConfig, KafkaProducerConfig } from './types'

class ConfigValidationError extends Error {
  constructor(message) {
    super(message)
    this.name = 'ConfigValidationError'
  }
}

export const assertString = (input: unknown): ConfigValidationError | void => {
  if (typeof input !== 'string') {
    throw new ConfigValidationError(`assertString() expected 'string', instead found ${typeof input}`)
  }
}

export const assertNumber = (input: unknown): ConfigValidationError | void => {
  if (typeof input !== 'number') {
    throw new ConfigValidationError(`assertNumber() expected 'number', instead found ${typeof input}`)
  }

  if (isNaN(input)) {
    throw new ConfigValidationError(`assertNumber() expected 'number', instead found NaN`)
  }
}

export const assertBoolean = (input: unknown): ConfigValidationError | void => {
  if (typeof input !== 'boolean') {
    throw new ConfigValidationError(`assertNumber() expected 'boolean', instead found ${typeof input}`)
  }
}

export const assertStringOrNull = (input: unknown) => {
  if (input === null) {
    return
  }

  return assertString(input)
}

export const assertProxyCacheConfig = (input: unknown): ConfigValidationError | void => {
  try {
    assert(input)
    // We could add more validation here, or simply rely on the `@mojaloop/inter-scheme-proxy-cache-lib`
    // to handle it
  } catch (err) {
    throw new ConfigValidationError(err.message)
  }
}

export const assertKafkaConfig = (input: unknown): ConfigValidationError | void => {

  const assertKafkaConsumerConfig = (inputConsumer: unknown) => {
    const unsafeConsumerConfig = inputConsumer as KafkaConsumerConfig
    assert(unsafeConsumerConfig)
    assert(unsafeConsumerConfig.config)
    // We could do some more asserting here, but this is a good start
    assert(unsafeConsumerConfig.config.options)
    assert(unsafeConsumerConfig.config.rdkafkaConf)
    assert(unsafeConsumerConfig.config.topicConf)
  }

  const assertKafkaProducerConfig = (inputConsumer: unknown) => {
    const unsafeProducerConfig = inputConsumer as KafkaProducerConfig
    assert(unsafeProducerConfig)
    assert(unsafeProducerConfig.config)
    // We could do some more asserting here, but this is a good start
    assert(unsafeProducerConfig.config.options)
    assert(unsafeProducerConfig.config.rdkafkaConf)
    assert(unsafeProducerConfig.config.topicConf)
  }

  // Check the `EVENT_TYPE_ACTION_TOPIC_MAP`
  const unsafeKafkaConfig = input as KafkaConfig
  if (!unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP) {
    throw new ConfigValidationError(`missing EVENT_TYPE_ACTION_TOPIC_MAP`)
  }
  if (!unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION) {
    throw new ConfigValidationError(`missing EVENT_TYPE_ACTION_TOPIC_MAP.POSITION`)
  }

  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.PREPARE)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_PREPARE)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.BULK_PREPARE)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.COMMIT)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.BULK_COMMIT)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.RESERVE)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_RESERVE)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.TIMEOUT_RESERVED)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_TIMEOUT_RESERVED)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.ABORT)
  assertStringOrNull(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_ABORT)

  // Check the `TOPIC_TEMPLATES`
  if (!unsafeKafkaConfig.TOPIC_TEMPLATES) {
    throw new ConfigValidationError(`missing TOPIC_TEMPLATES`)
  }
  if (!unsafeKafkaConfig.TOPIC_TEMPLATES.PARTICIPANT_TOPIC_TEMPLATE) {
    throw new ConfigValidationError(`missing TOPIC_TEMPLATES.PARTICIPANT_TOPIC_TEMPLATE`)
  }
  if (!unsafeKafkaConfig.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE) {
    throw new ConfigValidationError(`missing TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE`)
  }

  // Check the Consumer Configs
  assert(unsafeKafkaConfig.CONSUMER)
  assert(unsafeKafkaConfig.CONSUMER.BULK)
  assert(unsafeKafkaConfig.CONSUMER.BULK.PREPARE)
  assert(unsafeKafkaConfig.CONSUMER.BULK.PROCESSING)
  assert(unsafeKafkaConfig.CONSUMER.BULK.FULFIL)
  assert(unsafeKafkaConfig.CONSUMER.BULK.GET)
  assert(unsafeKafkaConfig.CONSUMER.TRANSFER)
  assert(unsafeKafkaConfig.CONSUMER.TRANSFER.PREPARE)
  assert(unsafeKafkaConfig.CONSUMER.TRANSFER.GET)
  assert(unsafeKafkaConfig.CONSUMER.TRANSFER.FULFIL)
  assert(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION)
  assert(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION_BATCH)
  assert(unsafeKafkaConfig.CONSUMER.ADMIN)
  assert(unsafeKafkaConfig.CONSUMER.ADMIN.TRANSFER)
  assert(unsafeKafkaConfig.CONSUMER.NOTIFICATION)
  assert(unsafeKafkaConfig.CONSUMER.NOTIFICATION.EVENT)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.PREPARE)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.PROCESSING)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.FULFIL)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.GET)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.PREPARE)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.GET)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.FULFIL)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION_BATCH)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.ADMIN.TRANSFER)
  assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.NOTIFICATION.EVENT)

  // Check the Producer Configs
  assert(unsafeKafkaConfig.PRODUCER)
  assert(unsafeKafkaConfig.PRODUCER.BULK)
  assert(unsafeKafkaConfig.PRODUCER.BULK.PROCESSING)
  assert(unsafeKafkaConfig.PRODUCER.TRANSFER)
  assert(unsafeKafkaConfig.PRODUCER.TRANSFER.PREPARE)
  assert(unsafeKafkaConfig.PRODUCER.TRANSFER.FULFIL)
  assert(unsafeKafkaConfig.PRODUCER.TRANSFER.POSITION)
  assert(unsafeKafkaConfig.PRODUCER.NOTIFICATION)
  assert(unsafeKafkaConfig.PRODUCER.NOTIFICATION.EVENT)
  assert(unsafeKafkaConfig.PRODUCER.ADMIN)
  assert(unsafeKafkaConfig.PRODUCER.ADMIN.TRANSFER)
  
  assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.BULK.PROCESSING)
  assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.TRANSFER.PREPARE)
  assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.TRANSFER.FULFIL)
  assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.TRANSFER.POSITION)
  assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.NOTIFICATION.EVENT)
  assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.ADMIN.TRANSFER)
}

export const defaultTo = <T>(input: unknown, defaultValue: T): T =>  {
  if (input === undefined) {
    return defaultValue
  }

  assert.equal(typeof input, typeof defaultValue)
  return input as T
}

export const stringToBoolean = (input: string): boolean => {
  assert(input !== undefined)
  assert(typeof input === 'string')

  switch (input.toLowerCase()) {
    case 'true': return true;
    case 'false': return false;
    default: {
      throw new Error(`stringToBoolean, unknown input: ${input}`)
    }
  }
}


export const defaultEnvString = (envName: string, defaultValue: string): string => {
  assert(defaultValue, 'expected a default value')

  let processEnvValue = process.env[envName]
  // need to protect for cases where the value may intentionally false!
  if (processEnvValue === undefined) {
    return defaultValue
  }

  if (Array.isArray(processEnvValue)) {
    processEnvValue = processEnvValue[0]
  }
  if (processEnvValue === undefined) {
    return defaultValue
  }

  return processEnvValue
}


