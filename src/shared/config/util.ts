import assert from 'assert'
import { KafkaConfig, KafkaConsumerConfig, KafkaProducerConfig } from './types'
import { logger } from '../logger'

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
    logger.warn(`defaultEnvString - ${envName} not set - defaulting to: ${defaultValue}`)
    return defaultValue
  }

  if (Array.isArray(processEnvValue)) {
    processEnvValue = processEnvValue[0]
  }
  if (processEnvValue === undefined) {
    logger.warn(`defaultEnvString - ${envName} not set - defaulting to: ${defaultValue}`)
    return defaultValue
  }

  logger.warn(`defaultEnvString - ${envName} is  set - resolved   to: ${processEnvValue}`)
  return processEnvValue
}

/**
 * @function kafkaWithBrokerDefaults
 * @description Allows us to easily configure the metadata.broker.list without needing to touch
 *   each config file. If config.rdkafkaConf['metadata.broker.list'] is already set, then this
 *   doesn't modify it.
 */
export const kafkaWithBrokerDefaults = (input: KafkaConfig, defaultBroker: string): KafkaConfig => {
  assert(defaultBroker)
  assert(input.CONSUMER)
  assert(input.PRODUCER)

  Object.keys(input).filter(groupKey => {
    if (groupKey === 'CONSUMER') {
      return true
    }
    if (groupKey === 'PRODUCER') {
      return true
    }
    return false
  }).forEach(groupKey => {
    const group = input[groupKey]

    Object.keys(group).forEach(key => {
      const topic = input[groupKey][key]
      Object.keys(topic).forEach(topicKey => {
        const leafConfig = topic[topicKey]
        const path = `input.${groupKey}.${key}.${topicKey}`
        if (leafConfig.config 
          && leafConfig.config.rdkafkaConf
          && !leafConfig.config.rdkafkaConf['metadata.broker.list']
        ) {
          // Disabled this noisy log - not sure if we should reenable it
          // logger.debug(`Config kafkaWithBrokerDefaults() defaulting: ${path}.config.rdkafkaConf['metadata.broker.list'] with: ${defaultBroker}`)
          input[groupKey][key][topicKey]['config']['rdkafkaConf']['metadata.broker.list'] = defaultBroker
        }
      })
    })
  })

  return input
}

/**
 * @function convertBigIntToNumber
 * @description Converts a bigint to a number, throwing an error if the bigint is outside of the 
 *  range (MIN_SAFE_INTEGER, MAX_SAFE_INTEGER)
 */
export function convertBigIntToNumber(input: bigint): number {
  if (input > BigInt(Number.MAX_SAFE_INTEGER) ||
    input < BigInt(Number.MIN_SAFE_INTEGER)
  ) {
    throw new Error(`convertBigIntToNumber failed: input is outside of safe range.`)
  }

  return Number(input)
}

/**
 * @function safeStringToNumber
 * @description Safetly convert from a string representation of a number to a js number
 */
export function safeStringToNumber(input: string) {
  assert(typeof input === 'string')
  // Trim whitespace
  const trimmed = input.trim();
  
  // Check if it's a valid number string
  if (trimmed === '' || isNaN(Number(trimmed))) {
    throw new Error(`Invalid number string: "${input}"`);
  }
  
  const num = Number(trimmed);
  
  // Check for infinity
  if (!isFinite(num)) {
    throw new Error(`Number out of range: "${input}"`);
  }
  
  return num;
}
