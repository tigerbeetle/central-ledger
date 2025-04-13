const Logger = require('@mojaloop/central-services-logger')

const defaultValue = (maybeValue, dflt) => {
  if (maybeValue === undefined) {
    return dflt
  }

  return maybeValue
}

const PATH_TO_CONFIG_FILE = defaultValue(process.env.PATH_TO_CONFIG_FILE,'../../config/default.json')
Logger.info(`Config - loading config file from '${PATH_TO_CONFIG_FILE}'`)

const RC = require('rc')('CLEDG', require(PATH_TO_CONFIG_FILE))
const assert = require('assert')

const stringToBool = (input) => {
  assert(input !== undefined)
  const lowerStr = `${input}`.toLowerCase()
  if (lowerStr === 'false') {
    return false
  }
  if (lowerStr === 'true') {
    return true
  }
  throw new Error(`stringToBool, invalid input: ${input}`)
}

/**
 * @function kafkaWithBrokerOverrides
 * @description Allows us to easily configure the metadata.broker.list without needing to touch
 *   each config file
 */
const kafkaWithBrokerOverrides = (input, defaultBroker) => {
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
          Logger.info(`Config kafkaWithBrokerOverrides() overriding: ${path}.config.rdkafkaConf['metadata.broker.list']`)
          input[groupKey][key][topicKey]['config']['rdkafkaConf']['metadata.broker.list'] = defaultBroker
        }
      })
    })
  })

  return input
}


const defaultBroker = defaultValue(RC.KAFKA.DEFAULT_BROKER, 'localhost:9192')
const kafka = kafkaWithBrokerOverrides(RC.KAFKA, defaultBroker)


const resolvedConfig = {
  HOSTNAME: RC.HOSTNAME.replace(/\/$/, ''),
  PORT: RC.PORT,
  MAX_FULFIL_TIMEOUT_DURATION_SECONDS: RC.MAX_FULFIL_TIMEOUT_DURATION_SECONDS || 300,
  MONGODB_HOST: RC.MONGODB.HOST,
  MONGODB_PORT: RC.MONGODB.PORT,
  MONGODB_USER: RC.MONGODB.USER,
  MONGODB_PASSWORD: RC.MONGODB.PASSWORD,
  MONGODB_DATABASE: RC.MONGODB.DATABASE,
  MONGODB_DEBUG: RC.MONGODB.DEBUG === true,
  MONGODB_DISABLED: RC.MONGODB.DISABLED === true,
  AMOUNT: RC.AMOUNT,
  EXPIRES_TIMEOUT: RC.EXPIRES_TIMEOUT,
  ERROR_HANDLING: RC.ERROR_HANDLING,
  HANDLERS: RC.HANDLERS,
  HANDLERS_DISABLED: RC.HANDLERS.DISABLED,
  HANDLERS_API: RC.HANDLERS.API,
  HANDLERS_API_DISABLED: RC.HANDLERS.API.DISABLED,
  HANDLERS_TIMEOUT: RC.HANDLERS.TIMEOUT,
  HANDLERS_TIMEOUT_DISABLED: RC.HANDLERS.TIMEOUT.DISABLED,
  HANDLERS_TIMEOUT_TIMEXP: RC.HANDLERS.TIMEOUT.TIMEXP,
  HANDLERS_TIMEOUT_TIMEZONE: RC.HANDLERS.TIMEOUT.TIMEZONE,
  CACHE_CONFIG: RC.CACHE,
  PROXY_CACHE_CONFIG: RC.PROXY_CACHE,
  // TODO (LD): CONFIG here is redundant, this is already config
  // Duplicating to plain KAFKA to maintain backwards compatibility
  KAFKA_CONFIG: kafka,
  KAFKA: {
    /**
     * DEFAULT_BROKER
     * 
     * Overwritten by specific producer/consumer config
     * 
     * Default: localhost:9192
     */
    DEFAULT_BROKER: defaultValue(RC.KAFKA.DEFAULT_BROKER, 'localhost:9192'),

    /**
     * DEBUG_EXTREME_BATCHING
     * 
     * Description: When `true`, uses in-message Kafka batching, where many Prepares and Fulfils
     *   are combined into the same Kafka message.
     * 
     * Default: false
     */
    DEBUG_EXTREME_BATCHING: stringToBool(defaultValue(RC.KAFKA.DEBUG_EXTREME_BATCHING || false)),
  },
  PARTICIPANT_INITIAL_POSITION: RC.PARTICIPANT_INITIAL_POSITION,
  RUN_MIGRATIONS: !RC.MIGRATIONS.DISABLED,
  RUN_DATA_MIGRATIONS: RC.MIGRATIONS.RUN_DATA_MIGRATIONS,
  INTERNAL_TRANSFER_VALIDITY_SECONDS: RC.INTERNAL_TRANSFER_VALIDITY_SECONDS,
  ENABLE_ON_US_TRANSFERS: RC.ENABLE_ON_US_TRANSFERS,
  HUB_ID: RC.HUB_PARTICIPANT.ID,
  HUB_NAME: RC.HUB_PARTICIPANT.NAME,
  HUB_ACCOUNTS: RC.HUB_PARTICIPANT.ACCOUNTS,
  INSTRUMENTATION_METRICS_DISABLED: RC.INSTRUMENTATION.METRICS.DISABLED,
  INSTRUMENTATION_METRICS_LABELS: RC.INSTRUMENTATION.METRICS.labels,
  INSTRUMENTATION_METRICS_CONFIG: RC.INSTRUMENTATION.METRICS.config,
  DATABASE: {
    client: RC.DATABASE.DIALECT,
    connection: {
      host: RC.DATABASE.HOST.replace(/\/$/, ''),
      port: RC.DATABASE.PORT,
      user: RC.DATABASE.USER,
      password: RC.DATABASE.PASSWORD,
      database: RC.DATABASE.SCHEMA
    },
    pool: {
      // minimum size
      min: RC.DATABASE.POOL_MIN_SIZE,
      // maximum size
      max: RC.DATABASE.POOL_MAX_SIZE,
      // acquire promises are rejected after this many milliseconds
      // if a resource cannot be acquired
      acquireTimeoutMillis: RC.DATABASE.ACQUIRE_TIMEOUT_MILLIS,
      // create operations are cancelled after this many milliseconds
      // if a resource cannot be acquired
      createTimeoutMillis: RC.DATABASE.CREATE_TIMEOUT_MILLIS,
      // destroy operations are awaited for at most this many milliseconds
      // new resources will be created after this timeout
      destroyTimeoutMillis: RC.DATABASE.DESTROY_TIMEOUT_MILLIS,
      // free resouces are destroyed after this many milliseconds
      idleTimeoutMillis: RC.DATABASE.IDLE_TIMEOUT_MILLIS,
      // how often to check for idle resources to destroy
      reapIntervalMillis: RC.DATABASE.REAP_INTERVAL_MILLIS,
      // long long to idle after failed create before trying again
      createRetryIntervalMillis: RC.DATABASE.CREATE_RETRY_INTERVAL_MILLIS
      // ping: function (conn, cb) { conn.query('SELECT 1', cb) }
    },
    debug: RC.DATABASE.DEBUG
  },
  API_DOC_ENDPOINTS_ENABLED: RC.API_DOC_ENDPOINTS_ENABLED || false,
  // If this is set to true, payee side currency conversion will not be allowed due to a limitation in the current implementation
  PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED: (RC.PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED === true || RC.PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED === 'true'),
  SETTLEMENT_MODELS: RC.SETTLEMENT_MODELS,

  /**
   * 
   */
  LEDGER: {

    /**
     * LEDGER.MODE
     * Description: Determines which Ledger should be used.
     *   TIGERBEETLE is currently in preview, and is not production-ready
     * 
     * Options: SQL | TIGERBEETLE
     * Default: SQL
     */
    MODE: defaultValue(RC.LEDGER.MODE, 'SQL'),
    OPTIONS: {
      /**
      * TB_ADDRESS
      * 
      * Description: Comma a separated list of TigerBeetle nodes
      * 
      * Default: 3000
      */
      TB_ADDRESS: defaultValue(RC.LEDGER.OPTIONS.TB_ADDRESS || '3000').split(','),

      /**
       * DEBUG_SKIP_TIGERBEETLE
       * 
       * Description: When `true`, calls to TigerBeetle will be skipped. This serves as a performance
       *   double check to find bottlenecks. Will be disabled before Production readyness
       * 
       * Default: false
       */
      DEBUG_SKIP_TIGERBEETLE: stringToBool(defaultValue(RC.LEDGER.OPTIONS.DEBUG_SKIP_TIGERBEETLE || false)),
    }
  },
}



// Validate config

if (['SQL', 'TIGERBEETLE'].indexOf(resolvedConfig.LEDGER.MODE) === -1) {
  throw new Error(`ConfigError - LEDGER.MODE must be SQL or TIGERBEETLE. Found: ${resolvedConfig.LEDGER.MODE}`)
}

if (resolvedConfig.LEDGER.MODE === 'SQL' && resolvedConfig.KAFKA.DEBUG_EXTREME_BATCHING) {
  throw new Error(`ConfigError - 'KAFKA.DEBUG_EXTREME_BATCHING' cannot be enabled when 'LEDGER.MODE' is SQL`)
}

if (resolvedConfig.LEDGER.MODE === 'TIGERBEETLE') {
  assert(resolvedConfig.LEDGER.OPTIONS.TB_ADDRESS)
  assert(Array.isArray(resolvedConfig.LEDGER.OPTIONS.TB_ADDRESS))
  assert(resolvedConfig.LEDGER.OPTIONS.TB_ADDRESS.length > 0)
  assert(resolvedConfig.LEDGER.OPTIONS.TB_ADDRESS.length <= 6)
}


module.exports = resolvedConfig