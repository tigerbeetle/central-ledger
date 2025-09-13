import path from 'node:path'
import parseStringsInObject from 'parse-strings-in-object'
import RC from 'rc'
import { ApplicationConfig } from './types'
import { assertBoolean, assertKafkaConfig, assertNumber, assertProxyCacheConfig, assertString, defaultEnvString, defaultTo } from './util'
import assert from 'node:assert'
import { raw } from 'mysql'
import { logger } from '../logger'

type UnsafeApplicationConfig = Partial<ApplicationConfig>

const resolveConfig = (rawConfig: any): UnsafeApplicationConfig  => {
  const unsafeConfig: UnsafeApplicationConfig = {
    HOSTNAME: rawConfig.HOSTNAME.replace(/\/$/, ''),
    PORT: rawConfig.PORT,
    MAX_FULFIL_TIMEOUT_DURATION_SECONDS: defaultTo(rawConfig.MAX_FULFIL_TIMEOUT_DURATION_SECONDS, 300),
    MONGODB_HOST: rawConfig.MONGODB.HOST,
    MONGODB_PORT: rawConfig.MONGODB.PORT,  
    MONGODB_USER: rawConfig.MONGODB.USER,
    MONGODB_PASSWORD: rawConfig.MONGODB.PASSWORD,
    MONGODB_DATABASE: rawConfig.MONGODB.DATABASE,
    MONGODB_DEBUG: rawConfig.MONGODB.DEBUG === true,
    MONGODB_DISABLED: rawConfig.MONGODB.DISABLED === true,
    AMOUNT: rawConfig.AMOUNT,
    ERROR_HANDLING: rawConfig.ERROR_HANDLING,
    HANDLERS: rawConfig.HANDLERS,
    HANDLERS_DISABLED: rawConfig.HANDLERS.DISABLED,
    HANDLERS_API: rawConfig.HANDLERS.API,
    HANDLERS_API_DISABLED: rawConfig.HANDLERS.API.DISABLED,
    HANDLERS_TIMEOUT: rawConfig.HANDLERS.TIMEOUT,
    HANDLERS_TIMEOUT_DISABLED: rawConfig.HANDLERS.TIMEOUT.DISABLED,
    HANDLERS_TIMEOUT_TIMEXP: rawConfig.HANDLERS.TIMEOUT.TIMEXP,
    HANDLERS_TIMEOUT_TIMEZONE: rawConfig.HANDLERS.TIMEOUT.TIMEZONE,
    CACHE_CONFIG: rawConfig.CACHE,
    PROXY_CACHE_CONFIG: rawConfig.PROXY_CACHE,
    KAFKA_CONFIG: rawConfig.KAFKA,
    PARTICIPANT_INITIAL_POSITION: rawConfig.PARTICIPANT_INITIAL_POSITION,
    RUN_MIGRATIONS: !rawConfig.MIGRATIONS.DISABLED,
    RUN_DATA_MIGRATIONS: rawConfig.MIGRATIONS.RUN_DATA_MIGRATIONS,
    INTERNAL_TRANSFER_VALIDITY_SECONDS: rawConfig.INTERNAL_TRANSFER_VALIDITY_SECONDS,
    ENABLE_ON_US_TRANSFERS: rawConfig.ENABLE_ON_US_TRANSFERS,
    HUB_ID: rawConfig.HUB_PARTICIPANT.ID,
    HUB_NAME: rawConfig.HUB_PARTICIPANT.NAME,
    HUB_ACCOUNTS: rawConfig.HUB_PARTICIPANT.ACCOUNTS,
    INSTRUMENTATION_METRICS_DISABLED: rawConfig.INSTRUMENTATION.METRICS.DISABLED,
    INSTRUMENTATION_METRICS_LABELS: rawConfig.INSTRUMENTATION.METRICS.labels,
    INSTRUMENTATION_METRICS_CONFIG: rawConfig.INSTRUMENTATION.METRICS.config,
    DATABASE: {
      client: rawConfig.DATABASE.DIALECT,
      connection: {
        host: rawConfig.DATABASE.HOST.replace(/\/$/, ''),
        port: rawConfig.DATABASE.PORT,
        user: rawConfig.DATABASE.USER,
        password: rawConfig.DATABASE.PASSWORD,
        database: rawConfig.DATABASE.SCHEMA
      },
      pool: {
        min: rawConfig.DATABASE.POOL_MIN_SIZE,
        max: rawConfig.DATABASE.POOL_MAX_SIZE,
        acquireTimeoutMillis: rawConfig.DATABASE.ACQUIRE_TIMEOUT_MILLIS,
        createTimeoutMillis: rawConfig.DATABASE.CREATE_TIMEOUT_MILLIS,
        destroyTimeoutMillis: rawConfig.DATABASE.DESTROY_TIMEOUT_MILLIS,
        idleTimeoutMillis: rawConfig.DATABASE.IDLE_TIMEOUT_MILLIS,
        reapIntervalMillis: rawConfig.DATABASE.REAP_INTERVAL_MILLIS,
        createRetryIntervalMillis: rawConfig.DATABASE.CREATE_RETRY_INTERVAL_MILLIS
      },
      debug: rawConfig.DATABASE.DEBUG
    },
    API_DOC_ENDPOINTS_ENABLED: defaultTo(rawConfig.API_DOC_ENDPOINTS_ENABLED, false),
    PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED: rawConfig.PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED,
    SERVER_PRINT_ROUTES_ON_STARTUP: defaultTo(rawConfig.SERVER_PRINT_ROUTES_ON_STARTUP, true),
    EXPERIMENTAL: {
      LEDGER: {
        PRIMARY: defaultTo(rawConfig.EXPERIMENTAL?.LEDGER?.PRIMARY, 'SQL'),
        SECONDARY: defaultTo(rawConfig.EXPERIMENTAL?.LEDGER?.SECONDARY, 'NONE'),
        TIGERBEETLE_METADATA_STORE: defaultTo(rawConfig.EXPERIMENTAL?.LEDGER?.TIGERBEETLE_METADATA_STORE, 'SQLITE'),
      },
      TIGERBEETLE: {
        CLUSTER_ID: defaultTo(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.CLUSTER_ID, 0n),
        ADDRESS: defaultTo(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.ADDRESS, ['3000']),
        UNSAFE_SKIP_TIGERBEETLE: defaultTo(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.UNSAFE_SKIP_TIGERBEETLE, false),
        CURRENCY_LEDGERS: defaultTo(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.CURRENCY_LEDGERS, [])
      },
      PROVISIONING: {
        enabled: defaultTo(rawConfig.EXPERIMENTAL?.PROVISIONING?.enabled, false),
        currencies: defaultTo(rawConfig.EXPERIMENTAL?.PROVISIONING?.currencies, []),
        hubAlertEmailAddress: rawConfig.EXPERIMENTAL?.PROVISIONING?.hubAlertEmailAddress,
        settlementModels: defaultTo(rawConfig.EXPERIMENTAL?.PROVISIONING?.settlementModels, []),
        oracles: defaultTo(rawConfig.EXPERIMENTAL?.PROVISIONING?.oracles, []),
      },
      EXTREME_BATCHING: defaultTo(rawConfig.EXPERIMENTAL?.EXTREME_BATCHING, false),
    }
  }
  return unsafeConfig
}

const validateConfig = (unsafeConfig: UnsafeApplicationConfig): ApplicationConfig => {
  assertString(unsafeConfig.HOSTNAME)
  assertNumber(unsafeConfig.PORT)
  assertString(unsafeConfig.MONGODB_HOST)
  assertNumber(unsafeConfig.MONGODB_PORT)
  assertString(unsafeConfig.MONGODB_USER)
  assertString(unsafeConfig.MONGODB_DATABASE)
  assertBoolean(unsafeConfig.MONGODB_DEBUG)
  assertBoolean(unsafeConfig.MONGODB_DISABLED)
  assertNumber(unsafeConfig.AMOUNT.PRECISION)
  assertNumber(unsafeConfig.AMOUNT.SCALE)
  assertBoolean(unsafeConfig.ERROR_HANDLING.includeCauseExtension)
  assertBoolean(unsafeConfig.ERROR_HANDLING.truncateExtensions)
  assertBoolean(unsafeConfig.HANDLERS_DISABLED)
  assertBoolean(unsafeConfig.HANDLERS_API.DISABLED)
  assertBoolean(unsafeConfig.HANDLERS_API_DISABLED)
  assertBoolean(unsafeConfig.HANDLERS_TIMEOUT.DISABLED)
  assertString(unsafeConfig.HANDLERS_TIMEOUT.TIMEXP)
  assertString(unsafeConfig.HANDLERS_TIMEOUT.TIMEZONE)
  assertBoolean(unsafeConfig.HANDLERS_TIMEOUT_DISABLED)
  assertString(unsafeConfig.HANDLERS_TIMEOUT_TIMEXP)
  assertString(unsafeConfig.HANDLERS_TIMEOUT_TIMEZONE)
  assertBoolean(unsafeConfig.CACHE_CONFIG.CACHE_ENABLED)
  assertNumber(unsafeConfig.CACHE_CONFIG.MAX_BYTE_SIZE)
  assertNumber(unsafeConfig.CACHE_CONFIG.EXPIRES_IN_MS)
  assertBoolean(unsafeConfig.PROXY_CACHE_CONFIG.enabled)
  assertString(unsafeConfig.PROXY_CACHE_CONFIG.type)
  assertProxyCacheConfig(unsafeConfig.PROXY_CACHE_CONFIG.proxyConfig)
  assertKafkaConfig(unsafeConfig.KAFKA_CONFIG)
  assertNumber(unsafeConfig.PARTICIPANT_INITIAL_POSITION)
  assertBoolean(unsafeConfig.RUN_MIGRATIONS)
  assertBoolean(unsafeConfig.RUN_DATA_MIGRATIONS)
  assertNumber(unsafeConfig.INTERNAL_TRANSFER_VALIDITY_SECONDS)
  assertBoolean(unsafeConfig.ENABLE_ON_US_TRANSFERS)
  assertNumber(unsafeConfig.HUB_ID)
  assertString(unsafeConfig.HUB_NAME)
  assert.ok(Array.isArray(unsafeConfig.HUB_ACCOUNTS))
  unsafeConfig.HUB_ACCOUNTS.forEach(unsafeAccountStr => assert(unsafeAccountStr))
  assertBoolean(unsafeConfig.INSTRUMENTATION_METRICS_DISABLED)

  assert(unsafeConfig.DATABASE)
  assert(unsafeConfig.DATABASE.connection)
  assertString(unsafeConfig.DATABASE.connection.host)

  // console.warn('TODO(LD): validateConfig() still need to validate `INSTRUMENTATION_METRICS_LABELS`')
  // console.warn('TODO(LD): validateConfig() still need to validate `INSTRUMENTATION_METRICS_CONFIG`')
  // console.warn('TODO(LD): validateConfig() still need to validate `DATABASE`')
  // console.warn('TODO(LD): validateConfig() still need to coerce values from `EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS`')
  
  // TODO: assert INSTRUMENTATION_METRICS_LABELS
  // TODO: assert INSTRUMENTATION_METRICS_CONFIG
  // TODO: assert DATABASE
  assertBoolean(unsafeConfig.API_DOC_ENDPOINTS_ENABLED)
  assertBoolean(unsafeConfig.PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED)
  assertBoolean(unsafeConfig.SERVER_PRINT_ROUTES_ON_STARTUP)

  // Now assert config business logic - apply rules
  assert.ok(unsafeConfig.EXPERIMENTAL.LEDGER.SECONDARY === 'NONE', 'Secondary ledger not implemented')
  assert.equal(unsafeConfig.EXPERIMENTAL.LEDGER.TIGERBEETLE_METADATA_STORE, 'SQLITE', 'Only SQLITE is supported for the metadata store')

  if (unsafeConfig.EXPERIMENTAL.LEDGER.PRIMARY !== 'TIGERBEETLE' 
    && unsafeConfig.EXPERIMENTAL.EXTREME_BATCHING === true ) {
      throw new Error(`EXPERIMENTAL.EXTREME_BATCHING requires EXPERIMENTAL.LEDGER.PRIMARY=TIGERBEETLE`)
  }

  if (unsafeConfig.EXPERIMENTAL.LEDGER.PRIMARY === 'TIGERBEETLE') {
    if (unsafeConfig.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS.length === 0) {
      throw new Error(`EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS must contain at least 1 currency/ledger mapping`)
    }
  }

  // TODO(LD): if and TigerBeetle is enabled, then PROVISIONING.enabled == true 

  return unsafeConfig as ApplicationConfig
}


const printConfigWarnings = (config: ApplicationConfig): void => {

  if (config.EXPERIMENTAL.LEDGER.PRIMARY === 'TIGERBEETLE') {
    console.warn('EXPERIMENTAL.LEDGER.PRIMARY = TIGERBEETLE. This ledger is currently a work in progress.')
  }

  if (config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE === true) {
    console.warn('EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE = true. This is an unsafe option for performance debugging purposes only')
  }

}

const makeConfig = (): ApplicationConfig => {
  const PATH_TO_CONFIG_FILE = defaultEnvString('PATH_TO_CONFIG_FILE', path.join(__dirname, '../../..', 'config/default.json'))
  logger.warn(`makeConfig() - loading config from: ${PATH_TO_CONFIG_FILE}`)
  const raw = parseStringsInObject(RC('CLEDG', require(PATH_TO_CONFIG_FILE)))
  const resolved = resolveConfig(raw)
  const validated = validateConfig(resolved)


  printConfigWarnings(validated)


  return validated
}


export { makeConfig, resolveConfig, validateConfig }
