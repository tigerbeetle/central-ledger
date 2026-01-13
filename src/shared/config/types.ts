export type LedgerType = 'TIGERBEETLE' | 'LEGACY';

interface KafkaTopicTemplate {
  TEMPLATE: string,
  REGEX: string
}

/**
 * These types were implied from the examples in `default.json`, so they may not be complete
 * I've decided to keep this simple for now, but if we need more complexity later on we can always
 * add it.
 */

export interface KafkaConsumerConfig {
  config: {
    options: KafkaConsumerGeneralOptions,
    rdkafkaConf: KafkaConsumerRdKafkaConfig,
    topicConf: KafkaConsumerTopicConfig
  }
}

export interface KafkaProducerConfig {
  config: {
    options: KafkaProducerGeneralOptions,
    rdkafkaConf: KafkaProducerRdKafkaConfig,
    topicConf: KafkaProducerTopicConfig
  }
}

interface KafkaConsumerGeneralOptions {
  mode: 0 | 1 | 2,
  batchSize: number,
  pollFrequency: number,
  recursiveTimeout: number,
  messageCharset: string
  messageAsJSON: boolean,
  sync: boolean
  consumeTimeout: number
}

interface KafkaProducerGeneralOptions {
  mode: 0 | 1 | 2,
  batchSize: number,
  pollFrequency: number,
  recursiveTimeout: number,
  messageCharset: string
  messageAsJSON: boolean,
  sync: boolean
  consumeTimeout: number
}

interface KafkaConsumerRdKafkaConfig {
  "metadata.broker.list": string,
  "client.id": string,
  "socket.keepalive.enable": true,
  "group.id": string
  "allow.auto.create.topics": true,
}

interface KafkaProducerRdKafkaConfig {
  "metadata.broker.list": string,
  "client.id": string,
  "socket.keepalive.enable": true,
  "event_cb": true,
  "dr_cb": true,
  "queue.buffering.max.messages": number
}

interface KafkaConsumerTopicConfig {
  'auto.offset.reset': string,
}

interface KafkaProducerTopicConfig {
  'request.required.acks': string,
  'partitioner': string,
}

export interface KafkaConfig {
  EVENT_TYPE_ACTION_TOPIC_MAP: {
    POSITION: {
      PREPARE: string | null,
      FX_PREPARE: string | null,
      BULK_PREPARE: string | null,
      COMMIT: string | null,
      BULK_COMMIT: string | null,
      RESERVE: string | null,
      FX_RESERVE: string | null,
      TIMEOUT_RESERVED: string | null,
      FX_TIMEOUT_RESERVED: string | null,
      ABORT: string | null,
      FX_ABORT: string | null,
    }
  },
  TOPIC_TEMPLATES: {
    PARTICIPANT_TOPIC_TEMPLATE: KafkaTopicTemplate,
    GENERAL_TOPIC_TEMPLATE: KafkaTopicTemplate,
  },
  CONSUMER: {
    BULK: {
      PREPARE: KafkaConsumerConfig,
      PROCESSING: KafkaConsumerConfig,
      FULFIL: KafkaConsumerConfig,
      GET: KafkaConsumerConfig,
    },
    TRANSFER: {
      PREPARE: KafkaConsumerConfig,
      GET: KafkaConsumerConfig,
      FULFIL: KafkaConsumerConfig,
      POSITION: KafkaConsumerConfig,
      POSITION_BATCH: KafkaConsumerConfig,
    },
    ADMIN: {
      TRANSFER: KafkaConsumerConfig
    },
    NOTIFICATION: {
      EVENT: KafkaConsumerConfig
    }
  },
  PRODUCER: {
    BULK: {
      PROCESSING: KafkaProducerConfig
    }
    TRANSFER: {
      PREPARE: KafkaProducerConfig,
      FULFIL: KafkaProducerConfig,
      POSITION: KafkaProducerConfig,
    },
    NOTIFICATION: {
      EVENT: KafkaProducerConfig,
    },
    ADMIN: {
      TRANSFER: KafkaProducerConfig
    }
  }
}

import { ProxyCacheConfig } from '@mojaloop/inter-scheme-proxy-cache-lib'

export interface InstrumentationConfig {
  METRICS: {
    DISABLED: boolean,
    labels: any,
    config: {
      timeout: number,
      prefix: string,
      defaultLabels: {
        serviceName: string
      }
    }
  }
}

export interface DatabaseConfig {
  client: string,
  connection: {
    host: string,
    port: number,
    user: string,
    password: string,
    database: string,
  },
  pool: {
    // minimum size
    min: number,
    // maximum size
    max: number
    // acquire promises are rejected after this many milliseconds
    // if a resource cannot be acquired
    acquireTimeoutMillis: number
    // create operations are cancelled after this many milliseconds
    // if a resource cannot be acquired
    createTimeoutMillis: number
    // destroy operations are awaited for at most this many milliseconds
    // new resources will be created after this timeout
    destroyTimeoutMillis: number
    // free resouces are destroyed after this many milliseconds
    idleTimeoutMillis: number
    // how often to check for idle resources to destroy
    reapIntervalMillis: number
    // long long to idle after failed create before trying again
    createRetryIntervalMillis: number
  }
  debug: unknown
}

/**
 * Defines a Currency in the TigerBeetle Ledger
 */
export interface CurrencyLedgerConfig {
  /**
   * ISO currency code
   */
  currency: string,

  /**
   * TigerBeetle stores values as bigints, assetScale allows us to define the precision of the
   * integer value of funds in the ledger.
   * 
   * For example, representing USD in cents uses an asset scale of 2.
   * 
   * Valid ranges: [-7, 8]
   */
  assetScale: number,

  /**
   * Operation Ledger for balance-sheet accounts and transfers
   * Valid ranges: [1001 - 1100]
   */
  ledgerOperation: number,

  /**
   * Control Ledger for non-balance sheet accounts and transfers
   * Valid ranges: [2001 - 2100]
   */
  ledgerControl: number,

  /**
   * Ids for currency-specific accounts 
   */
  accountIdSettlementBalance: bigint,


  /**
   * The LedgerId to use for the clearing side of the ledger
   * @deprecated

   * 
   * TODO: I'm not sure if we need to separate out the ledgerIds at this stage, 
   *   but it can't hurt
   */
  clearingLedgerId: number,

  /**
   * The LedgerId to use for the settlement side of the ledger
   * @deprecated
   * 
   * TODO: I'm not sure if we need to separate out the ledgerIds at this stage, 
   *   but it can't hurt
   */
  settlementLedgerId: number

  /**
   * The LedgerId to use for non-real assets or liabilities, but relating
   * to the currency.
   */
  controlLedgerId: number
}

/**
 * @interface ApplicationConfig
 * @description Root config for central-ledger
 */
export interface ApplicationConfig {
  HOSTNAME: string,
  /**
   * The port number for the server to listen on.
   * Defaults to 3001
   */
  PORT: number,
  MAX_FULFIL_TIMEOUT_DURATION_SECONDS: number,
  // TODO: these should be nested under a MONGO key
  MONGODB_HOST: string,
  MONGODB_PORT: number,
  MONGODB_USER: string,
  MONGODB_PASSWORD: string,
  MONGODB_DATABASE: string,
  MONGODB_DEBUG: boolean,
  MONGODB_DISABLED: boolean,
  AMOUNT: {
    PRECISION: number,
    SCALE: number,
  },
  ERROR_HANDLING: {
    includeCauseExtension: boolean,
    truncateExtensions: boolean,
  },
  HANDLERS: {
    // TODO:
    DISABLED: boolean
  },
  HANDLERS_DISABLED: boolean
  HANDLERS_API: {
    DISABLED: boolean,
  },
  HANDLERS_API_DISABLED: boolean,
  HANDLERS_TIMEOUT: {
    DIST_LOCK: {
      distLockKey: string,
      lockTimeout: number,
      acquireTimeout: number,
    }
    DISABLED: boolean,
    TIMEXP: string,
    TIMEZONE: string,
  },
  HANDLERS_TIMEOUT_DISABLED: boolean,
  HANDLERS_TIMEOUT_TIMEXP: string,
  HANDLERS_TIMEOUT_TIMEZONE: string,
  // TODO: rename to just CACHE - we already know it's a config!
  CACHE_CONFIG: {
    // TODO: align between ENABLED and DISABLED configs
    CACHE_ENABLED: boolean
    MAX_BYTE_SIZE: number,
    EXPIRES_IN_MS: number,
  },
  // TODO: rename to just PROXY_CACHE - we already know it's a config!
  PROXY_CACHE_CONFIG: {
    enabled: boolean,
    type: string,
    proxyConfig: ProxyCacheConfig
  },
  // TODO: rename to just KAFKA - we already know it's a config!
  KAFKA_CONFIG: KafkaConfig,
  PARTICIPANT_INITIAL_POSITION: number,
  // TODO: keep this as migrations for consistency
  RUN_MIGRATIONS: boolean,
  RUN_DATA_MIGRATIONS: boolean,
  INTERNAL_TRANSFER_VALIDITY_SECONDS: number,
  ENABLE_ON_US_TRANSFERS: boolean,
  HUB_ID: number,
  HUB_NAME: string,
  HUB_ACCOUNTS: Array<string>,
  INSTRUMENTATION_METRICS_DISABLED: boolean,
  // TODO: not sure what this should look like
  INSTRUMENTATION_METRICS_LABELS: any
  // TODO: rename to just INSTRUMENTATION - keep consistency between default.json and this config
  INSTRUMENTATION_METRICS_CONFIG: InstrumentationConfig,
  DATABASE: DatabaseConfig,
  API_DOC_ENDPOINTS_ENABLED: boolean,
  PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED: boolean,

  /**
   * Controls whether or not to print the routes when the server is started
   * @default true
   */
  SERVER_PRINT_ROUTES_ON_STARTUP: boolean,

  /**
   * Experimental Configs. Not ready for prime time just yet.
   */
  EXPERIMENTAL: {
    LEDGER: {
      /**
       * Configures the underlying primary ledger.
       * - `SQL` uses the existing MySQL central-ledger implementation
       * - `TIGERBEETLE` uses the TigerBeetle OLTP Database
       * 
       * @default 'SQL'
       */
      PRIMARY: 'SQL' | 'TIGERBEETLE'

      /**
       * Configure a secondary ledger. If configured, the secondary ledger runs alongside the 
       * primary ledger. 
       * - When PRIMARY is SQL, SECONDARY MUST be either NONE or TIGERBEETLE
       * - When PRIMARY is TIGERBEETLE, SECONDARY MUST be either SQL or NONE
       * 
       * @default 'NONE'
       */
      SECONDARY: 'SQL' | 'TIGERBEETLE' | 'NONE',

      /**
       * What Database to use to store the Ledger Metadata when the TigerBeetle ledger is enabled
       * 
       * In development, only sqlite is supported, but we'll likely shift this to MySQL 
       * shortly
       * 
       * @default 'SQLITE'
       */
      TIGERBEETLE_METADATA_STORE: 'SQLITE'
    },

    TIGERBEETLE: {

      /**
      * The TigerBeetle cluster id
      * See: https://docs.tigerbeetle.com/coding/clients/node/#creating-a-client
      * 
      * @default 0n
      */
      CLUSTER_ID: bigint

      /**
       * The TigerBeetle Address String
       * See: https://docs.tigerbeetle.com/coding/clients/node/#creating-a-client
       * 
       * @default ['3000']
       */
      ADDRESS: Array<string>,

      /**
       * When using the TIGERBEETLE Ledger, setting this option to true will skip writing to
       * TigerBeetle completely.
       * 
       * @default false
       */
      UNSAFE_SKIP_TIGERBEETLE: boolean,

      /**
       * CURRENCY_LEDGERS
       * 
       * This determines the mapping between the currency + ledgerId in TigerBeetle.
       * 
       * IMPORTANT: Once a Currency + LedgerId is defined, it cannot be changed. We run a saftey 
       *   check upon startup, to detect changes to this option and shut down in case it was
       *   modified
       */
      CURRENCY_LEDGERS: Array<CurrencyLedgerConfig>
    },


    /**
     * When enabled, the switch will automatically provision itself on startup.
     * When running with TigerBeetle, PROVISIONING MUST be enabled
     */
    PROVISIONING: {
      /**
       * @default false
       */
      enabled: boolean,

      /**
       * A list of ISO Currency codes that the switch supports.
       */
      currencies: Array<string>,

      /**
       * Which email address to register for the following hub alerts:
       * - `SETTLEMENT_TRANSFER_POSITION_CHANGE_EMAIL`
       * - `NET_DEBIT_CAP_ADJUSTMENT_EMAIL`
       * - `NET_DEBIT_CAP_THRESHOLD_BREACH_EMAIL`
       */
      hubAlertEmailAddress: string | undefined,

      /**
       * TODO(LD): define these settlement models. I think this should be a 1-1 mapping
       * between currency for now, but we may choose to add some specificity, e.g. a 
       * mapping between (currency, paymentType) => Settlement Model
       */
      settlementModels: [],

      /**
       * TODO: I'm not sure if oracles should live here, but I did see it in the 
       * Testing Toolkit Provisioning tests, so let's keep it around until we decide
       * it may be better off somewhere else.
       */
      oracles: []
    }

    /**
     * Extreme Batching joins together multiple ml-api-adapter messages into a single
     * kafka payload, amortizing the cost of writes to disk
     * 
     * For EXTREME_BATCHING=true:
     * - EXPERIMENTAL.LEDGER.PRIMARY = `TIGERBEETLE`
     * - EXPERIMENTAL.LEDGER.SECONDARY = `NONE` (extreme batching is not supported for the SQL ledger)
     * - All Kafka consumer and producer batch sizes must be set to 1
     * - ml-api-adapter must also have EXTREME_BATCHING enabled
     * 
     * 
     *  * @default false
     */
    EXTREME_BATCHING: boolean,



  }

}