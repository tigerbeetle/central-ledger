"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateConfig = exports.resolveConfig = exports.makeConfig = void 0;
const node_path_1 = __importDefault(require("node:path"));
const parse_strings_in_object_1 = __importDefault(require("parse-strings-in-object"));
const rc_1 = __importDefault(require("rc"));
const util_1 = require("./util");
const node_assert_1 = __importDefault(require("node:assert"));
const resolveConfig = (rawConfig) => {
    const unsafeConfig = {
        HOSTNAME: rawConfig.HOSTNAME.replace(/\/$/, ''),
        PORT: rawConfig.PORT,
        MAX_FULFIL_TIMEOUT_DURATION_SECONDS: (0, util_1.defaultTo)(rawConfig.MAX_FULFIL_TIMEOUT_DURATION_SECONDS, 300),
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
        API_DOC_ENDPOINTS_ENABLED: (0, util_1.defaultTo)(rawConfig.API_DOC_ENDPOINTS_ENABLED, false),
        PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED: rawConfig.PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED,
        SERVER_PRINT_ROUTES_ON_STARTUP: (0, util_1.defaultTo)(rawConfig.SERVER_PRINT_ROUTES_ON_STARTUP, true),
        EXPERIMENTAL: {
            LEDGER: {
                PRIMARY: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.LEDGER?.PRIMARY, 'SQL'),
                SECONDARY: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.LEDGER?.SECONDARY, 'NONE'),
                TIGERBEETLE_METADATA_STORE: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.LEDGER?.TIGERBEETLE_METADATA_STORE, 'SQLITE'),
            },
            TIGERBEETLE: {
                ADDRESS: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.ADDRESS, '3000'),
                UNSAFE_SKIP_TIGERBEETLE: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.UNSAFE_SKIP_TIGERBEETLE, false),
                CURRENCY_LEDGERS: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.TIGERBEETLE?.CURRENCY_LEDGERS, [])
            },
            PROVISIONING: {
                enabled: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.PROVISIONING?.enabled, false),
                currencies: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.PROVISIONING?.currencies, []),
                hubAlertEmailAddress: rawConfig.EXPERIMENTAL?.PROVISIONING?.hubAlertEmailAddress,
                settlementModels: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.PROVISIONING?.settlementModels, []),
                oracles: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.PROVISIONING?.oracles, []),
            },
            EXTREME_BATCHING: (0, util_1.defaultTo)(rawConfig.EXPERIMENTAL?.EXTREME_BATCHING, false),
        }
    };
    return unsafeConfig;
};
exports.resolveConfig = resolveConfig;
const validateConfig = (unsafeConfig) => {
    (0, util_1.assertString)(unsafeConfig.HOSTNAME);
    (0, util_1.assertNumber)(unsafeConfig.PORT);
    (0, util_1.assertString)(unsafeConfig.MONGODB_HOST);
    (0, util_1.assertNumber)(unsafeConfig.MONGODB_PORT);
    (0, util_1.assertString)(unsafeConfig.MONGODB_USER);
    (0, util_1.assertString)(unsafeConfig.MONGODB_DATABASE);
    (0, util_1.assertBoolean)(unsafeConfig.MONGODB_DEBUG);
    (0, util_1.assertBoolean)(unsafeConfig.MONGODB_DISABLED);
    (0, util_1.assertNumber)(unsafeConfig.AMOUNT.PRECISION);
    (0, util_1.assertNumber)(unsafeConfig.AMOUNT.SCALE);
    (0, util_1.assertBoolean)(unsafeConfig.ERROR_HANDLING.includeCauseExtension);
    (0, util_1.assertBoolean)(unsafeConfig.ERROR_HANDLING.truncateExtensions);
    (0, util_1.assertBoolean)(unsafeConfig.HANDLERS_DISABLED);
    (0, util_1.assertBoolean)(unsafeConfig.HANDLERS_API.DISABLED);
    (0, util_1.assertBoolean)(unsafeConfig.HANDLERS_API_DISABLED);
    (0, util_1.assertBoolean)(unsafeConfig.HANDLERS_TIMEOUT.DISABLED);
    (0, util_1.assertString)(unsafeConfig.HANDLERS_TIMEOUT.TIMEXP);
    (0, util_1.assertString)(unsafeConfig.HANDLERS_TIMEOUT.TIMEZONE);
    (0, util_1.assertBoolean)(unsafeConfig.HANDLERS_TIMEOUT_DISABLED);
    (0, util_1.assertString)(unsafeConfig.HANDLERS_TIMEOUT_TIMEXP);
    (0, util_1.assertString)(unsafeConfig.HANDLERS_TIMEOUT_TIMEZONE);
    (0, util_1.assertBoolean)(unsafeConfig.CACHE_CONFIG.CACHE_ENABLED);
    (0, util_1.assertNumber)(unsafeConfig.CACHE_CONFIG.MAX_BYTE_SIZE);
    (0, util_1.assertNumber)(unsafeConfig.CACHE_CONFIG.EXPIRES_IN_MS);
    (0, util_1.assertBoolean)(unsafeConfig.PROXY_CACHE_CONFIG.enabled);
    (0, util_1.assertString)(unsafeConfig.PROXY_CACHE_CONFIG.type);
    (0, util_1.assertProxyCacheConfig)(unsafeConfig.PROXY_CACHE_CONFIG.proxyConfig);
    (0, util_1.assertKafkaConfig)(unsafeConfig.KAFKA_CONFIG);
    (0, util_1.assertNumber)(unsafeConfig.PARTICIPANT_INITIAL_POSITION);
    (0, util_1.assertBoolean)(unsafeConfig.RUN_MIGRATIONS);
    (0, util_1.assertBoolean)(unsafeConfig.RUN_DATA_MIGRATIONS);
    (0, util_1.assertNumber)(unsafeConfig.INTERNAL_TRANSFER_VALIDITY_SECONDS);
    (0, util_1.assertBoolean)(unsafeConfig.ENABLE_ON_US_TRANSFERS);
    (0, util_1.assertNumber)(unsafeConfig.HUB_ID);
    (0, util_1.assertString)(unsafeConfig.HUB_NAME);
    node_assert_1.default.ok(Array.isArray(unsafeConfig.HUB_ACCOUNTS));
    unsafeConfig.HUB_ACCOUNTS.forEach(unsafeAccountStr => (0, node_assert_1.default)(unsafeAccountStr));
    (0, util_1.assertBoolean)(unsafeConfig.INSTRUMENTATION_METRICS_DISABLED);
    console.warn('TODO(LD): validateConfig() still need to validate `INSTRUMENTATION_METRICS_LABELS`');
    console.warn('TODO(LD): validateConfig() still need to validate `INSTRUMENTATION_METRICS_CONFIG`');
    console.warn('TODO(LD): validateConfig() still need to validate `DATABASE`');
    console.warn('TODO(LD): validateConfig() still need to coerce values from `EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS`');
    // TODO: assert INSTRUMENTATION_METRICS_LABELS
    // TODO: assert INSTRUMENTATION_METRICS_CONFIG
    // TODO: assert DATABASE
    (0, util_1.assertBoolean)(unsafeConfig.API_DOC_ENDPOINTS_ENABLED);
    (0, util_1.assertBoolean)(unsafeConfig.PAYEE_PARTICIPANT_CURRENCY_VALIDATION_ENABLED);
    (0, util_1.assertBoolean)(unsafeConfig.SERVER_PRINT_ROUTES_ON_STARTUP);
    // Now assert config business logic - apply rules
    node_assert_1.default.ok(unsafeConfig.EXPERIMENTAL.LEDGER.SECONDARY === 'NONE', 'Secondary ledger not implemented');
    node_assert_1.default.equal(unsafeConfig.EXPERIMENTAL.LEDGER.TIGERBEETLE_METADATA_STORE, 'SQLITE', 'Only SQLITE is supported for the metadata store');
    if (unsafeConfig.EXPERIMENTAL.LEDGER.PRIMARY !== 'TIGERBEETLE'
        && unsafeConfig.EXPERIMENTAL.EXTREME_BATCHING === true) {
        throw new Error(`EXPERIMENTAL.EXTREME_BATCHING requires EXPERIMENTAL.LEDGER.PRIMARY=TIGERBEETLE`);
    }
    if (unsafeConfig.EXPERIMENTAL.LEDGER.PRIMARY === 'TIGERBEETLE') {
        if (unsafeConfig.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS.length === 0) {
            throw new Error(`EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS must contain at least 1 currency/ledger mapping`);
        }
    }
    // TODO(LD): if and TigerBeetle is enabled, then PROVISIONING.enabled == true 
    return unsafeConfig;
};
exports.validateConfig = validateConfig;
const printConfigWarnings = (config) => {
    if (config.EXPERIMENTAL.LEDGER.PRIMARY === 'TIGERBEETLE') {
        console.warn('EXPERIMENTAL.LEDGER.PRIMARY = TIGERBEETLE. This ledger is currently a work in progress.');
    }
    if (config.EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE === true) {
        console.warn('EXPERIMENTAL.TIGERBEETLE.UNSAFE_SKIP_TIGERBEETLE = true. This is an unsafe option for performance debugging purposes only');
    }
};
const makeConfig = () => {
    const PATH_TO_CONFIG = (0, util_1.defaultEnvString)('PATH_TO_CONFIG', node_path_1.default.join(__dirname, '../../..', 'config/default.json'));
    const raw = (0, parse_strings_in_object_1.default)((0, rc_1.default)('CLEDG', require(PATH_TO_CONFIG)));
    const resolved = resolveConfig(raw);
    const validated = validateConfig(resolved);
    printConfigWarnings(validated);
    return validated;
};
exports.makeConfig = makeConfig;
