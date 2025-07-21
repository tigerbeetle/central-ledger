"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.defaultEnvString = exports.stringToBoolean = exports.defaultTo = exports.assertKafkaConfig = exports.assertProxyCacheConfig = exports.assertStringOrNull = exports.assertBoolean = exports.assertNumber = exports.assertString = void 0;
const assert_1 = __importDefault(require("assert"));
class ConfigValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = 'ConfigValidationError';
    }
}
const assertString = (input) => {
    if (typeof input !== 'string') {
        throw new ConfigValidationError(`assertString() expected 'string', instead found ${typeof input}`);
    }
};
exports.assertString = assertString;
const assertNumber = (input) => {
    if (typeof input !== 'number') {
        throw new ConfigValidationError(`assertNumber() expected 'number', instead found ${typeof input}`);
    }
    if (isNaN(input)) {
        throw new ConfigValidationError(`assertNumber() expected 'number', instead found NaN`);
    }
};
exports.assertNumber = assertNumber;
const assertBoolean = (input) => {
    if (typeof input !== 'boolean') {
        throw new ConfigValidationError(`assertNumber() expected 'boolean', instead found ${typeof input}`);
    }
};
exports.assertBoolean = assertBoolean;
const assertStringOrNull = (input) => {
    if (input === null) {
        return;
    }
    return (0, exports.assertString)(input);
};
exports.assertStringOrNull = assertStringOrNull;
const assertProxyCacheConfig = (input) => {
    try {
        (0, assert_1.default)(input);
        // We could add more validation here, or simply rely on the `@mojaloop/inter-scheme-proxy-cache-lib`
        // to handle it
    }
    catch (err) {
        throw new ConfigValidationError(err.message);
    }
};
exports.assertProxyCacheConfig = assertProxyCacheConfig;
const assertKafkaConfig = (input) => {
    const assertKafkaConsumerConfig = (inputConsumer) => {
        const unsafeConsumerConfig = inputConsumer;
        (0, assert_1.default)(unsafeConsumerConfig);
        (0, assert_1.default)(unsafeConsumerConfig.config);
        // We could do some more asserting here, but this is a good start
        (0, assert_1.default)(unsafeConsumerConfig.config.options);
        (0, assert_1.default)(unsafeConsumerConfig.config.rdkafkaConf);
        (0, assert_1.default)(unsafeConsumerConfig.config.topicConf);
    };
    const assertKafkaProducerConfig = (inputConsumer) => {
        const unsafeProducerConfig = inputConsumer;
        (0, assert_1.default)(unsafeProducerConfig);
        (0, assert_1.default)(unsafeProducerConfig.config);
        // We could do some more asserting here, but this is a good start
        (0, assert_1.default)(unsafeProducerConfig.config.options);
        (0, assert_1.default)(unsafeProducerConfig.config.rdkafkaConf);
        (0, assert_1.default)(unsafeProducerConfig.config.topicConf);
    };
    // Check the `EVENT_TYPE_ACTION_TOPIC_MAP`
    const unsafeKafkaConfig = input;
    if (!unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP) {
        throw new ConfigValidationError(`missing EVENT_TYPE_ACTION_TOPIC_MAP`);
    }
    if (!unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION) {
        throw new ConfigValidationError(`missing EVENT_TYPE_ACTION_TOPIC_MAP.POSITION`);
    }
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.PREPARE);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_PREPARE);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.BULK_PREPARE);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.COMMIT);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.BULK_COMMIT);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.RESERVE);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_RESERVE);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.TIMEOUT_RESERVED);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_TIMEOUT_RESERVED);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.ABORT);
    (0, exports.assertStringOrNull)(unsafeKafkaConfig.EVENT_TYPE_ACTION_TOPIC_MAP.POSITION.FX_ABORT);
    // Check the `TOPIC_TEMPLATES`
    if (!unsafeKafkaConfig.TOPIC_TEMPLATES) {
        throw new ConfigValidationError(`missing TOPIC_TEMPLATES`);
    }
    if (!unsafeKafkaConfig.TOPIC_TEMPLATES.PARTICIPANT_TOPIC_TEMPLATE) {
        throw new ConfigValidationError(`missing TOPIC_TEMPLATES.PARTICIPANT_TOPIC_TEMPLATE`);
    }
    if (!unsafeKafkaConfig.TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE) {
        throw new ConfigValidationError(`missing TOPIC_TEMPLATES.GENERAL_TOPIC_TEMPLATE`);
    }
    // Check the Consumer Configs
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.BULK);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.BULK.PREPARE);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.BULK.PROCESSING);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.BULK.FULFIL);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.BULK.GET);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.TRANSFER);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.TRANSFER.PREPARE);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.TRANSFER.GET);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.TRANSFER.FULFIL);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION_BATCH);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.ADMIN);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.ADMIN.TRANSFER);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.NOTIFICATION);
    (0, assert_1.default)(unsafeKafkaConfig.CONSUMER.NOTIFICATION.EVENT);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.PREPARE);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.PROCESSING);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.FULFIL);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.BULK.GET);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.PREPARE);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.GET);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.FULFIL);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.TRANSFER.POSITION_BATCH);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.ADMIN.TRANSFER);
    assertKafkaConsumerConfig(unsafeKafkaConfig.CONSUMER.NOTIFICATION.EVENT);
    // Check the Producer Configs
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.BULK);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.BULK.PROCESSING);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.TRANSFER);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.TRANSFER.PREPARE);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.TRANSFER.FULFIL);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.TRANSFER.POSITION);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.NOTIFICATION);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.NOTIFICATION.EVENT);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.ADMIN);
    (0, assert_1.default)(unsafeKafkaConfig.PRODUCER.ADMIN.TRANSFER);
    assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.BULK.PROCESSING);
    assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.TRANSFER.PREPARE);
    assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.TRANSFER.FULFIL);
    assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.TRANSFER.POSITION);
    assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.NOTIFICATION.EVENT);
    assertKafkaProducerConfig(unsafeKafkaConfig.PRODUCER.ADMIN.TRANSFER);
};
exports.assertKafkaConfig = assertKafkaConfig;
const defaultTo = (input, defaultValue) => {
    if (input === undefined) {
        return defaultValue;
    }
    assert_1.default.equal(typeof input, typeof defaultValue);
    return input;
};
exports.defaultTo = defaultTo;
const stringToBoolean = (input) => {
    (0, assert_1.default)(input !== undefined);
    (0, assert_1.default)(typeof input === 'string');
    switch (input.toLowerCase()) {
        case 'true': return true;
        case 'false': return false;
        default: {
            throw new Error(`stringToBoolean, unknown input: ${input}`);
        }
    }
};
exports.stringToBoolean = stringToBoolean;
const defaultEnvString = (envName, defaultValue) => {
    (0, assert_1.default)(defaultValue, 'expected a default value');
    let processEnvValue = process.env[envName];
    // need to protect for cases where the value may intentionally false!
    if (processEnvValue === undefined) {
        return defaultValue;
    }
    if (Array.isArray(processEnvValue)) {
        processEnvValue = processEnvValue[0];
    }
    if (processEnvValue === undefined) {
        return defaultValue;
    }
    return processEnvValue;
};
exports.defaultEnvString = defaultEnvString;
