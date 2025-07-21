'use strict';
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const config_1 = __importDefault(require("../shared/config"));
const routes_1 = __importDefault(require("./routes"));
const setup_1 = require("../shared/setup");
const central_services_metrics_1 = require("@mojaloop/central-services-metrics");
const server = {
    run: () => {
        return (0, setup_1.initialize)({
            service: 'api',
            port: config_1.default.PORT,
            modules: [routes_1.default, !config_1.default.INSTRUMENTATION_METRICS_DISABLED && central_services_metrics_1.plugin].filter(Boolean),
            runMigrations: config_1.default.RUN_MIGRATIONS,
            runHandlers: !config_1.default.HANDLERS_DISABLED,
        });
    }
};
exports.default = server;
