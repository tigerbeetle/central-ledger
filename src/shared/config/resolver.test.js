"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_test_1 = require("node:test");
const node_assert_1 = __importDefault(require("node:assert"));
const resolver_1 = require("./resolver");
(0, node_test_1.describe)('config/resolver', () => {
    (0, node_test_1.it)('loads the config', () => {
        // Arrange
        // Act
        const config = (0, resolver_1.makeConfig)();
        // Assert
        // rules are asserted internally
        node_assert_1.default.ok(config);
    });
});
