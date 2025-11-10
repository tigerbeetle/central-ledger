import { after, before, describe, it } from 'node:test';
import TigerBeetleLedger from '../../domain/ledger-v2/TigerBeetleLedger';
import { makeConfig } from '../../shared/config/resolver';
import { logger } from '../../shared/logger';
import { HarnessApi, HarnessApiConfig } from '../../testing/harness/harness-api';

import Db from '../../lib/db';
import { TestUtils } from '../../testing/testutils';

import * as ParticipantHandler from './handler'
import assert from 'assert';
import path from 'path';

describe('api/participants/handler', () => {
  let harnessApi: HarnessApi
  let ledger: TigerBeetleLedger

  before(async () => {
    try {
      const projectRoot = path.join(__dirname, '../../..')
      const config: HarnessApiConfig = {
        databaseConfig: {
          databaseName: 'central_ledger_test',
          mysqlImage: 'mysql:8.0',
          memorySize: '256m',
          port: 3307,
          // migration: { type: 'knex', updateSqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint1.sql') }
          migration: { type: 'sql', sqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql') }
        },
        tigerBeetleConfig: {
          tigerbeetleBinaryPath: path.join(projectRoot, '../../', '.bin/tigerbeetle')
          // tigerbeetleBinaryPath: '/Users/lewisdaly/tb/tigerloop/.bin/tigerbeetle'

        },
        applicationConfig: makeConfig()
      }
      const participantService = require('../../domain/participant');
      harnessApi = new HarnessApi(config, Db, participantService);

      const harnessApiResult = await harnessApi.start()
      ledger = harnessApiResult.ledger

    } catch (err) {
      logger.error(`before() - failed with error: ${err.message}`)
      if (err.stack) {
        logger.error(err.stack)
      }
      await harnessApi.teardown()
    }
  })

  after(async () => {
    await harnessApi.teardown()
  })

  describe('GET  /participants', () => {
    it('Lists information about all participants', async () => {
      // Arrange
      const request = {
        payload: {},
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
       const {
        code, body
      } = await TestUtils.unwrapHapiResponse(reply => ParticipantHandler.getAll(request))

      // Assert
      assert.equal(code, 200)

    })
  })
  describe('POST /participants')
  describe('GET  /participants/limits')
  describe('GET  /participants/{name}')
  describe('PUT  /participants/{name}')
  describe('GET  /participants/{name}/endpoints')
  describe('POST /participants/{name}/endpoints')
  describe('GET  /participants/{name}/limits')
  describe('PUT  /participants/{name}/limits')
  describe('GET  /participants/{name}/positions')
  describe('GET  /participants/{name}/accounts')
  describe('PUT  /participants/{name}/accounts')
  describe('PUT  /participants/{name}/accounts/{id}')
  describe('POST /participants/{name}/accounts/{id}')
  describe('POST /participants/{name}/accounts/{id}/transfers/{id}')
  describe('POST /participants/{name}/initialPositionAndLimits')


})