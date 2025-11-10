import assert from 'assert';
import { after, before, describe, it } from 'node:test';
import path from 'path';
import { Ledger } from '../../domain/ledger-v2/Ledger';
import Db from '../../lib/db';
import { makeConfig } from '../../shared/config/resolver';
import { logger } from '../../shared/logger';
import { initializeCache } from '../../shared/setup-new';
import { HarnessApi, HarnessApiConfig } from '../../testing/harness/harness-api';
import { TestUtils } from '../../testing/testutils';
import * as ParticipantHandler from './handler';
import { checkSnapshotObject, unwrapSnapshot } from '../../testing/snapshot';

describe('api/participants/handler', () => {
  let harnessApi: HarnessApi
  let ledger: Ledger

  before(async () => {
    try {
      const projectRoot = path.join(__dirname, '../../..')
      const config: HarnessApiConfig = {
        databaseConfig: {
          databaseName: 'central_ledger_test',
          mysqlImage: 'mysql:8.0',
          memorySize: '256m',
          port: 3307,
          migration: { type: 'sql', sqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql') }
        },
        tigerBeetleConfig: {
          tigerbeetleBinaryPath: path.join(projectRoot, '../../', '.bin/tigerbeetle')
        },
        applicationConfig: makeConfig()
      }
      // TODO: hopefully we can remove this at some point
      const participantService = require('../../domain/participant');
      harnessApi = new HarnessApi(config, Db, participantService);

      const harnessApiResult = await harnessApi.start()
      // Annoying global that we seem to need to call
      await initializeCache()
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

  describe('GET /participants', () => {
    it('01 Returns the hub information', async () => {
      // Arrange
      const request = {
        query: {
          isProxy: false
        },
        payload: {},
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const result = await ParticipantHandler.getAll(request)

      // Assert
      assert(result, 'Expected a response from getAll()')
      const snapshot = [
        {
          name: 'Hub',
          id: 'http://central-ledger/participants/Hub',
          "created:ignore": true,
          isActive: 1,
          links: { self: 'http://central-ledger/participants/Hub' },
          accounts: [
            {
              createdBy: "unknown",
              createdDate: null,
              currency: "USD",
              id: 1,
              isActive: 1,
              ledgerAccountType: "HUB_MULTILATERAL_SETTLEMENT"
            },
            {
              createdBy: "unknown",
              createdDate: null,
              currency: "USD",
              id: 2,
              isActive: 1,
              ledgerAccountType: "HUB_RECONCILIATION"
            },
            {
              createdBy: "unknown",
              createdDate: null,
              currency: "KES",
              id: 3,
              isActive: 1,
              ledgerAccountType: "HUB_MULTILATERAL_SETTLEMENT"
            },
            {
              createdBy: "unknown",
              createdDate: null,
              currency: "KES",
              id: 4,
              isActive: 1,
              ledgerAccountType: "HUB_RECONCILIATION"
            }
          ],
          isProxy: 0
        }
      ]
      unwrapSnapshot(checkSnapshotObject(result, snapshot))
    })
  })


  describe('DFSP Onboarding', () => {
    it('02 Creates a new DFSP', async () => {
      // Arrange
      const request = {
        query: {
          isProxy: false
        },
        payload: {
          currency: 'USD',
          name: 'dfsp_x',
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const {
        code, body
      } = await TestUtils.unwrapHapiResponse(h => ParticipantHandler.create(request, h))

      // Assert
      assert.equal(code, 201)
      const snapshot = {
        name: 'dfsp_x',
        id: 'http://central-ledger/participants/dfsp_x',
        created: ':ignore',
        isActive: 1,
        links: { self: 'http://central-ledger/participants/dfsp_x' },
        accounts: [
          {
            id: 5,
            ledgerAccountType: 'POSITION',
            currency: 'USD',
            isActive: 0,
            "createdDate:ignore": true,
            createdBy: 'unknown'
          },
          {
            id: 6,
            ledgerAccountType: 'SETTLEMENT',
            currency: 'USD',
            isActive: 0,
            "createdDate:ignore": true,
            createdBy: 'unknown'
          }
        ],
        isProxy: 0
      }
      unwrapSnapshot(checkSnapshotObject(body, snapshot))
    })

    it('03 Adds a second currency to an existing DFSP', async () => {
      const request = {
        query: {
          isProxy: false
        },
        payload: {
          currency: 'KES',
          name: 'dfsp_x',
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const {
        code, body
      } = await TestUtils.unwrapHapiResponse(h => ParticipantHandler.create(request, h))

      // Assert
      assert.equal(code, 201)
      const snapshot = {
        name: 'dfsp_x',
        id: 'http://central-ledger/participants/dfsp_x',
        created: ':ignore',
        isActive: 1,
        links: { self: 'http://central-ledger/participants/dfsp_x' },
        accounts: [
          {
            id: 5,
            ledgerAccountType: 'POSITION',
            currency: 'USD',
            isActive: 0,
            "createdDate:ignore": true,
            createdBy: 'unknown'
          },
          {
            id: 6,
            ledgerAccountType: 'SETTLEMENT',
            currency: 'USD',
            isActive: 0,
            "createdDate:ignore": true,
            createdBy: 'unknown'
          },
          {
            id: 7,
            ledgerAccountType: 'POSITION',
            currency: 'KES',
            isActive: 0,
            "createdDate:ignore": true,
            createdBy: 'unknown'
          },
          {
            id: 8,
            ledgerAccountType: 'SETTLEMENT',
            currency: 'KES',
            isActive: 0,
            "createdDate:ignore": true,
            createdBy: 'unknown'
          }
        ],
        isProxy: 0
      }
      unwrapSnapshot(checkSnapshotObject(body, snapshot))
    })
    
    it.todo('04 Gets the Participant by name')
  })
  // describe('GET  /participants/limits')
  // describe('GET  /participants/{name}')
  // describe('PUT  /participants/{name}')
  // describe('GET  /participants/{name}/endpoints')
  // describe('POST /participants/{name}/endpoints')
  // describe('GET  /participants/{name}/limits')
  // describe('PUT  /participants/{name}/limits')
  // describe('GET  /participants/{name}/positions')
  // describe('GET  /participants/{name}/accounts')
  // describe('PUT  /participants/{name}/accounts')
  // describe('PUT  /participants/{name}/accounts/{id}')
  // describe('POST /participants/{name}/accounts/{id}')
  // describe('POST /participants/{name}/accounts/{id}/transfers/{id}')
  // describe('POST /participants/{name}/initialPositionAndLimits')
})