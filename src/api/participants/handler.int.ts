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
import { checkSnapshotObject, checkSnapshotString, unwrapSnapshot } from '../../testing/snapshot';

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
    /**
     * Note: These tests are prefixed with a number since they depend on the state of the 
     *       previous tests to be completed succesfully and in order. An alternative option
     *       would have been to put them all inside one `it()` block, but I feel that the 
     *       approach chosen here makes the test output more readable.
     */
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


  describe.skip('DFSP Onboarding', () => {
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

    it('04 deactivates a participant', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_x'
        },
        payload: {
          isActive: false,
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const body = await ParticipantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 0)
    })

    it('05 reactivates a participant', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_x'
        },
        payload: {
          isActive: true,
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const body = await ParticipantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 1)
    })

    it('06 deactivating a deactivated participant has no effect', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_x'
        },
        payload: {
          isActive: false,
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      await ParticipantHandler.update(request)
      const body = await ParticipantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 0)
    })

    it('07 cannot deactivate a participant that does not exist', async () => {
      // Arrange
      const request = {
        params: {
          name: 'not_a_dfsp'
        },
        payload: {
          isActive: false,
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      try {
        await ParticipantHandler.update(request)
        throw new Error('Test failed')
      } catch (err) {
        assert.equal(err.message, 'Participant does not exist')
      }
    })

    it('08 can deactivate the Hub participant', async () => {
      // Arrange
      const request = {
        params: {
          name: 'Hub'
        },
        payload: {
          isActive: false,
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const body = await ParticipantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 0)
    })
  })

  describe('Limits', () => {
    it('01 Setup', async () => {
      // Arrange
      const request = {
        query: {
          isProxy: false
        },
        payload: {
          currency: 'USD',
          name: 'dfsp_y',
        },
        server: {
          app: {
            ledger
          }
        }
      }

      await TestUtils.unwrapHapiResponse(h => ParticipantHandler.create(request, h))
    })

    it('02 Gets the opening limits', async () => {
      // Arrange
      const request = {
        query: {
          currency: 'USD',
          type: 'NET_DEBIT_CAP'
        },
        params: {
          name: 'dfsp_y',
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const body = await ParticipantHandler.getLimits(request)

      // Assert
      unwrapSnapshot(checkSnapshotString(JSON.stringify(body), "[]"))
    })
  })

  // describe('GET  /participants/limits') -> tested incidentally
  // describe('GET  /participants/{name}') -> tested incidentally
  // describe('PUT  /participants/{name}') ✅
  // describe('GET  /participants/{name}/limits')
  // describe('PUT  /participants/{name}/limits')
  // describe('GET  /participants/{name}/accounts')
  // describe('PUT  /participants/{name}/accounts')
  // describe('PUT  /participants/{name}/accounts/{id}')
  // describe('POST /participants/{name}/accounts/{id}')
  // describe('POST /participants/{name}/accounts/{id}/transfers/{id}')
  // describe('POST /participants/{name}/initialPositionAndLimits')
  // describe('GET  /participants/{name}/positions')


  // always kinda covered
  // describe('GET  /participants/{name}/endpoints')
  // describe('POST /participants/{name}/endpoints')

})