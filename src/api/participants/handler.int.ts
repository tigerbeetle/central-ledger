import assert from 'assert';
import { randomUUID } from 'crypto';
import { after, before, describe, it } from 'node:test';
import path from 'path';
import { Ledger } from '../../domain/ledger-v2/Ledger';
import Db from '../../lib/db';
import { makeConfig } from '../../shared/config/resolver';
import { logger } from '../../shared/logger';
import { initializeCache } from '../../shared/setup-new';
import { HarnessApi, HarnessApiConfig } from '../../testing/harness/harness-api';
import { checkSnapshotObject, checkSnapshotString, unwrapSnapshot } from '../../testing/snapshot';
import { TestUtils } from '../../testing/testutils';
import * as snapshots from './__snapshots__/handler.int.snapshots';
import ParticipantAPIHandlerV2 from './HandlerV2';

type GetAccountResponseDTO = {
  changedDate: unknown,
  currency: string,
  id: string,
  isActive: number,
  ledgerAccountType: 'POSITION' | 'SETTLEMENT'
  reservedValue: number
  value: number
}

describe('api/participants/handler', () => {
  let harnessApi: HarnessApi
  let ledger: Ledger
  let participantHandler: ParticipantAPIHandlerV2 = new ParticipantAPIHandlerV2()

  before(async () => {
    try {
      const projectRoot = path.join(__dirname, '../../..')

      const applicationConfig = makeConfig()
      // TODO: figure out a nicer way to override these sorts of config options
      applicationConfig.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS = [
        {
          currency: 'USD',
          assetScale: 4,
          clearingLedgerId: 12,
          settlementLedgerId: 13,
          controlLedgerId: 14,

          ledgerOperation: 1001,
          ledgerControl: 2001,
          accountIdSettlementBalance: 123098124n
        },
        {
          currency: 'KES',
          assetScale: 4,
          clearingLedgerId: 22,
          settlementLedgerId: 23,
          controlLedgerId: 24,
          ledgerOperation: 1002,
          ledgerControl: 2002,
          accountIdSettlementBalance: 9253488424n
        },
      ]
      const config: HarnessApiConfig = {
        databaseConfig: {
          databaseName: 'central_ledger_test',
          mysqlImage: 'mysql:8.0',
          memorySize: '256m',
          port: 3307,
          migration: { type: 'sql', sqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql') }
          // migration: { type: 'knex', updateSqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql') }
        },
        tigerBeetleConfig: {
          tigerbeetleBinaryPath: path.join(projectRoot, '../../', '.bin/tigerbeetle')
        },
        messageBusConfig: {
          port: 9092,
          internalPort: 9192
        },
        applicationConfig
      }
      // TODO(LD): Hopefully we can remove this at some point
      const participantService = require('../../domain/participant');
      harnessApi = new HarnessApi(config, Db, participantService);

      const harnessApiResult = await harnessApi.start()

      // Annoying global that needs to be initialized for database calls to work.
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

  // Note: 
  // TigerBeetleLedger has no notion of 'Hub Accounts', so it's not possible to add new hub accounts
  // on the fly. We may revisit this decision at a later point, but I'm disabling this test for now.
  describe.skip('Hub', () => {
    it('01 creates a new hub account for currency MWK', async () => {
      // Arrange
      const requestMultilateralSettlement = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_MULTILATERAL_SETTLEMENT',
          currency: 'MWK'
        },
        server: {
          app: {
            ledger
          }
        }
      }

      const requestReconciliation = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_RECONCILIATION',
          currency: 'MWK'
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const {
        code: code1,
        body: body1
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.createHubAccount(requestMultilateralSettlement, h))

      const {
        code: code2,
        body: body2
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.createHubAccount(requestReconciliation, h))

      // Assert
      assert.equal(code1, 201)
      assert.equal(code2, 201)

      unwrapSnapshot(checkSnapshotObject(body2, snapshots.createsANewHubAccountForCurrencyMWK))
    })

    it('02 does not throw an error when creating hub accounts for the same currecy twice', async () => {
      // Arrange
      const request = {
        params: {
          name: 'Hub'
        },
        payload: {
          type: 'HUB_MULTILATERAL_SETTLEMENT',
          currency: 'USD' // USD hub account already exists from provisioning
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const { code } = await TestUtils.unwrapHapiResponse(h => participantHandler.createHubAccount(request, h))

      // Assert
      assert.equal(code, 201)
    })
  })

  describe('Participants', () => {
    /**
     * Note: These tests are prefixed with a number since they depend on the state of the 
     *       previous tests to be completed succesfully and in order. An alternative option
     *       would have been to put them all inside one `it()` block, but I feel that the 
     *       approach chosen here makes the test output more readable.
     */
    it('01 Returns the hub information', async () => {
      // Arrange
      const request = {
        query: { isProxy: false },
        payload: {},
        server: { app: { ledger } }
      }

      // Act
      const result = await participantHandler.getAll(request)

      // Assert
      assert(result, 'Expected a response from getAll()')
      const snapshot = snapshots.returnsHubInformation
      unwrapSnapshot(checkSnapshotObject(result, snapshot))
    })

    it('02 Creates a new DFSP and then calls getAll', async () => {
      // Arrange
      const request = {
        query: { isProxy: false },
        payload: {
          currency: 'USD',
          name: 'dfsp_d',
        },
        server: { app: { ledger } }
      }

      // Act
      const {
        code, body
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))

      // Assert
      const result = await participantHandler.getAll(request)

      assert(result, 'Expected a response from getAll()')
      const snapshot = snapshots.createsNewDfspThenCallsGetAll
      unwrapSnapshot(checkSnapshotObject(result, snapshot))
    })
  })

  describe('DFSP Onboarding', () => {
    it('01 Creates a new DFSP', async () => {
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
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))

      // Assert
      assert.equal(code, 201)
      unwrapSnapshot(checkSnapshotObject(body, snapshots.createsANewDfsp))
    })

    it('02 Adds a second currency to an existing DFSP', async () => {
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
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))

      // Assert
      assert.equal(code, 201)
      const snapshot = snapshots.addsASecondCurrencyToExistingDFSP
      unwrapSnapshot(checkSnapshotObject(body, snapshot))
    })

    it('03 deactivates a participant', async () => {
      // Arrange
      const request = {
        params: { name: 'dfsp_x' },
        payload: { isActive: false, },
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 0)
    })

    it('04 reactivates a participant', async () => {
      // Arrange
      const request = {
        params: { name: 'dfsp_x' },
        payload: { isActive: true, },
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 1)
    })

    it('05 deactivating a deactivated participant has no effect', async () => {
      // Arrange
      const request = {
        params: { name: 'dfsp_x' },
        payload: { isActive: false, },
        server: { app: { ledger } }
      }


      // Act
      await participantHandler.update(request)
      const body = await participantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 0)
    })

    it('06 cannot deactivate a participant that does not exist', async () => {
      // Arrange
      const request = {
        params: { name: 'not_a_dfsp' },
        payload: { isActive: false, },
        server: { app: { ledger } }
      }

      // Act
      try {
        await participantHandler.update(request)
        throw new Error('Test failed')
      } catch (err) {
        assert.equal(err.message, 'Participant does not exist')
      }
    })

    it('07 cannot deactivate the Hub participant', async () => {
      // Arrange
      const request = {
        params: { name: 'Hub' },
        payload: { isActive: false, },
        server: { app: { ledger } }
      }

      // Act
      try {
        await participantHandler.update(request)
        throw new Error('Test failed')
      } catch (err) {
        assert.equal(err.message, 'Cannot update the Hub account.')
      }
    })

    it('08 cannot create the same currency for the dfsp twice', async () => {
      const request = {
        query: { isProxy: false },
        payload: {
          currency: 'KES',
          name: 'dfsp_x',
        },
        server: { app: { ledger } }
      }

      // Act
      try {
        const {
          code, body
        } = await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))
        throw new Error('Test failed')
      } catch (err) {
        assert.equal(err.message, 'Participant currency has already been registered')
      }
    })

    it('09 activating an already active participant has no effect', async () => {
      // Arrange
      const request = {
        params: { name: 'dfsp_x' },
        payload: { isActive: true, },
        server: { app: { ledger } }
      }

      // Act
      await participantHandler.update(request)
      const body = await participantHandler.update(request)

      // Assert
      assert.equal(body.isActive, 1)
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

      await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))
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
      const body = await participantHandler.getLimits(request)

      // Assert
      unwrapSnapshot(checkSnapshotString(JSON.stringify(body), "[]"))
    })

    it('03 Sets the initial limit', async () => {
      // Arrange
      const request = {
        payload: {
          currency: 'USD',
          limit: {
            value: 10000000,
            type: 'NET_DEBIT_CAP',
          }
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
      const {
        code, body
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.addLimitAndInitialPosition(request, h))
      const checkLimitRequest = {
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

      const newLimit = await participantHandler.getLimits(checkLimitRequest)

      // Assert
      assert.equal(code, 201)
      assert.equal(body, undefined)
      unwrapSnapshot(checkSnapshotObject(newLimit, [{
        currency: 'USD',
        limit: {
          type: "NET_DEBIT_CAP",
          value: 10000000,
          alarmPercentage: 10,
        }
      }]))
    })

    it('04 Changes the limit', async () => {
      // Arrange
      const request = {
        payload: {
          currency: 'USD',
          limit: {
            value: 4_000_000,
            type: 'NET_DEBIT_CAP',
          }
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
      const {
        body
      } = await TestUtils.unwrapHapiResponse(h => participantHandler.adjustLimits(request, h))

      // Assert
      unwrapSnapshot(checkSnapshotObject(body, {
        currency: 'USD',
        limit: {
          type: "NET_DEBIT_CAP",
          value: 4_000_000,
        }
      }))
    })

    it('05 Gets the limits for all participants', async () => {
      // Arrange
      const request = {
        query: {
          currency: 'USD',
          type: 'NET_DEBIT_CAP'
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const body = await participantHandler.getLimitsForAllParticipants(request)

      // Assert
      unwrapSnapshot(checkSnapshotObject(body, [{
        name: 'dfsp_y',
        currency: 'USD',
        limit: {
          type: 'NET_DEBIT_CAP',
          value: 4_000_000,
          alarmPercentage: 10
        }
      }]))
    })

    // TODO: need test the rebalancing the restricted/unrestricted when:
    // 1. limited NDC -> unlimited NDC:       all funds should move into unrestricted
    // 2. limited NDC -> smaller limited NDC: unrestricted must <= limit 
  })

  describe('Accounts', () => {
    let positionAccountId: string
    let settlementAccountId: string

    // Helpers
    const getDFSPAccounts = async (dsfpId: string): Promise<Array<GetAccountResponseDTO>> => {
      const request = {
        query: { currency: 'USD' },
        params: { name: dsfpId },
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.getAccounts(request)
      return body
    }

    const setInitialPositions = async (dfspId: string, currency: string, limit: number): Promise<void> => {
      const request = {
        payload: {
          currency,
          limit: {
            value: limit,
            type: 'NET_DEBIT_CAP',
          }
        },
        params: { name: dfspId },
        server: { app: { ledger } }
      }

      await TestUtils.unwrapHapiResponse(h => participantHandler.addLimitAndInitialPosition(request, h))
    }

    it('01 Setup', async () => {
      // Arrange
      const request = {
        query: {
          isProxy: false
        },
        payload: {
          currency: 'USD',
          name: 'dfsp_u',
        },
        server: { app: { ledger } }
      }

      await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))
      await setInitialPositions('dfsp_u', 'USD', 50_000)
    })

    it('02 Gets the opening accounts', async () => {
      // Arrange
      // Act
      const accounts = await getDFSPAccounts('dfsp_u')

      // Assert
      unwrapSnapshot(checkSnapshotObject(accounts, [
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer',
          isActive: 1,
          ledgerAccountType: "POSITION",
          reservedValue: 0,
          value: 0
        },
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "SETTLEMENT",
          reservedValue: 0,
          value: -50000
        },
      ]))

      positionAccountId = accounts.filter(acc => acc.ledgerAccountType === 'POSITION')[0].id
      settlementAccountId = accounts.filter(acc => acc.ledgerAccountType === 'SETTLEMENT')[0].id
    })

    it('03 Deposits working capital', async () => {
      // Arrange
      assert(positionAccountId, 'value expected from previous `it` block')
      assert(settlementAccountId, 'value expected from previous `it` block')

      const request = {
        payload: {
          transferId: randomUUID(),
          externalReference: "12345",
          action: "recordFundsIn",
          reason: "deposit",
          amount: {
            currency: 'USD',
            amount: '100000.00'
          }
        },
        params: {
          name: 'dfsp_u',
          id: Number.parseInt(settlementAccountId), // accountId from above
          transferId: '67890',
        },
        server: { app: { ledger } }
      }

      // Act
      const {
        code,
        body
      } = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.recordFunds(request, h)
      )
      assert.equal(code, 202)
      assert.equal(body, undefined)
      const updatedAccounts = await getDFSPAccounts('dfsp_u')

      // Assert
      unwrapSnapshot(checkSnapshotObject(updatedAccounts, [
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer',
          isActive: 1,
          ledgerAccountType: "POSITION",
          reservedValue: 0,
          value: 0
        },
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer',
          isActive: 1,
          ledgerAccountType: "SETTLEMENT",
          reservedValue: 0,
          value: -150000
        },
      ]))
    })

    it('04 deactivates and reactivates the position account', async () => {
      assert(positionAccountId, 'value expected from previous `it` block')

      const requestDeactivate = {
        payload: { isActive: false },
        params: {
          name: 'dfsp_u',
          id: Number.parseInt(positionAccountId), // accountId from above
        },
        server: { app: { ledger } }
      }

      // Act
      const deactivateResponse = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.updateAccount(requestDeactivate, h)
      )
      assert.equal(deactivateResponse.code, 200)
      assert.equal(deactivateResponse.body, undefined)

      let updatedAccounts = await getDFSPAccounts('dfsp_u')
      let positionAccount = updatedAccounts.filter(acc => acc.ledgerAccountType === 'POSITION')[0]
      assert.equal(positionAccount.isActive, 0)

      // now reset
      const requestReactivate = {
        payload: { isActive: true, },
        params: {
          name: 'dfsp_u',
          id: positionAccountId, // accountId from above
        },
        server: { app: { ledger } }
      }
      const reactivateResponse = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.updateAccount(requestReactivate, h)
      )
      assert.equal(reactivateResponse.code, 200)
      assert.equal(reactivateResponse.body, undefined)
      // should be reactivated
      updatedAccounts = await getDFSPAccounts('dfsp_u')
      positionAccount = updatedAccounts.filter(acc => acc.ledgerAccountType === 'POSITION')[0]
      assert.equal(positionAccount.isActive, 1)
    })

    it('05 does not allow deactivating the settlement account', async () => {
      assert(settlementAccountId, 'value expected from previous `it` block')
      const request = {
        payload: { isActive: false, },
        params: {
          name: 'dfsp_u',
          id: Number.parseInt(settlementAccountId), // accountId from above
        },
        server: { app: { ledger } }
      }

      // Act
      try {
        const {
          code,
          body
        } = await TestUtils.unwrapHapiResponse(h =>
          participantHandler.updateAccount(request, h)
        )
        throw new Error('Test Error')
      } catch (err) {
        assert.equal(err.message, 'Only position account update is permitted')
      }
    })

    it('06 withdraws funds in 2 steps', async () => {
      // Arrange
      assert(positionAccountId, 'value expected from previous `it` block')
      assert(settlementAccountId, 'value expected from previous `it` block')

      const transferId = randomUUID()
      const requestWithdraw = {
        payload: {
          transferId,
          externalReference: "67890",
          action: "recordFundsOutPrepareReserve",
          reason: "withdrawal",
          amount: {
            currency: 'USD',
            amount: '50000.00'
          }
        },
        params: {
          name: 'dfsp_u',
          id: settlementAccountId, // accountId from above
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const responseWithdraw = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.recordFunds(requestWithdraw, h)
      )
      assert.equal(responseWithdraw.code, 202)
      assert.equal(responseWithdraw.body, undefined)

      let updatedAccounts = await getDFSPAccounts('dfsp_u')

      // Assert
      unwrapSnapshot(checkSnapshotObject(updatedAccounts, [
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "POSITION",
          reservedValue: 0,
          value: 0
        },
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "SETTLEMENT",
          // Funds out has no effect on `reservedValue`
          reservedValue: 0,
          value: -100000
        },
      ]))

      const requestConfirm = {
        payload: {
          action: "recordFundsOutCommit",
          reason: "withdrawal",
        },
        params: {
          name: 'dfsp_u',
          id: settlementAccountId, // accountId from above
          transferId
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const responseConfirm = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.recordFunds(requestConfirm, h)
      )
      assert.equal(responseConfirm.code, 202)
      assert.equal(responseConfirm.body, undefined)

      updatedAccounts = await getDFSPAccounts('dfsp_u')

      // Assert
      unwrapSnapshot(checkSnapshotObject(updatedAccounts, [
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "POSITION",
          reservedValue: 0,
          value: 0
        },
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "SETTLEMENT",
          reservedValue: 0,
          value: -100000
        },
      ]))
    })

    it('07 withdraw fails if not enough funds are available', async () => {
      // Arrange
      assert(settlementAccountId, 'value expected from previous `it` block')

      const transferId = randomUUID()
      const requestWithdraw = {
        payload: {
          transferId,
          externalReference: "insufficient-funds-test",
          action: "recordFundsOutPrepareReserve",
          reason: "withdrawal",
          amount: {
            currency: 'USD',
            amount: '200000.00' // Attempting to withdraw more than available
          }
        },
        params: {
          name: 'dfsp_u',
          id: settlementAccountId,
        },
        server: {
          app: {
            ledger
          }
        }
      }

      // Act
      const responseWithdraw = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.recordFunds(requestWithdraw, h)
      )

      // Assert - withdrawal request is accepted (202) but silently rejected due to insufficient funds
      assert.equal(responseWithdraw.code, 202)

      const updatedAccounts = await getDFSPAccounts('dfsp_u')

      // Settlement account balance should remain unchanged (withdrawal was silently rejected)
      unwrapSnapshot(checkSnapshotObject(updatedAccounts, [
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "POSITION",
          reservedValue: 0,
          value: 0
        },
        {
          changedDate: ":string",
          currency: "USD",
          id: ':integer', isActive: 1,
          ledgerAccountType: "SETTLEMENT",
          reservedValue: 0,
          value: -100000 // Balance unchanged - withdrawal was rejected
        },
      ]))
    })
  })

  describe('Positions', () => {

    // shortcuts
    const createDfspForCurrency = async (dfspId: string, currency: string): Promise<void> => {
      const request = {
        query: { isProxy: false },
        payload: {
          currency,
          name: dfspId
        },
        server: { app: { ledger } }
      }

      await TestUtils.unwrapHapiResponse(h => participantHandler.create(request, h))

    }
    const setInitialPositions = async (dfspId: string, currency: string, limit: number): Promise<void> => {
      const request = {
        payload: {
          currency,
          limit: {
            value: limit,
            type: 'NET_DEBIT_CAP',
          }
        },
        params: {
          name: dfspId
        },
        server: { app: { ledger } }
      }

      await TestUtils.unwrapHapiResponse(h => participantHandler.addLimitAndInitialPosition(request, h))
    }

    it('01 Setup', async () => {
      // Arrange
      await createDfspForCurrency('dfsp_w', 'USD')
      await createDfspForCurrency('dfsp_w', 'KES')
      await createDfspForCurrency('dfsp_v', 'USD')

      await setInitialPositions('dfsp_w', 'USD', 100_000)
      await setInitialPositions('dfsp_w', 'KES', 100_000_000)
      await setInitialPositions('dfsp_v', 'USD', 10_000)
    })

    it('02 Gets positions across multiple currencies', async () => {
      // Arrange
      const request = {
        query: {},
        params: {
          name: 'dfsp_w',
        },
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.getPositions(request)

      // Assert
      unwrapSnapshot(checkSnapshotObject(body, [{
        changedDate: ":ignore",
        currency: "USD",
        value: 0
      },
      {
        changedDate: ":ignore",
        currency: "KES",
        value: 0
      }]))
    })

    it('03 Gets positions for a single currency', async () => {
      // Arrange
      const request = {
        query: {
          currency: 'USD',
        },
        params: {
          name: 'dfsp_v',
        },
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.getPositions(request)

      // Assert
      unwrapSnapshot(checkSnapshotObject(body, {
        changedDate: ":ignore",
        currency: "USD",
        value: 0
      }))
    })
  })

  describe('Endpoints', () => {
    it('01 Setup', async () => {
      // Arrange - create 2 DFSPs for endpoint tests
      const createDfsp1 = {
        query: { isProxy: false },
        payload: {
          currency: 'USD',
          name: 'dfsp_endpoint1'
        },
        server: { app: { ledger } }
      }

      const createDfsp2 = {
        query: { isProxy: false },
        payload: {
          currency: 'USD',
          name: 'dfsp_endpoint2'
        },
        server: { app: { ledger } }
      }

      // Act
      await TestUtils.unwrapHapiResponse(h => participantHandler.create(createDfsp1, h))
      await TestUtils.unwrapHapiResponse(h => participantHandler.create(createDfsp2, h))
    })

    it('02 Adds an endpoint for a participant', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_endpoint1'
        },
        payload: {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST',
          value: 'http://dfsp_endpoint1.example.com/transfers'
        },
        server: { app: { ledger } }
      }

      // Act
      const { code } = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.addEndpoint(request, h)
      )

      // Assert
      assert.equal(code, 201)
    })

    it('03 Fails to add an endpoint for an invalid participant', async () => {
      // Arrange
      const request = {
        params: {
          name: 'invalid_dfsp_that_does_not_exist'
        },
        payload: {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST',
          value: 'http://invalid.example.com/transfers'
        },
        server: { app: { ledger } }
      }

      // Act & Assert
      try {
        await TestUtils.unwrapHapiResponse(h =>
          participantHandler.addEndpoint(request, h)
        )
        throw new Error('Test failed - should have thrown an error')
      } catch (err) {
        assert(err.message.includes('does not exist') || err.message.includes('not found') || err.message.includes('Participant'),
          `Expected error about participant not existing, got: ${err.message}`)
      }
    })

    it('04 Fails to add an endpoint for an invalid endpoint type', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_endpoint2'
        },
        payload: {
          type: 'INVALID_ENDPOINT_TYPE_THAT_DOES_NOT_EXIST',
          value: 'http://dfsp_endpoint2.example.com/invalid'
        },
        server: { app: { ledger } }
      }

      // Act & Assert
      try {
        await TestUtils.unwrapHapiResponse(h =>
          participantHandler.addEndpoint(request, h)
        )
        throw new Error('Test failed - should have thrown an error')
      } catch (err) {
        // The error can be about invalid type or about endpointTypeId not being found
        assert(
          err.message.indexOf('Cannot read properties of undefined') > -1,
          `Expected error about invalid endpoint type, got: ${err.message}`
        )
      }
    })

    it('05 Gets an endpoint for a given type', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_endpoint1'
        },
        query: {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST'
        },
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.getEndpoint(request)

      // Assert
      unwrapSnapshot(checkSnapshotObject(body, {
        type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST',
        value: 'http://dfsp_endpoint1.example.com/transfers'
      }))
    })

    it('06 Gets all endpoints across all types', async () => {
      // Arrange - first add another endpoint for the same participant
      const addRequest = {
        params: {
          name: 'dfsp_endpoint1'
        },
        payload: {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_PUT',
          value: 'http://dfsp_endpoint1.example.com/transfers/{{transferId}}'
        },
        server: { app: { ledger } }
      }

      await TestUtils.unwrapHapiResponse(h =>
        participantHandler.addEndpoint(addRequest, h)
      )

      const request = {
        params: {
          name: 'dfsp_endpoint1'
        },
        query: {},
        server: { app: { ledger } }
      }

      // Act
      const body = await participantHandler.getEndpoint(request)

      // Assert
      unwrapSnapshot(checkSnapshotObject(body, [
        {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST',
          value: 'http://dfsp_endpoint1.example.com/transfers'
        },
        {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_PUT',
          value: 'http://dfsp_endpoint1.example.com/transfers/{{transferId}}'
        }
      ]))
    })

    it('07 Adds multiple endpoints at once using array', async () => {
      // Arrange
      const request = {
        params: {
          name: 'dfsp_endpoint2'
        },
        payload: [
          {
            type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST',
            value: 'http://dfsp_endpoint2.example.com/transfers'
          },
          {
            type: 'FSPIOP_CALLBACK_URL_TRANSFER_PUT',
            value: 'http://dfsp_endpoint2.example.com/transfers/{{transferId}}'
          },
          {
            type: 'FSPIOP_CALLBACK_URL_TRANSFER_ERROR',
            value: 'http://dfsp_endpoint2.example.com/transfers/{{transferId}}/error'
          }
        ],
        server: { app: { ledger } }
      }

      // Act
      const { code } = await TestUtils.unwrapHapiResponse(h =>
        participantHandler.addEndpoint(request, h)
      )

      // Assert
      assert.equal(code, 201)

      // Verify all endpoints were added by fetching them
      const getRequest = {
        params: {
          name: 'dfsp_endpoint2'
        },
        query: {},
        server: { app: { ledger } }
      }

      const body = await participantHandler.getEndpoint(getRequest)
      unwrapSnapshot(checkSnapshotObject(body, [
        {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_POST',
          value: 'http://dfsp_endpoint2.example.com/transfers'
        },
        {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_PUT',
          value: 'http://dfsp_endpoint2.example.com/transfers/{{transferId}}'
        },
        {
          type: 'FSPIOP_CALLBACK_URL_TRANSFER_ERROR',
          value: 'http://dfsp_endpoint2.example.com/transfers/{{transferId}}/error'
        }
      ]))
    })
  })
})

