import assert from 'node:assert';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { Client, createClient } from 'tigerbeetle-node';
import { CommitTransferDto, CreateTransferDto } from '../../handlers-v2/types';
import Db from '../../lib/db';
import { ApplicationConfig } from '../../shared/config';
import { makeConfig } from '../../shared/config/resolver';
import { logger } from '../../shared/logger';
import Provisioner, { ProvisioningConfig } from '../../shared/provisioner';
import { initializeCache } from '../../shared/setup-new';
import DFSPProvisioner, { DFSPProvisionerConfig } from '../../testing/dfsp-provisioner';
import { DatabaseConfig, HarnessDatabase } from '../../testing/harness/harness-database';
import { HarnessTigerBeetle, TigerBeetleConfig } from '../../testing/harness/harness-tigerbeetle';
import { TestUtils } from '../../testing/testutils';
import { PersistedSpecStore } from './SpecStorePersisted';
import TigerBeetleLedger, { AccountCode, TigerBeetleLedgerDependencies } from "./TigerBeetleLedger";
import { TransferBatcher } from './TransferBatcher';
import { PrepareResultType } from './types';
import path from 'node:path';

describe('TigerBeetleLedger', () => {
  let ledger: TigerBeetleLedger
  let transferBatcher: TransferBatcher
  let client: Client
  let config: ApplicationConfig;
  let dbHarness: HarnessDatabase;
  let dbConfig: DatabaseConfig;
  let tbHarness: HarnessTigerBeetle;
  let tbConfig: TigerBeetleConfig

  before(async () => {
    try {
      const projectRoot = path.join(__dirname, '../../..')
      
      // Set up Docker MySQL container for integration testing
      dbHarness = new HarnessDatabase({
        databaseName: 'central_ledger_test',
        mysqlImage: 'mysql:8.0',
        memorySize: '256m',
        port: 3307,
        // migration: { type: 'knex' }
        migration: { type: 'sql', sqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql') }
        
      });
      
      tbHarness = new HarnessTigerBeetle({
        // tigerbeetleBinaryPath: '../../.bin/tigerbeetle'
        tigerbeetleBinaryPath: '/Users/lewisdaly/tb/tigerloop/.bin/tigerbeetle'
      });
      [dbConfig, tbConfig] = await Promise.all([dbHarness.start(), tbHarness.start()])

      config = makeConfig();
      // TODO: figure out a nicer way to override these sorts of config options
      config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS = [
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

      // Override database config to use the test container
      const testDbConfig = {
        ...config.DATABASE,
        connection: {
          ...config.DATABASE.connection,
          host: dbConfig.host,
          port: dbConfig.port,
          user: dbConfig.user,
          password: dbConfig.password,
          database: dbConfig.database
        }
      };

      // Initialize database connection to test container
      await Db.connect(testDbConfig);
      assert(Db._tables, 'expected Db._tables to be defined')
      assert(Db._tables.length)

      client = createClient({
        cluster_id: tbConfig.clusterId,
        replica_addresses: tbConfig.address,
      })
      transferBatcher = new TransferBatcher(client, 100, 1)
      const deps: TigerBeetleLedgerDependencies = {
        config,
        client,
        specStore: new PersistedSpecStore(Db.getKnex()),
        transferBatcher,
      }
      ledger = new TigerBeetleLedger(deps)

      // Provision the switch
      const provisionConfig: ProvisioningConfig = {
        currencies: ['USD'],
        settlementModels: [],
        oracles: []
      }
      const provisioner = new Provisioner(provisionConfig, { ledger })
      await provisioner.run();

      // Provision dfsps
      const dfspAConfig: DFSPProvisionerConfig = {
        dfspId: 'dfsp_a',
        currencies: ['USD'],
        startingDeposits: [100000]
      }
      const dfspBConfig: DFSPProvisionerConfig = {
        dfspId: 'dfsp_b',
        currencies: ['USD'],
        startingDeposits: [100000]
      }
      // TODO(LD): Hopefully we can remove this at some point
      const participantService = require('../participant');
      // Annoying global that needs to be initialized for database calls to work.
      await initializeCache()
      // TODO(LD): would be great to refactor this into just a single provisioner
      const dfspProvisioner = new DFSPProvisioner({
        ledger: ledger,
        participantService
      })
      await dfspProvisioner.run(dfspAConfig)
      await dfspProvisioner.run(dfspBConfig)
    } catch (err) {
      logger.error(`before() - failed with error: ${err.message}`)
      if (err.stack) {
        logger.error(err.stack)
      }

      // Clean up database connection and cache
      await Db.disconnect();
      if (client) {
        client.destroy()
      }

      // Clean up containers and tigerbeetle
      if (tbHarness) {
        await tbHarness.teardown();
      }
      if (dbHarness) {
        await dbHarness.teardown();
      }

      transferBatcher.cleanup()

      throw err
    }
  })

  after(async () => {
    await Db.disconnect();
    client.destroy()

    // Clean up containers and tigerbeetle
    if (tbHarness) {
      await tbHarness.teardown();
    }
    if (dbHarness) {
      await dbHarness.teardown();
    }

    transferBatcher.cleanup()
  });

  describe('lifecycle', () => {
    it('creates a dfsp, deposits funds, sets the limit and adjusts the limit', async () => {
      const participantService = require('../participant');
      const dfspId = 'dfsp_c';
      const currency = 'USD';
      const depositAmount = 10000;
      const adjustedLimit = 6000;

      // Act 1: Create participant and DFSP
      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))

      // Act 2: Deposit funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount
      }))

      // Act 3: Query DFSP after deposit
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      TestUtils.printLedgerDfsps([ledgerDfsp])

      // Assert 3: Unrestricted account net credits match deposit amount
      const unrestrictedAfterDeposit = ledgerDfsp.accounts.find(
        acc => acc.code === AccountCode.Unrestricted && acc.currency === currency
      );
      assert.ok(unrestrictedAfterDeposit, 'Expected to find Unrestricted account');
      assert.strictEqual(unrestrictedAfterDeposit.netCreditsPosted, depositAmount);

      // Act 4: Adjust the net debit cap to lower than deposit amount
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: adjustedLimit
      }))

      // Act 5: Query DFSP after limit adjustment
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      TestUtils.printLedgerDfsps([ledgerDfsp])

      // Assert 5: Verify account balances after limit adjustment
      const unrestrictedAfterAdjust = ledgerDfsp.accounts.find(
        acc => acc.code === AccountCode.Unrestricted && acc.currency === currency
      );
      const restrictedAfterAdjust = ledgerDfsp.accounts.find(
        acc => acc.code === AccountCode.Restricted && acc.currency === currency
      );

      assert.ok(unrestrictedAfterAdjust, 'Expected to find Unrestricted account');
      assert.ok(restrictedAfterAdjust, 'Expected to find Restricted account');

      const transfers = await ledger.getRecentTransfers(11)
      TestUtils.printTransferHistory(transfers);

      // Unrestricted net credits must match the net debit cap
      assert.strictEqual(
        unrestrictedAfterAdjust.netCreditsPosted,
        adjustedLimit,
        'Unrestricted account net credits should match the net debit cap'
      );

      // Sum of Unrestricted and Restricted must match the original deposit
      const totalCredits = unrestrictedAfterAdjust.netCreditsPosted + restrictedAfterAdjust.netCreditsPosted;
      assert.strictEqual(
        totalCredits,
        depositAmount,
        'Sum of Unrestricted and Restricted net credits should match the deposit amount'
      );
    })
  })

  // TODO(LD): come back to these next week!
  describe.skip('timeout handling', () => {
    it('prepares a transfer, waits for timeout, and sweeps', async () => {
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Arrange
       const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: {
          amount: '100',
          currency: 'USD'
        },
        ilpPacket,
        condition,
        // 1 second expiry
        expiration: new Date(Date.now() + 1050).toISOString()
      };
      const input = TestUtils.buildValidPrepareInput(transferId, payload)
      const prepareResult = await ledger.prepare(input)
      assert(prepareResult.type === PrepareResultType.PASS)

      // Act
      await TestUtils.sleep(1500) // wait for TigerBeetle to timeout the transfer
      const sweepResult = await ledger.sweepTimedOut()

      // Assert
      assert(sweepResult.type === 'SUCCESS')
      const ids = sweepResult.transfers.map(t => t.id)
      assert(ids.includes(transferId))
    })

    it('once a transfer is swept, it cannot be swept again', async () => {
      const transferId = randomUUID()
      const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
      const { ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

      // Arrange
       const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: {
          amount: '100',
          currency: 'USD'
        },
        ilpPacket,
        condition,
        // 1 second expiry
        expiration: new Date(Date.now() + 1050).toISOString()
      };
      const input = TestUtils.buildValidPrepareInput(transferId, payload)
      const prepareResult = await ledger.prepare(input)
      assert(prepareResult.type === PrepareResultType.PASS)

      // Act
      await TestUtils.sleep(1500) // wait for TigerBeetle to timeout the transfer
      const sweepResultA = await ledger.sweepTimedOut()
      const sweepResultB = await ledger.sweepTimedOut()

      // Assert
      assert(sweepResultA.type === 'SUCCESS')
      const ids = sweepResultA.transfers.map(t => t.id)
      assert(ids.includes(transferId))
      assert(sweepResultB.type === 'SUCCESS')
      assert(sweepResultB.transfers.length === 0)
    })
  })

  describe.skip('happy path prepare and fulfill', () => {
    const transferId = randomUUID()
    const mockQuoteResponse = TestUtils.generateMockQuoteILPResponse(transferId, new Date(Date.now() + 60000))
    const { fulfilment, ilpPacket, condition } = TestUtils.generateQuoteILPResponse(mockQuoteResponse)

    it('01 prepare transfer', async () => {
      // Arrange
      const payload: CreateTransferDto = {
        transferId,
        payerFsp: 'dfsp_a',
        payeeFsp: 'dfsp_b',
        amount: {
          amount: '100',
          currency: 'USD'
        },
        ilpPacket,
        condition,
        expiration: new Date(Date.now() + 60000).toISOString()
      };
      const input = TestUtils.buildValidPrepareInput(transferId, payload)

      // Act
      const result = await ledger.prepare(input);

      // Assert
      assert.ok(result);
      assert.equal(result.type, PrepareResultType.PASS);
    });

    it('02 fulfill transfer', async () => {
      // Arrange
      const payload: CommitTransferDto = {
        transferState: 'COMMITTED',
        fulfilment,
        completedTimestamp: new Date().toISOString()
      };
      const input = TestUtils.buildValidFulfilInput(transferId, payload)

      // Act
      const result = await ledger.fulfil(input);

      // Assert
      assert.ok(result);
      assert.equal(result.type, PrepareResultType.PASS);
    });
  });
})