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
import { checkSnapshotLedgerDfsp, unwrapSnapshot } from '../../testing/snapshot';

const participantService = require('../participant');

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
        // migration: { type: 'knex', updateSqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql') }
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
      const dfspId = 'dfsp_c';
      const currency = 'USD';
      const depositAmount = 10000;
      const adjustedLimit = 6000;

      // Arrange: Create participant and DFSP
      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))

      // Act: Deposit funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))

      // Assert
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,0,0,10000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,0,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,0,0;`
      ))

      // Act: Adjust the net debit cap to lower than deposit amount
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: adjustedLimit
      }))

      // Assert
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,4000,0,10000,6000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,4000,4000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,6000,6000;`
      ))

      // Act: Now adjust NDC to be greater than deposit amount
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'UNLIMITED',
        dfspId,
        currency,
      }))

      // Assert: Query DFSP after limit adjustment
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,4000,0,14000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,4000,0,4000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,6000,6000;`
      ))

      // Act: Now deposit more funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        dfspId,
        currency,
        transferId: randomUUID(),
        amount: 10000,
        reason: 'Additional deposit'
      }))

      // Assert: Query DFSP after limit adjustment
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }))
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,20000,0,0,20000;
        USD,20100,0,4000,0,24000,20000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,4000,0,4000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,6000,6000;`
      ))
    })

    it('applies the net debit cap on the entire deposit amount', async () => {
      // Set net debit cap to 10k, deposit 11k
      // Then deposit another 2k, unrestricted should be 10k, restricted should be 3k
      const dfspId = 'dfsp_d';
      const currency = 'USD';

      // Arrange: Create participant and DFSP
      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))

      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: 10000
      }))

      // Act: Deposit funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: 11000,
        reason: 'Test deposit'
      }))

      // Assert
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,11000,0,0,11000;
        USD,20100,0,1000,0,11000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,1000,1000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,10000,10000;`
      ))

      // Act: Deposit another 2,000
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: 2000,
        reason: 'Additional deposit'
      }))

      // Assert
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,13000,0,0,13000;
        USD,20100,0,4000,0,14000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,1000,0,4000,3000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,10000,10000;`
      ))
    })

    it('deposit is idempotent', async () => {
      // Set net debit cap to 10k, deposit 11k
      // Then deposit another 2k, unrestricted should be 10k, restricted should be 3k
      const dfspId = 'dfsp_e';
      const currency = 'USD';
      const transferId = '123456'

      // Arrange: Create participant and DFSP
      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))

      // Deposit funds
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId,
        dfspId,
        currency,
        amount: 11000,
        reason: 'First deposit'
      }))


      // Act
      const depositResponseB = await ledger.deposit({
        transferId,
        dfspId,
        currency,
        amount: 11000,
        reason: 'First deposit'
      })

      assert(depositResponseB.type === 'ALREADY_EXISTS')
    })

    it('prepares the withdrawal', async () => {
      // Arrange
      const dfspId = 'dfsp_f'
      const currency = 'USD'
      const depositAmount = 10000
      const netDebitCap = 5000
      const withdrawAmount = 6000
      const withdrawalTransferId = '230482309234234'

      await participantService.ensureExists(dfspId); 
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: netDebitCap
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,5000,0,10000,5000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,0,0,5000,5000;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))

      // Act
      const withdrawPrepareResult = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      })
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,6000,0,4000;
        USD,20100,6000,5000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))
      assert(withdrawPrepareResult.type === 'SUCCESS', 'expected success result')
    })

    it('withdraws funds in 2 phases', async () => {
      // Arrange
      const dfspId = 'dfsp_g'
      const currency = 'USD'
      const depositAmount = 10000
      const netDebitCap = 5000
      const withdrawAmount = 6000
      const withdrawalTransferId = '2345872398928374'

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: netDebitCap
      }))
      TestUtils.unwrapSuccess(await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,6000,0,4000;
        USD,20100,6000,5000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))

      // Act
      const withdrawCommitResult = await ledger.withdrawCommit({
        transferId: withdrawalTransferId,
      })
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,6000,4000;
        USD,20100,0,11000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))
      assert(withdrawCommitResult.type === 'SUCCESS', 'expected success result')
    })

    it('withdraw fails if there are not enough funds available', async () => {
      // Arrange
      const dfspId = 'dfsp_h'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawAmount = 3000
      const withdrawalTransferId = '23984723984723'

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))

      // Act
      const result = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      })

      // Assert
      assert(result.type === 'INSUFFICIENT_FUNDS')
    })

    it('fails in the prepare phase if the id has been reused', async () => {
      // Arrange
      const dfspId = 'dfsp_i'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawAmount = 3000
      const withdrawalTransferId = '12348239898723498'

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))
      await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'First withdrawal'
      })

      // Act
      const duplicateWithdrawalResult = await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: 100,
        reason: 'Duplicate withdrawal'
      })

      // Assert
      assert(duplicateWithdrawalResult.type === 'FAILURE')
      assert.strictEqual(
        duplicateWithdrawalResult.error.message,
        'Withdrawal failed - transferId has already been used.'
      )
    })

    it('handles a withdrawCommit() where the id is not found', async () => {
      // Arrange
      const dfspId = 'dfsp_j'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawalTransferId = randomUUID()

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))

      // Act
      const duplicateWithdrawalResult = await ledger.withdrawCommit({
        transferId: withdrawalTransferId,
      })

      // Assert
      assert(duplicateWithdrawalResult.type === 'FAILURE')
      assert.strictEqual(duplicateWithdrawalResult.error.message, `transferId: ${withdrawalTransferId} not found`)
    })

    it('aborts a withdrawal', async () => {
      // Arrange
      const dfspId = 'dfsp_k'
      const currency = 'USD'
      const depositAmount = 10000
      const netDebitCap = 5000
      const withdrawAmount = 6000
      const withdrawalTransferId = randomUUID()

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))
      TestUtils.unwrapSuccess(await ledger.setNetDebitCap({
        netDebitCapType: 'AMOUNT',
        dfspId,
        currency,
        amount: netDebitCap
      }))
      TestUtils.unwrapSuccess(await ledger.withdrawPrepare({
        transferId: withdrawalTransferId,
        dfspId,
        currency,
        amount: withdrawAmount,
        reason: 'Test withdrawal'
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,6000,0,4000;
        USD,20100,6000,5000,0,15000,4000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))

      // Act
      const withdrawCommitResult = await ledger.withdrawAbort({
        transferId: withdrawalTransferId,
      })
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      unwrapSnapshot(checkSnapshotLedgerDfsp(ledgerDfsp, `
        USD,10200,0,10000,0,0,10000;
        USD,20100,0,5000,0,15000,10000;
        USD,20101,0,0,0,0,0;
        USD,20200,0,5000,0,5000,0;
        USD,20300,0,0,0,0,0;
        USD,20400,0,0,0,0,0;
        USD,60200,0,0,0,5000,5000;`
      ))
      assert(withdrawCommitResult.type === 'SUCCESS', 'expected success result')
    })

    it('handles a withdrawal abort where the id is not found', async () => {
      // Arrange
      const dfspId = 'dfsp_l'
      const currency = 'USD'
      const depositAmount = 2500
      const withdrawalTransferId = randomUUID()

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: depositAmount,
        reason: 'Initial deposit'
      }))

      // Act
      const duplicateWithdrawalResult = await ledger.withdrawAbort({
        transferId: withdrawalTransferId,
      })

      // Assert
      assert(duplicateWithdrawalResult.type === 'FAILURE')
      assert.strictEqual(duplicateWithdrawalResult.error.message, `transferId: ${withdrawalTransferId} not found`)
    })

    it.only('disables the Deposit account', async () => {
      // Arrange
      const dfspId = 'dfsp_m'
      const currency = 'USD'

      await participantService.ensureExists(dfspId);
      TestUtils.unwrapSuccess(await ledger.createDfsp({
        dfspId,
        currencies: [currency]
      }))
      TestUtils.unwrapSuccess(await ledger.deposit({
        transferId: randomUUID(),
        dfspId,
        currency,
        amount: 2500,
        reason: 'Initial deposit'
      }))
      let ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      const depositAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert(depositAccount, 'deposit account not found')

      // Act
      const closeAccountResult = await ledger.disableDfspAccount({
        dfspId,
        accountId: Number(depositAccount.id)
      })

      // Assert
      assert(closeAccountResult.type === 'SUCCESS')
      ledgerDfsp = TestUtils.unwrapSuccess(await ledger.getDfspV2({ dfspId }));
      const updatedDepositAccount = ledgerDfsp.accounts.find(acc => acc.code === AccountCode.Deposit)
      assert.strictEqual(updatedDepositAccount.status, 'DISABLED')
    })

    it.todo('fails to disable an account for a dfsp that does not exist')
    it.todo('fails to disable a valid account that is not a Deposit or Unrestricted account')
    it.todo('has no effect if the account is already closed')


    // TODO(LD): blocked by implementing this!
    it.todo('fails to withdraw if the deposit account is disabled')
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