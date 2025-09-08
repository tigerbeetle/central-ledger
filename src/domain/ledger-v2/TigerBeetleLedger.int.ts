import { Enum } from '@mojaloop/central-services-shared';
import assert from 'node:assert';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { FusedFulfilHandlerInput } from '../../handlers-v2/FusedFulfilHandler';
import { FusedPrepareHandlerInput } from '../../handlers-v2/FusedPrepareHandler';
import { CommitTransferDto, CreateTransferDto } from '../../handlers-v2/types';
import Db from '../../lib/db';
import { ApplicationConfig } from '../../shared/config';
import { makeConfig } from '../../shared/config/resolver';
import { logger } from '../../shared/logger';
import Provisioner, { ProvisioningConfig } from '../../shared/provisioner';
import DFSPProvisioner, { DFSPProvisionerConfig } from '../../testing/dfsp-provisioner';
import { DatabaseConfig, IntegrationHarnessDatabase, IntegrationHarnessTigerBeetle, TigerBeetleConfig } from '../../testing/integration-harness';
import { MojaloopMockQuoteILPResponse, TestUtils } from '../../testing/testutils';
import { PrepareResultType } from './types';

import { Client, createClient } from 'tigerbeetle-node';
import { PersistedMetadataStore } from './PersistedMetadataStore';
import TigerBeetleLedger, { TigerBeetleLedgerDependencies } from "./TigerBeetleLedger";
import { TransferBatcher } from './TransferBatcher';

describe('TigerBeetleLedger', () => {
  let ledger: TigerBeetleLedger
  let client: Client
  let config: ApplicationConfig;
  let dbHarness: IntegrationHarnessDatabase;
  let dbConfig: DatabaseConfig;
  let tbHarness: IntegrationHarnessTigerBeetle;
  let tbConfig: TigerBeetleConfig

  before(async () => {
    try {
      // Set up Docker MySQL container for integration testing
      // TODO: add Tigerbeetle in memory binary to harness
      dbHarness = new IntegrationHarnessDatabase({
        databaseName: 'central_ledger_test',
        mysqlImage: 'mysql:8.0',
        memorySize: '256m',
        port: 3307,
        migration: { type: 'knex' }
        // migration: { type: 'sql', sqlFilePath: './central_ledger.checkpoint.sql' }
      });
      dbConfig = await dbHarness.start();

      tbHarness = new IntegrationHarnessTigerBeetle({
        // tigerbeetleBinaryPath: '../../.bin/tigerbeetle'
        tigerbeetleBinaryPath: '/Users/lewisdaly/tb/tigerloop/.bin/tigerbeetle'
      })
      tbConfig = await tbHarness.start()

      config = makeConfig();

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
      const deps: TigerBeetleLedgerDependencies = {
        config,
        client,
        metadataStore: new PersistedMetadataStore(Db.getKnex()),
        transferBatcher: new TransferBatcher(client, 100, 1),
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
        initialLimits: [100000]
      }
      const dfspBConfig: DFSPProvisionerConfig = {
        dfspId: 'dfsp_b',
        currencies: ['USD'],
        initialLimits: [100000]
      }
      const dfspProvisioner = new DFSPProvisioner({
        ledger: ledger,
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
  });

  describe('happy path prepare and fulfill', () => {
    const transferId = randomUUID()
    const mockQuoteResponse: MojaloopMockQuoteILPResponse = {
      quoteId: '00001',
      // TODO: how do we get this determinitically?
      transactionId: '00001',
      transactionType: 'unknown',
      payerId: 'dfsp_a',
      payeeId: 'dfsp_b',
      transferId,
      amount: 100,
      currency: 'USD',
      expiration: new Date(Date.now() + 60000).toISOString()
    }
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

      const input: FusedPrepareHandlerInput = {
        payload,
        transferId,
        headers: {
          'fspiop-source': 'dfsp_a',
          'fspiop-destination': 'dfsp_b',
          'content-type': 'application/vnd.interoperability.transfers+json;version=1.0'
        },
        message: {
          value: {
            from: 'payerfsp',
            to: 'payeefsp',
            id: `msg-${transferId}`,
            type: 'application/json',
            content: {
              headers: {
                'fspiop-source': 'dfsp_a',
                'fspiop-destination': 'dfsp_b'
              },
              payload,
              uriParams: { id: transferId }
            },
            metadata: {
              event: {
                id: `event-${transferId}`,
                type: 'transfer',
                action: 'prepare',
                createdAt: new Date().toISOString(),
                state: {
                  status: 'success',
                  code: 0
                }
              }
            }
          }
        },
        action: Enum.Events.Event.Action.PREPARE,
        metric: 'transfer_prepare',
        functionality: Enum.Events.Event.Type.TRANSFER,
        actionEnum: 'PREPARE'
      };

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

      const input: FusedFulfilHandlerInput = {
        payload,
        transferId,
        headers: {
          'fspiop-source': 'dfsp_b',
          'fspiop-destination': 'dfsp_a',
          'content-type': 'application/vnd.interoperability.transfers+json;version=1.0'
        },
        message: {
          value: {
            from: 'dfsp_b',
            to: 'dfsp_a',
            id: `msg-${transferId}`,
            type: 'application/json',
            content: {
              headers: {
                'fspiop-source': 'dfsp_b',
                'fspiop-destination': 'dfsp_a',
              },
              payload,
              uriParams: { id: transferId }
            },
            metadata: {
              event: {
                id: `event-${transferId}`,
                type: 'transfer',
                action: 'commit',
                createdAt: new Date().toISOString(),
                state: {
                  status: 'success',
                  code: 0
                }
              }
            }
          }
        },
        action: Enum.Events.Event.Action.COMMIT,
        eventType: 'fulfil',
        kafkaTopic: 'topic-transfer-fulfil'
      };

      // Act
      const result = await ledger.fulfil(input);

      // Assert
      assert.ok(result);
      assert.equal(result.type, PrepareResultType.PASS);
    });
  });
})