// Mock Kafka before any imports
const mockKafkaProducer = {
  produceGeneralMessage: async () => ({ success: true }),
  connect: async () => { },
  disconnect: async () => { }
};

// Simple module patching approach
const Module = require('module');
const originalRequire = Module.prototype.require;
Module.prototype.require = function (id: string) {
  if (id === '@mojaloop/central-services-stream') {
    return {
      Util: {
        Producer: mockKafkaProducer
      }
    };
  }
  return originalRequire.apply(this, arguments);
};

import { describe, it, beforeEach, afterEach, before, after } from 'node:test';
import assert from 'node:assert';
import { Enum } from '@mojaloop/central-services-shared';
import LegacyCompatibleLedger, { LegacyCompatibleLedgerDependencies } from './LegacyCompatibleLedger';
import { PrepareResultType, FulfilResultType, PrepareResult, FulfilResult } from './types';
import { CreateTransferDto, CommitTransferDto } from '../../handlers-v2/types';
import { FusedPrepareHandlerInput } from '../../handlers-v2/FusedPrepareHandler';
import { FusedFulfilHandlerInput } from '../../handlers-v2/FusedFulfilHandler';
import { ApplicationConfig } from '../../shared/config';
import { makeConfig } from '../../shared/config/resolver';
import Db from '../../lib/db';
import Cache from '../../lib/cache';
import EnumCached from '../../lib/enumCached';
import ParticipantCached from '../../models/participant/participantCached';
import ParticipantCurrencyCached from '../../models/participant/participantCurrencyCached';
import ParticipantLimitCached from '../../models/participant/participantLimitCached';
import BatchPositionModelCached from '../../models/position/batchCached';
import externalParticipantCached from '../../models/participant/externalParticipantCached';
import SettlementModelCached from '../../models/settlement/settlementModelCached';
import { IntegrationHarness, DatabaseConfig } from '../../testing/integration-harness';
import Provisioner, { ProvisionerDependencies, ProvisioningConfig } from '../../shared/provisioner';
import { logger } from '../../shared/logger';
import DFSPProvisioner, { DFSPProvisionerConfig } from '../../testing/dfsp-provisioner';
import { randomUUID } from 'node:crypto';
import { MojaloopMockQuoteILPResponse, TestUtils } from '../../testing/testutils';

describe('LegacyCompatibleLedger', () => {
  let ledger: LegacyCompatibleLedger;
  let config: ApplicationConfig;
  let harness: IntegrationHarness;
  let dbConfig: DatabaseConfig;

  before(async () => {
    try {
      // Set up Docker MySQL container for integration testing
      harness = new IntegrationHarness({
        databaseName: 'central_ledger_test',
        mysqlImage: 'mysql:8.0',
        memorySize: '256m',
        port: 3307,
        migration: { type: 'knex' }
      });

      dbConfig = await harness.start();
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

      // Initialize all cached models (same as in setup-new.ts)
      await EnumCached.initialize();
      await ParticipantCached.initialize();
      await ParticipantCurrencyCached.initialize();
      await ParticipantLimitCached.initialize();
      await BatchPositionModelCached.initialize();
      await SettlementModelCached.initialize();
      externalParticipantCached.initialize();
      await Cache.initCache();

      // Provision the switch
      const provisionConfig: ProvisioningConfig = {
        currencies: ['USD'],
        settlementModels: [],
        oracles: []
      }
      const provisionerDependencies: ProvisionerDependencies = {
        participantsHandler: require('../../api/participants/handler'),
        participantService: require('../../domain/participant'),
        settlementModelDomain: require('../../domain/settlement'),
      }
      const provisioner = new Provisioner(provisionConfig, provisionerDependencies)
      await provisioner.run();

      // Provision dfsps
      const dfspAConfig: DFSPProvisionerConfig = {
        id: 'dfsp_a',
        currencies: ['USD'],
        initialLimits: [100000]
      }
      const dfspBConfig: DFSPProvisionerConfig = {
        id: 'dfsp_b',
        currencies: ['USD'],
        initialLimits: [100000]
      }
      const dfspProvisioner = new DFSPProvisioner({
        ledger: null as LegacyCompatibleLedger,
        participantsHandler: require('../../api/participants/handler'),
        participantService: require('../../domain/participant'),
        participantFacade: require('../../models/participant/facade'),
        transferService: require('../../domain/transfer'),
        enums: await require('../../lib/enumCached').getEnums('all'),
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
      await Cache.destroyCache();

      // Clean up Docker container
      if (harness) {
        await harness.teardown();
      }

      throw err
    }
  });

  after(async () => {
    // Clean up database connection and cache
    await Db.disconnect();
    await Cache.destroyCache();

    // Clean up Docker container
    if (harness) {
      await harness.teardown();
    }
  });

  beforeEach(async () => {
    // Initialize ledger with real dependencies (same as initializeLedger in setup-new.ts)
    const Validator = require('../../handlers/transfers/validator');
    const TransferService = require('../../domain/transfer/index');
    const Participant = require('../../domain/participant');
    const participantFacade = require('../../models/participant/facade');
    const Comparators = require('@mojaloop/central-services-shared').Util.Comparators;
    const TransferObjectTransform = require('../../domain/transfer/transform');
    const PositionService = require('../../domain/position');
    const prepareModule = require('../../handlers/transfers/prepare');

    const deps: LegacyCompatibleLedgerDependencies = {
      config,
      validatePrepare: Validator.validatePrepare,
      validateParticipantByName: Validator.validateParticipantByName,
      validatePositionAccountByNameAndCurrency: Validator.validatePositionAccountByNameAndCurrency,
      validateParticipantTransferId: Validator.validateParticipantTransferId,
      validateFulfilCondition: Validator.validateFulfilCondition,
      validationReasons: Validator.reasons,
      handlePayeeResponse: TransferService.handlePayeeResponse,
      getTransferById: TransferService.getById,
      getTransferInfoToChangePosition: TransferService.getTransferInfoToChangePosition,
      getTransferFulfilmentDuplicateCheck: TransferService.getTransferFulfilmentDuplicateCheck,
      saveTransferFulfilmentDuplicateCheck: TransferService.saveTransferFulfilmentDuplicateCheck,
      transformTransferToFulfil: TransferObjectTransform.toFulfil,
      duplicateCheckComparator: Comparators.duplicateCheckComparator,
      checkDuplication: prepareModule.checkDuplication,
      savePreparedRequest: prepareModule.savePreparedRequest,
      calculatePreparePositionsBatch: PositionService.calculatePreparePositionsBatch,
      changeParticipantPosition: PositionService.changeParticipantPosition,
      getAccountByNameAndCurrency: Participant.getAccountByNameAndCurrency,
      getByIDAndCurrency: participantFacade.getByIDAndCurrency,
    };

    ledger = new LegacyCompatibleLedger(deps);
  });

  afterEach(async () => {
    // Clean up database connection
    // await Db.disconnect();
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

    it('01. prepare transfer', async () => {
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
});