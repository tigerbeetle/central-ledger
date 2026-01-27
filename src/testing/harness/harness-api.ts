import assert from "assert";
import { Client, createClient } from "tigerbeetle-node";
import { AdminHandler } from "../../handlers-v2/AdminHandler";
import { Ledger } from "../../domain/ledger-v2/Ledger";
import LegacyLedger, { LegacyLedgerDependencies } from "../../domain/ledger-v2/LegacyLedger";
import { PersistedSpecStore } from "../../domain/ledger-v2/SpecStorePersisted";
import TigerBeetleLedger, { TigerBeetleLedgerDependencies } from "../../domain/ledger-v2/TigerBeetleLedger";
import { TransferBatcher } from "../../domain/ledger-v2/TransferBatcher";
import { ApplicationConfig } from "../../shared/config";
import { logger } from "../../shared/logger";
import Provisioner, { ProvisioningConfig } from "../../shared/provisioner";
import { Harness } from "./base";
import { HarnessDatabase, HarnessDatabaseConfig } from "./harness-database";
import { HarnessTigerBeetle, HarnessTigerBeetleConfig } from "./harness-tigerbeetle";
import { HarnessMessageBus, HarnessMessageBusConfig } from "./harness-messagebus";

import Cache from '../../lib/cache';
import Db from '../../lib/db';
import EnumCached from '../../lib/enumCached';
import externalParticipantCached from '../../models/participant/externalParticipantCached';
import ParticipantCached from '../../models/participant/participantCached';
import ParticipantCurrencyCached from '../../models/participant/participantCurrencyCached';
import ParticipantLimitCached from '../../models/participant/participantLimitCached';
import BatchPositionModelCached from '../../models/position/batchCached';
import SettlementModelCached from '../../models/settlement/settlementModelCached';



// For now, assume we are using the TigerBeetle Ledger, but in the future we should be able
// to configure the Ledger
export interface HarnessApiConfig {
  databaseConfig: HarnessDatabaseConfig,
  tigerBeetleConfig: HarnessTigerBeetleConfig,
  messageBusConfig: HarnessMessageBusConfig,
  applicationConfig: ApplicationConfig
}

/**
 * @class HarnessApi
 * @description An API Harness that contains other suboordinate Harnesses. Spins up everything we
 *   need to write API Integration tests
 */
export class HarnessApi implements Harness {
  private harnessDatabase: HarnessDatabase
  private harnessTigerBeetle: HarnessTigerBeetle
  private client: Client
  private transferBatcher: TransferBatcher

  constructor(
    private config: HarnessApiConfig,
    private dbLib: any,
    private participantService: any,
  ) {
    this.harnessDatabase = new HarnessDatabase(config.databaseConfig)
    this.harnessTigerBeetle = new HarnessTigerBeetle(config.tigerBeetleConfig)
  }

  public async start(): Promise<{ ledger: Ledger }> {
    logger.info('HarnessApi - start()')
    const [dbConfig, tbConfig] = await Promise.all([
      this.harnessDatabase.start(),
      this.harnessTigerBeetle.start(),
    ])

    // Override database config to use the test container
    const testDbConfig = {
      ...this.config.applicationConfig.DATABASE,
      connection: {
        ...this.config.applicationConfig.DATABASE.connection,
        host: dbConfig.host,
        port: dbConfig.port,
        user: dbConfig.user,
        password: dbConfig.password,
        database: dbConfig.database
      }
    };

    // Initialize database connection to test container
    await this.dbLib.connect(testDbConfig);
    assert(this.dbLib._tables, 'expected Db._tables to be defined')
    assert(this.dbLib._tables.length)

    // Set up TigerBeetle
    this.client = createClient({
      cluster_id: tbConfig.clusterId,
      replica_addresses: tbConfig.address,
    })
    this.transferBatcher = new TransferBatcher(this.client, 100, 1)
    // const ledger = await this.initLegacyLedger()
    const ledger = await this.initTigerBeetleLedger()

    // Provision the switch
    const provisionConfig: ProvisioningConfig = {
      currencies: ['USD', 'KES'],
      settlementModels: [],
      oracles: []
    }
    const provisioner = new Provisioner(provisionConfig, { ledger })
    await provisioner.run();

    return {
      ledger
    }
  }

  public async teardown(): Promise<void> {
    logger.info('HarnessApi - teardown()')

    // Disconnect all Kafka producers and consumers BEFORE stopping the message bus
    // TODO(LD): move this to elsewhere in the code to stop the runaway errors!
    await this.disconnectAllKafkaProducers();
    await this.disconnectAllKafkaConsumers();

    await this.dbLib.disconnect()
    if (this.client) {
      this.client.destroy()
    }

    if (this.transferBatcher) {
      this.transferBatcher.cleanup()
    }

    if (this.harnessDatabase) {
      await this.harnessDatabase.teardown()
    }

    if (this.harnessTigerBeetle) {
      await this.harnessTigerBeetle.teardown()
    }


  }

  /**
   * Disconnect all Kafka producers using the built-in disconnect function
   * from @mojaloop/central-services-stream
   */
  private async disconnectAllKafkaProducers(): Promise<void> {
    try {
      const KafkaProducer = require('@mojaloop/central-services-stream').Util.Producer;
      logger.info('HarnessApi - disconnecting all Kafka producers');

      // Call disconnect() with null to disconnect ALL producers
      await KafkaProducer.disconnect();

      logger.debug('HarnessApi - all Kafka producers disconnected');
    } catch (err) {
      // Log but don't throw - we want teardown to continue even if this fails
      logger.warn('HarnessApi - failed to disconnect Kafka producers:', err.message);
    }
  }

  /**
   * Disconnect all Kafka consumers using the built-in functions
   * from @mojaloop/central-services-stream
   */
  private async disconnectAllKafkaConsumers(): Promise<void> {
    try {
      const KafkaConsumer = require('@mojaloop/central-services-stream').Util.Consumer;
      logger.info('HarnessApi - disconnecting all Kafka consumers');

      // Get all consumer topic names and disconnect each one
      const topics = KafkaConsumer.getListOfTopics();
      logger.debug(`HarnessApi - found ${topics.length} consumers to disconnect`);

      for (const topic of topics) {
        try {
          const consumer = KafkaConsumer.getConsumer(topic);
          await new Promise((resolve) => {
            consumer.disconnect(() => {
              logger.debug(`HarnessApi - disconnected consumer for topic: ${topic}`);
              resolve(null);
            });
          });
        } catch (err) {
          logger.warn(`HarnessApi - failed to disconnect consumer for topic ${topic}:`, err.message);
        }
      }

      logger.debug('HarnessApi - all Kafka consumers disconnected');
    } catch (err) {
      // Log but don't throw - we want teardown to continue even if this fails
      logger.warn('HarnessApi - failed to disconnect Kafka consumers:', err.message);
    }
  }

  private async initTigerBeetleLedger(): Promise<TigerBeetleLedger> {
    const knex = this.dbLib.getKnex()
    const deps: TigerBeetleLedgerDependencies = {
      config: this.config.applicationConfig,
      client: this.client,
      specStore: new PersistedSpecStore(knex),
      knex
    }
    return new TigerBeetleLedger(deps)
  }

  private async initLegacyLedger(): Promise<LegacyLedger> {
    // Initialize all cached models (same as in setup-new.ts)
    await EnumCached.initialize();
    await ParticipantCached.initialize();
    await ParticipantCurrencyCached.initialize();
    await ParticipantLimitCached.initialize();
    await BatchPositionModelCached.initialize();
    await SettlementModelCached.initialize();
    externalParticipantCached.initialize();
    await Cache.initCache();


    // Initialize ledger with real dependencies (same as initializeLedger in setup-new.ts)
    const Validator = require('../../handlers/transfers/validator');
    const TransferService = require('../../domain/transfer/index');
    const Participant = require('../../domain/participant');
    const participantFacade = require('../../models/participant/facade');
    const Comparators = require('@mojaloop/central-services-shared').Util.Comparators;
    const TransferObjectTransform = require('../../domain/transfer/transform');
    const PositionService = require('../../domain/position');
    const prepareModule = require('../../handlers/transfers/prepare');

    // Initialize AdminHandler
    const adminHandler = new AdminHandler({
      committer: null as any, // Not needed for direct calls
      config: this.config.applicationConfig,
      transferService: TransferService,
      comparators: Comparators,
      db: Db
    });

    const deps: LegacyLedgerDependencies = {
      config: this.config.applicationConfig,
      knex: Db.getKnex(),
      lifecycle: {
        participantService: require('../../domain/participant'),
        participantFacade: require('../../models/participant/facade'),
        transferService: require('../../domain/transfer'),
        transferFacade: require('../../models/transfer/facade'),
        adminHandler: adminHandler,
        enums: await require('../../lib/enumCached').getEnums('all'),
        settlementModelDomain: require('../../domain/settlement'),
      },
      settlement: {
        settlementWindowModel: require('../../settlement/models/settlementWindow'),
        settlementDomain: require('../../settlement/domain/settlement'),
        enums: require('../../settlement/models/lib/enums')
      },
      clearing: {
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
        getTransferErrorDuplicateCheck: TransferService.getTransferErrorDuplicateCheck,
        saveTransferErrorDuplicateCheck: TransferService.saveTransferErrorDuplicateCheck,
        transformTransferToFulfil: TransferObjectTransform.toFulfil,
        duplicateCheckComparator: Comparators.duplicateCheckComparator,
        checkDuplication: prepareModule.checkDuplication,
        savePreparedRequest: prepareModule.savePreparedRequest,
        calculatePreparePositionsBatch: PositionService.calculatePreparePositionsBatch,
        changeParticipantPosition: PositionService.changeParticipantPosition,
        getAccountByNameAndCurrency: Participant.getAccountByNameAndCurrency,
        getByIDAndCurrency: participantFacade.getByIDAndCurrency,
        timeoutService: require('../../domain/timeout'),
      }
    };

    return new LegacyLedger(deps)
  }
}