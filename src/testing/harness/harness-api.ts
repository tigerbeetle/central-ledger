import assert from "assert";
import { Client, createClient } from "tigerbeetle-node";
import { Ledger } from "../../domain/ledger-v2/Ledger";
import LegacyCompatibleLedger, { LegacyCompatibleLedgerDependencies } from "../../domain/ledger-v2/LegacyCompatibleLedger";
import { PersistedMetadataStore } from "../../domain/ledger-v2/PersistedMetadataStore";
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
  private harnessMessageBus: HarnessMessageBus
  private client: Client
  private transferBatcher: TransferBatcher

  constructor(
    private config: HarnessApiConfig,
    private dbLib: any,
    private participantService: any,
  ) {
    this.harnessDatabase = new HarnessDatabase(config.databaseConfig)
    this.harnessTigerBeetle = new HarnessTigerBeetle(config.tigerBeetleConfig)
    this.harnessMessageBus = new HarnessMessageBus(config.messageBusConfig)
  }

  public async start(): Promise<{ ledger: Ledger }> {
    logger.info('HarnessApi - start()')
    // Start the respective harnesses
    const [dbConfig, tbConfig, messageBusConfig] = await Promise.all([
      this.harnessDatabase.start(),
      this.harnessTigerBeetle.start(),
      this.harnessMessageBus.start()
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
    const ledger = await this.initLegacyLedger()

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

    if (this.harnessMessageBus) {
      await this.harnessMessageBus.teardown()
    }
  }

  private async initTigerBeetleLedger(): Promise<TigerBeetleLedger> {
    const deps: TigerBeetleLedgerDependencies = {
      // TODO: do we need to set up the ledger config based on what the harness did?
      config: this.config.applicationConfig,
      client: this.client,
      metadataStore: new PersistedMetadataStore(this.dbLib.getKnex()),
      transferBatcher: this.transferBatcher,
      participantService: this.participantService,
    }
    return new TigerBeetleLedger(deps)
  }

  private async initLegacyLedger(): Promise<LegacyCompatibleLedger> {
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

    const deps: LegacyCompatibleLedgerDependencies = {
      config: this.config.applicationConfig,
      knex: Db.getKnex(),
      lifecycle: {
        participantsHandler: require('../../api/participants/HandlerV1'),
        participantService: require('../../domain/participant'),
        participantFacade: require('../../models/participant/facade'),
        transferService: require('../../domain/transfer'),
        transferFacade: require('../../models/transfer/facade'),
        enums: await require('../../lib/enumCached').getEnums('all'),
        settlementModelDomain: require('../../domain/settlement'),
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

    return new LegacyCompatibleLedger(deps)
  }
}