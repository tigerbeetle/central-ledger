import assert from 'assert';
import path from 'path';
import { Client, createClient } from 'tigerbeetle-node';
import { AdminHandler } from '../../handlers-v2/AdminHandler';
import { Ledger } from '../../domain/ledger-v2/Ledger';
import LegacyLedger, { LegacyLedgerDependencies } from '../../domain/ledger-v2/LegacyLedger';
import { PersistedSpecStore } from '../../domain/ledger-v2/SpecStorePersisted';
import TigerBeetleLedger, { TigerBeetleLedgerDependencies } from '../../domain/ledger-v2/TigerBeetleLedger';
import Db from '../../lib/db';
import Cache from '../../lib/cache';
import EnumCached from '../../lib/enumCached';
import externalParticipantCached from '../../models/participant/externalParticipantCached';
import ParticipantCached from '../../models/participant/participantCached';
import ParticipantCurrencyCached from '../../models/participant/participantCurrencyCached';
import ParticipantLimitCached from '../../models/participant/participantLimitCached';
import BatchPositionModelCached from '../../models/position/batchCached';
import SettlementModelCached from '../../models/settlement/settlementModelCached';
import { ApplicationConfig, CurrencyLedgerConfig, LedgerType } from '../../shared/config';
import { makeConfig } from '../../shared/config/resolver';
import { logger } from '../../shared/logger';
import Provisioner, { ProvisioningConfig } from '../../shared/provisioner';
import { initializeCache } from '../../shared/setup-new';
import DFSPProvisioner, { DFSPProvisionerConfig } from '../dfsp-provisioner';
import { Harness } from './base';
import { DatabaseConfig, HarnessDatabase, HarnessDatabaseConfig } from './harness-database';
import { HarnessTigerBeetle, HarnessTigerBeetleConfig, TigerBeetleConfig } from './harness-tigerbeetle';

/**
 * Configuration for IntegrationHarness
 */
export interface IntegrationHarnessConfig {
  /**
   * Which ledger implementation to use. Defaults to 'TIGERBEETLE'
   */
  ledgerType?: LedgerType;
  /**
   * Currency ledger configurations. If not provided, uses sensible defaults for USD and KES
   */
  currencyLedgers?: CurrencyLedgerConfig[];

  /**
   * Database configuration. If not provided, uses sensible defaults
   */
  database?: Partial<HarnessDatabaseConfig>;

  /**
   * TigerBeetle configuration. If not provided, uses sensible defaults
   */
  tigerbeetle?: Partial<HarnessTigerBeetleConfig>;

  /**
   * Hub currencies to provision. Defaults to ['USD']
   */
  hubCurrencies?: string[];

  /**
   * DFSPs to provision at startup. Defaults to empty array
   */
  provisionDfsps?: DFSPProvisionerConfig[];

  /**
   * Whether to initialize the cache. Defaults to true
   */
  initializeCache?: boolean;

  /**
   * Custom application config overrides
   */
  applicationConfigOverrides?: Partial<ApplicationConfig>;
}

/**
 * Resources created by the IntegrationHarness
 */
export interface IntegrationHarnessResources {
  ledger: Ledger;
  client: Client;
  config: ApplicationConfig;
  dbConfig: DatabaseConfig;
  tbConfig: TigerBeetleConfig;
}

/**
 * Default currency ledger configurations for testing
 */
const DEFAULT_CURRENCY_LEDGERS: CurrencyLedgerConfig[] = [
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
];

/**
 * IntegrationHarness - A composable test harness for integration testing
 *
 * Implements the Harness interface and provides a convenient way to set up
 * all infrastructure needed for integration tests (database, TigerBeetle, ledger, etc.)
 *
 * Usage:
 * ```typescript
 * let harness: IntegrationHarness;
 *
 * before(async () => {
 *   harness = await IntegrationHarness.create({
 *     hubCurrencies: ['USD'],
 *     provisionDfsps: [
 *       { dfspId: 'dfsp_a', currencies: ['USD'], startingDeposits: [100000] }
 *     ]
 *   });
 * });
 *
 * after(async () => {
 *   await harness.teardown();
 * });
 * ```
 */
export class IntegrationHarness implements Harness {
  private dbHarness: HarnessDatabase;
  private tbHarness: HarnessTigerBeetle;
  private client: Client;
  private config: ApplicationConfig;
  private resources: IntegrationHarnessResources;

  constructor(private harnessConfig: IntegrationHarnessConfig = {}) {}

  /**
   * Create and start a new integration harness (convenience method)
   */
  static async create(config: IntegrationHarnessConfig = {}): Promise<IntegrationHarness> {
    const harness = new IntegrationHarness(config);
    await harness.start();
    return harness;
  }

  /**
   * Get the resources created by this harness
   */
  getResources(): IntegrationHarnessResources {
    return this.resources;
  }

  /**
   * Start the integration harness and provision infrastructure
   */
  async start(): Promise<IntegrationHarnessResources> {
    try {
      const projectRoot = path.join(__dirname, '../../..');

      // Build configuration with defaults
      const ledgerType = this.harnessConfig.ledgerType || 'TIGERBEETLE';
      const currencyLedgers = this.harnessConfig.currencyLedgers || DEFAULT_CURRENCY_LEDGERS;
      const hubCurrencies = this.harnessConfig.hubCurrencies || ['USD'];
      const provisionDfsps = this.harnessConfig.provisionDfsps || [];
      const shouldInitializeCache = this.harnessConfig.initializeCache !== false;

      // Create application config with overrides
      this.config = makeConfig();
      this.config.EXPERIMENTAL.TIGERBEETLE.CURRENCY_LEDGERS = currencyLedgers;

      // Apply any additional application config overrides
      if (this.harnessConfig.applicationConfigOverrides) {
        Object.assign(this.config, this.harnessConfig.applicationConfigOverrides);
      }

      // Set up database harness
      const dbConfig: HarnessDatabaseConfig = {
        databaseName: 'central_ledger_test',
        mysqlImage: 'mysql:8.0',
        memorySize: '256m',
        port: 3307,
        migration: {
          // type: 'sql', sqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql')
          // uncomment to update the checkpoint file
          type: 'knex', updateSqlFilePath: path.join(projectRoot, 'ddl/central_ledger.checkpoint.sql')
        },
        ...this.harnessConfig.database
      };
      this.dbHarness = new HarnessDatabase(dbConfig);

      // Set up TigerBeetle harness
      const tbConfig: HarnessTigerBeetleConfig = {
        tigerbeetleBinaryPath: this.resolveTigerBeetlePath(projectRoot),
        ...this.harnessConfig.tigerbeetle
      };
      this.tbHarness = new HarnessTigerBeetle(tbConfig);

      // Start harnesses in parallel
      const [dbResult, tbResult] = await Promise.all([
        this.dbHarness.start(),
        this.tbHarness.start()
      ]);

      // Connect to database
      const testDbConfig = {
        ...this.config.DATABASE,
        connection: {
          ...this.config.DATABASE.connection,
          host: dbResult.host,
          port: dbResult.port,
          user: dbResult.user,
          password: dbResult.password,
          database: dbResult.database
        }
      };

      await Db.connect(testDbConfig);
      assert(Db._tables, 'expected Db._tables to be defined');
      assert(Db._tables.length);

      // Initialize cache if requested
      if (shouldInitializeCache) {
        await initializeCache();
      }

      // Set up TigerBeetle client
      this.client = createClient({
        cluster_id: tbResult.clusterId,
        replica_addresses: tbResult.address,
      });

      // Create ledger based on type
      const ledger = ledgerType === 'TIGERBEETLE'
        ? await this.createTigerBeetleLedger()
        : await this.createLegacyLedger();

      // Provision hub
      const provisionConfig: ProvisioningConfig = {
        currencies: hubCurrencies,
        settlementModels: [],
        oracles: []
      };
      const provisioner = new Provisioner(provisionConfig, { ledger });
      await provisioner.run();

      // Provision DFSPs if requested
      if (provisionDfsps.length > 0) {
        const participantService = require('../../domain/participant');
        const dfspProvisioner = new DFSPProvisioner({
          ledger,
          participantService
        });

        for (const dfspConfig of provisionDfsps) {
          await dfspProvisioner.run(dfspConfig);
        }
      }

      // Store resources
      this.resources = {
        ledger,
        client: this.client,
        config: this.config,
        dbConfig: dbResult,
        tbConfig: tbResult,
      };

      logger.info('IntegrationHarness started successfully');
      return this.resources;
    } catch (err) {
      logger.error(`IntegrationHarness.start() failed: ${err.message}`);
      if (err.stack) {
        logger.error(err.stack);
      }

      // Clean up on error
      await this.teardown();
      throw err;
    }
  }

  /**
   * Teardown the integration harness and clean up resources
   */
  async teardown(): Promise<void> {
    try {
      // Disconnect database
      if (Db) {
        await Db.disconnect();
      }

      // Destroy TigerBeetle client
      if (this.client) {
        this.client.destroy();
      }

      // Teardown harnesses
      if (this.tbHarness) {
        await this.tbHarness.teardown();
      }
      if (this.dbHarness) {
        await this.dbHarness.teardown();
      }

      logger.info('IntegrationHarness teardown completed');
    } catch (err) {
      logger.error(`IntegrationHarness.teardown() failed: ${err.message}`);
      // Don't throw - best effort cleanup
    }
  }

  /**
   * Create TigerBeetleLedger instance
   */
  private async createTigerBeetleLedger(): Promise<TigerBeetleLedger> {
    const knex = Db.getKnex()
    const deps: TigerBeetleLedgerDependencies = {
      config: this.config,
      client: this.client,
      specStore: new PersistedSpecStore(knex),
      knex
    };
    return new TigerBeetleLedger(deps);
  }

  /**
   * Create LegacyLedger instance
   */
  private async createLegacyLedger(): Promise<LegacyLedger> {
    // Initialize all cached models
    await EnumCached.initialize();
    await ParticipantCached.initialize();
    await ParticipantCurrencyCached.initialize();
    await ParticipantLimitCached.initialize();
    await BatchPositionModelCached.initialize();
    await SettlementModelCached.initialize();
    externalParticipantCached.initialize();
    await Cache.initCache();

    // Initialize AdminHandler
    const TransferService = require('../../domain/transfer/index');
    const Comparators = require('@mojaloop/central-services-shared').Util.Comparators;
    const adminHandler = new AdminHandler({
      committer: null as any,
      config: this.config,
      transferService: TransferService,
      comparators: Comparators,
      db: Db
    });

    // Initialize dependencies
    const Validator = require('../../handlers/transfers/validator');
    const Participant = require('../../domain/participant');
    const participantFacade = require('../../models/participant/facade');
    const TransferObjectTransform = require('../../domain/transfer/transform');
    const PositionService = require('../../domain/position');
    const prepareModule = require('../../handlers/transfers/prepare');

    const deps: LegacyLedgerDependencies = {
      config: this.config,
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
        settlementModel: require('../../settlement/models/settlement'),
        settlementWindows: require('../../settlement/domain/settlementWindow'),
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

    return new LegacyLedger(deps);
  }

  /**
   * Resolve the TigerBeetle binary path, checking common locations
   */
  private resolveTigerBeetlePath(projectRoot: string): string {
    // Try common locations in order
    const possiblePaths = [
      // Relative to project root (for CI/standard setup)
      path.join(projectRoot, '../../.bin/tigerbeetle'),
      // Hardcoded path (fallback for development)
      '/Users/lewisdaly/tb/tigerloop/.bin/tigerbeetle',
    ];

    // For now, just use the first path and let it fail if not found
    // In the future, we could check fs.existsSync() for each path
    return possiblePaths[0];
  }
}
