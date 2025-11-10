import assert from "assert";
import TigerBeetleLedger, { TigerBeetleLedgerDependencies } from "../../domain/ledger-v2/TigerBeetleLedger";
import { TransferBatcher } from "../../domain/ledger-v2/TransferBatcher";
import { ApplicationConfig } from "../../shared/config";
import { Client, createClient } from "tigerbeetle-node";
import { Harness } from "./base";
import { HarnessDatabase, HarnessDatabaseConfig } from "./harness-database";
import { HarnessTigerBeetle, HarnessTigerBeetleConfig } from "./harness-tigerbeetle";
import { PersistedMetadataStore } from "../../domain/ledger-v2/PersistedMetadataStore";
import Provisioner, { ProvisioningConfig } from "../../shared/provisioner";
import { logger } from "../../shared/logger";


// For now, assume we are using the TigerBeetle Ledger, but in the future we should be able
// to configure the Ledger
export interface HarnessApiConfig {
  databaseConfig: HarnessDatabaseConfig,
  tigerBeetleConfig: HarnessTigerBeetleConfig,
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

  // public async start(): Promise<{dbConfig: DatabaseConfig, tbConfig: TigerBeetleConfig}> {
  public async start(): Promise<{ledger: TigerBeetleLedger}> {
    logger.info('HarnessApi - start()')
    // Start the respective harnesses
    const [dbConfig, tbConfig] = await Promise.all([
      this.harnessDatabase.start(),
      this.harnessTigerBeetle.start()
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
    // const testTBConfig = {
    //   ...this.config.applicationConfig,
    // }


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
    const deps: TigerBeetleLedgerDependencies = {
      // TODO: do we need to set up the ledger config based on what the harness did?
      config: this.config.applicationConfig,
      client: this.client,
      metadataStore: new PersistedMetadataStore(this.dbLib.getKnex()),
      transferBatcher: this.transferBatcher,
      participantService: this.participantService,
    }
    const ledger = new TigerBeetleLedger(deps)

    // Provision the switch
    const provisionConfig: ProvisioningConfig = {
      currencies: ['USD'],
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
  }
}