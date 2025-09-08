import { exec } from 'child_process';
import { promisify } from 'util';
import { readFile } from 'fs/promises';
import { logger } from '../shared/logger';
import assert from 'assert';
import path from 'path';
import { TestUtils } from './testutils';

import { spawn } from 'child_process';
import { randomBytes } from 'crypto';
const fs = require('fs').promises;
import os from 'os';

const execAsync = promisify(exec);

export interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export interface MigrationOptionsKnex {
  type: 'knex';
}

export interface MigrationOptionsSql {
  type: 'sql';
  sqlFilePath: string;
}

export type MigrationOptions = MigrationOptionsKnex | MigrationOptionsSql;

export interface IntegrationHarness {
  start(): Promise<any>;
  teardown(): Promise<void>;
}

export interface IntegrationHarnessConfig {
  databaseName: string;
  mysqlImage: string;
  memorySize: string;
  port: number;
  migration: MigrationOptions;
}

export class IntegrationHarnessDatabase implements IntegrationHarness {
  private containerId: string | null = null;
  private config: IntegrationHarnessConfig;
  private port: number = 0;

  constructor(config: IntegrationHarnessConfig) {
    this.config = config;
  }

  /**
   * Start MySQL container with in-memory storage
   * @returns DatabaseConfig for connecting to the test database
   */
  async start(): Promise<DatabaseConfig> {
    const containerName = `cl-test-mysql-${Date.now()}`;

    // Find an available port starting from the configured port
    const availablePort = await TestUtils.findAvailablePort(this.config.port);
    this.port = availablePort;

    try {
      // Launch MySQL container with tmpfs for in-memory storage
      const dockerCommand = [
        'docker', 'run',
        '--name', containerName,
        '--tmpfs', `/var/lib/mysql:rw,size=${this.config.memorySize}`,
        '-e', 'MYSQL_ROOT_PASSWORD=password',
        '-e', `MYSQL_DATABASE=${this.config.databaseName}`,
        '-p', `${availablePort}:3306`,
        '-d',
        this.config.mysqlImage
      ];

      const { stdout } = await execAsync(dockerCommand.join(' '));
      this.containerId = stdout.trim();

      logger.info(`Started MySQL test container: ${this.containerId.substring(0, 12)}`);

      await this.waitForMysqlReady();
      await this.migrate();
      await this.seed();

      return {
        host: 'localhost',
        port: availablePort,
        user: 'root',
        password: 'password',
        database: this.config.databaseName
      };

    } catch (error) {
      throw new Error(`Failed to start MySQL container: ${error.message}`);
    }
  }

  /**
   * Run database migrations to set up schema
   */
  async migrate(): Promise<void> {
    if (!this.containerId) {
      throw new Error('Container not started. Call start() first.');
    }

    try {
      switch (this.config.migration.type) {
        case 'knex':
          await this.migrateWithKnex();
          break;
        case 'sql':
          assert(this.config.migration.sqlFilePath, 'sqlFilePath must be specified for sql migration type');
          await this.migrateWithSqlFile(this.config.migration.sqlFilePath);
          break;
        default:
          throw new Error(`Unknown migration type: ${(this.config.migration as any).type}`);
      }

      logger.info('Database migrations completed successfully');

    } catch (error) {
      logger.error('Migration failed:', error.message);
      throw new Error(`Database migration failed: ${error.message}`);
    }
  }

  /**
   * Seed the database with test data
   */
  async seed(): Promise<void> {
    if (!this.containerId) {
      throw new Error('Container not started. Call start() first.');
    }

    try {
      const knex = require('knex')({
        client: 'mysql2',
        connection: {
          host: 'localhost',
          port: this.port,
          user: 'root',
          password: 'password',
          database: this.config.databaseName
        },
        seeds: {
          directory: './src/seeds'
        }
      });

      try {
        await knex.seed.run();
        logger.debug('Knex seeds completed');
      } finally {
        await knex.destroy();
      }

    } catch (error) {
      logger.error('Seeding failed:', error.message);
      throw new Error(`Database seeding failed: ${error.message}`);
    }
  }

  /**
   * Clean up and destroy the test database container
   */
  async teardown(): Promise<void> {
    if (!this.containerId) {
      logger.info('No container to teardown');
      return;
    }

    try {
      // Stop and remove the container
      await execAsync(`docker rm -f ${this.containerId}`);
      logger.info(`Cleaned up MySQL test container: ${this.containerId.substring(0, 12)}`);
      this.containerId = null;

    } catch (error) {
      logger.error(`Failed to cleanup container: ${error.message}`);
      // Don't throw here - cleanup should be best effort
    }
  }

  /**
   * Get the container ID for this instance
   */
  getContainerId(): string | null {
    return this.containerId;
  }

  /**
   * Wait for MySQL to be ready to accept database connections
   */
  private async waitForMysqlReady(maxAttempts: number = 300, delayMs: number = 10): Promise<void> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Test actual database connection instead of just ping
        await execAsync(`docker exec ${this.containerId} mysql -u root -ppassword -e "SELECT 1" ${this.config.databaseName}`);
        logger.info('MySQL is ready for database connections');
        return;
      } catch (error) {
        if (attempt === maxAttempts) {
          throw new Error(`MySQL failed to start after ${maxAttempts} attempts: ${error.message}`);
        }

        logger.debug(`Waiting for MySQL database (attempt ${attempt}/${maxAttempts})...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }

  /**
   * Run knex migrations
   */
  private async migrateWithKnex(): Promise<void> {
    const knex = require('knex')({
      client: 'mysql2',
      connection: {
        host: 'localhost',
        port: this.port,
        user: 'root',
        password: 'password',
        database: this.config.databaseName
      },
      migrations: {
        directory: './src/migrations'
      }
    });

    try {
      await knex.migrate.latest();
      logger.debug('Knex migrations completed');
    } finally {
      await knex.destroy();
    }
  }

  /**
   * Run migrations from SQL file
   */
  private async migrateWithSqlFile(sqlFilePath: string): Promise<void> {
    try {
      // const sqlContent = await readFile(sqlFilePath, 'utf-8');
      logger.debug('restoreFromCheckpoint()')
      const fullFilePath = path.join(__dirname, sqlFilePath)
      const cmd = `docker cp ${fullFilePath} ${this.containerId}:/tmp/checkpoint.sql && \
      docker exec -i ${this.containerId} sh -c 'mysql -u root -ppassword ${this.config.databaseName} < /tmp/checkpoint.sql'`
      const { stdout, stderr } = await execAsync(cmd);

      if (stderr && !stderr.includes('warning')) {
        logger.warn('SQL migration warnings:', stderr);
      }

      logger.debug(`SQL migration from ${sqlFilePath} completed`);

    } catch (error) {
      throw new Error(`Failed to execute SQL file ${sqlFilePath}: ${error.message}`);
    }
  }

}

export interface IntegrationHarnessTigerBeetleConfig {
  tigerbeetleBinaryPath: string;
}

export interface TigerBeetleConfig {
  clusterId: bigint,
  address: Array<string>
}

export class IntegrationHarnessTigerBeetle implements IntegrationHarness {
  private process: import('child_process').ChildProcess | null = null;
  private config: IntegrationHarnessTigerBeetleConfig;
  private dataFilePath: string | null = null;

  constructor(config: IntegrationHarnessTigerBeetleConfig) {
    this.config = config;
  }

  public async start(): Promise<TigerBeetleConfig> {
    const clusterId = 0n;
    const port = await TestUtils.findAvailablePort(10243)

    // Create temporary data file for TigerBeetle
    const tempDir = os.tmpdir();
    const randomSuffix = randomBytes(8).toString('hex');
    this.dataFilePath = path.join(tempDir, `tigerbeetle-test-${randomSuffix}.tigerbeetle`);

    try {
      // Format the data file
      logger.info(`Formatting TigerBeetle data file: ${this.dataFilePath}`);
      const formatProcess = spawn(this.config.tigerbeetleBinaryPath, [
        'format',
        `--cluster=${clusterId}`,
        `--replica=0`,
        `--replica-count=1`,
        this.dataFilePath
      ]);

      await new Promise<void>((resolve, reject) => {
        let stdout = '';
        let stderr = '';
        
        formatProcess.stdout?.on('data', (data) => {
          stdout += data.toString();
        });
        
        formatProcess.stderr?.on('data', (data) => {
          stderr += data.toString();
        });
        
        formatProcess.on('close', (code) => {
          logger.info(`TigerBeetle format process closed with code: ${code}`);
          if (stdout) logger.info(`TigerBeetle format stdout: ${stdout}`);
          if (stderr) logger.info(`TigerBeetle format stderr: ${stderr}`);
          
          if (code === 0) {
            logger.info('TigerBeetle format completed successfully');
            resolve();
          } else {
            reject(new Error(`TigerBeetle format failed with code ${code}. stderr: ${stderr}, stdout: ${stdout}`));
          }
        });
        
        formatProcess.on('error', (error) => {
          logger.error(`TigerBeetle format process error: ${error.message}`);
          reject(error);
        });
      });

      // Start TigerBeetle server
      logger.info(`Starting TigerBeetle server on port ${port}`);
      this.process = spawn(this.config.tigerbeetleBinaryPath, [
        'start',
        `--addresses=127.0.0.1:${port}`,
        this.dataFilePath
      ]);

      // Add logging for the server process
      this.process.stdout?.on('data', (data) => {
        logger.debug(`[tigerbeetle stdout] ${data.toString()}`);
      });

      this.process.stderr?.on('data', (data) => {
        logger.debug(`[tigerbeetle stderr] ${data.toString()}`);
      });

      this.process.on('error', (error) => {
        logger.error(`[tigerbeetle error] ${error.message}`);
      });

      this.process.on('exit', (code, signal) => {
        logger.info(`TigerBeetle server process exited with code ${code} and signal ${signal}`);
      });

      return {
        clusterId,
        address: [`${port}`],
      };

    } catch (error) {
      throw new Error(`Failed to start TigerBeetle: ${error.message}`);
    }
  }

  public async teardown(): Promise<void> {
    if (this.process) {
      try {
        this.process.kill('SIGTERM');
        await new Promise<void>((resolve) => {
          if (this.process) {
            this.process.on('close', () => resolve());
            setTimeout(() => {
              if (this.process && !this.process.killed) {
                this.process.kill('SIGKILL');
              }
              resolve();
            }, 5000);
          } else {
            resolve();
          }
        });
        logger.info('TigerBeetle process terminated');
      } catch (error) {
        logger.error(`Failed to terminate TigerBeetle process: ${error.message}`);
      }
      this.process = null;
    }

    // Clean up data file
    if (this.dataFilePath) {
      try {
        const fs = require('fs').promises;
        await fs.unlink(this.dataFilePath);
        logger.info(`Cleaned up TigerBeetle data file: ${this.dataFilePath}`);
      } catch (error) {
        logger.warn(`Failed to cleanup data file: ${error.message}`);
      }
      this.dataFilePath = null;
    }
  }

  private async waitForTigerBeetleReady(port: number, maxAttempts: number = 100, delayMs: number = 50): Promise<void> {
    const net = require('net');

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await new Promise<void>((resolve, reject) => {
          const socket = net.createConnection(port, '127.0.0.1');
          socket.on('connect', () => {
            socket.end();
            resolve();
          });
          socket.on('error', reject);
          setTimeout(() => reject(new Error('Connection timeout')), 1000);
        });

        logger.info('TigerBeetle server is ready');
        return;
      } catch (error) {
        if (attempt === maxAttempts) {
          throw new Error(`TigerBeetle failed to start after ${maxAttempts} attempts: ${error.message}`);
        }

        logger.debug(`Waiting for TigerBeetle server (attempt ${attempt}/${maxAttempts})...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
}