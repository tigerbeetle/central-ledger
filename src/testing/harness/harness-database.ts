import { exec } from 'child_process';
import { promisify } from 'util';
import { readFile } from 'fs/promises';
import { logger } from '../../shared/logger';
import assert from 'assert';
import path from 'path';
import { TestUtils } from '../testutils';

import { spawn } from 'child_process';
import { randomBytes } from 'crypto';
const fs = require('fs').promises;
import os from 'os';
import { Harness } from './base';

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


export interface HarnessDatabaseConfig {
  databaseName: string;
  mysqlImage: string;
  memorySize: string;
  port: number;
  migration: MigrationOptions;
}

export class HarnessDatabase implements Harness {
  private containerId: string | null = null;
  private config: HarnessDatabaseConfig;
  private port: number = 0;

  constructor(config: HarnessDatabaseConfig) {
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

        logger.debug(`Waiting for MySQL database (attempt ${`${attempt}`.padStart(3)}/${maxAttempts})...`);
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