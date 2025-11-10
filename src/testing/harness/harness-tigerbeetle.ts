import { exec } from 'child_process';
import path from 'path';
import { promisify } from 'util';
import { logger } from '../../shared/logger';
import { TestUtils } from '../testutils';

import { spawn } from 'child_process';
import { randomBytes } from 'crypto';
import os from 'os';
import { Harness } from './base';
const fs = require('fs').promises;

export interface HarnessTigerBeetleConfig {
  tigerbeetleBinaryPath: string;
}

export interface TigerBeetleConfig {
  clusterId: bigint,
  address: Array<string>
}

export class HarnessTigerBeetle implements Harness {
  private process: import('child_process').ChildProcess | null = null;
  private config: HarnessTigerBeetleConfig;
  private dataFilePath: string | null = null;

  constructor(config: HarnessTigerBeetleConfig) {
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