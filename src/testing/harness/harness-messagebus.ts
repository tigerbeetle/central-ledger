import { exec } from 'child_process';
import { promisify } from 'util';
import { logger } from '../../shared/logger';
import { Harness } from './base';

const execAsync = promisify(exec);

export interface MessageBusConfig {
  brokerAddress: string;
  containerName: string;
}

export interface HarnessMessageBusConfig {
  containerName?: string;
  port?: number;
  internalPort?: number;
  networkName?: string;
}

export class HarnessMessageBus implements Harness {
  private containerId: string | null = null;
  private config: HarnessMessageBusConfig;
  private containerName: string;

  constructor(config: HarnessMessageBusConfig = {}) {
    this.config = config;
    this.containerName = config.containerName || `cl-test-redpanda-${Date.now()}`;
  }

  async start(): Promise<MessageBusConfig> {
    const port = this.config.port || 9092;
    const internalPort = this.config.internalPort || 9192;
    const networkName = this.config.networkName || 'cl_test_network';

    logger.info(`HarnessMessageBus - start() - containerName: ${this.containerName}`);

    try {
      // Clean up any existing container with the same name
      await execAsync(`docker rm -f ${this.containerName}`, { encoding: 'utf-8' }).catch(() => {
        // Ignore errors if container doesn't exist
      });

      // Create network if it doesn't exist
      await execAsync(`docker network create ${networkName}`, { encoding: 'utf-8' }).catch(() => {
        // Ignore errors if network already exists
      });

      // Check if container already exists and try to start it
      try {
        await execAsync(`docker inspect ${this.containerName}`, { encoding: 'utf-8' });
        await execAsync(`docker start ${this.containerName}`, { encoding: 'utf-8' });
        logger.debug('HarnessMessageBus - start() - restarted existing container');
      } catch {
        // Container doesn't exist, create a new one
        const dockerCommand = [
          'docker', 'run', '-d',
          '--name', this.containerName,
          '--network', networkName,
          '-p', `${port}:9092`,
          '-p', `${internalPort}:29092`,
          '--health-cmd="rpk cluster info"',
          '--health-interval=1s',
          '--health-timeout=2s',
          '--health-retries=100',
          '--health-start-period=2s',
          'docker.io/redpandadata/redpanda:latest',
          'redpanda', 'start',
          '--smp', '1',
          '--memory', '1G',
          '--reserve-memory', '0M',
          '--overprovisioned',
          '--node-id', '0',
          '--check=false',
          '--kafka-addr', 'internal://0.0.0.0:29092,external://0.0.0.0:9092',
          '--advertise-kafka-addr', `internal://${this.containerName}:29092,external://localhost:9092`
        ];

        const { stdout } = await execAsync(dockerCommand.join(' '), { encoding: 'utf-8' });
        this.containerId = stdout.trim();
        logger.info(`Started Redpanda test container: ${this.containerId.substring(0, 12)}`);
      }

      await this.waitForHealth();
      await this.createTopics();

      return {
        brokerAddress: `localhost:${port}`,
        containerName: this.containerName
      };

    } catch (error) {
      logger.error(`Failed to start Redpanda container: ${error.message}`);
      throw new Error(`Failed to start Redpanda container: ${error.message}`);
    }
  }

  async teardown(): Promise<void> {
    if (!this.containerName) {
      logger.info('HarnessMessageBus - No container to teardown');
      return;
    }

    try {
      logger.info(`HarnessMessageBus - teardown() - stopping and removing container: ${this.containerName}`);
      await execAsync(`docker stop ${this.containerName}`, { encoding: 'utf-8' });
      await execAsync(`docker rm -f ${this.containerName}`, { encoding: 'utf-8' });
      this.containerId = null;
      logger.info('HarnessMessageBus - teardown() - complete');

    } catch (error) {
      logger.error(`Failed to cleanup Redpanda container: ${error.message}`);
      // Don't throw here - cleanup should be best effort
    }
  }

  private async waitForHealth(timeoutSec: number = 60): Promise<void> {
    const start = Date.now();
    logger.debug(`HarnessMessageBus - waitForHealth() - waiting for container: ${this.containerName} to become healthy`);

    while (true) {
      try {
        const { stdout } = await execAsync(
          `docker inspect --format='{{.State.Health.Status}}' ${this.containerName}`,
          { encoding: 'utf-8' }
        );
        const status = stdout.trim();

        if (status === 'healthy') {
          logger.debug('HarnessMessageBus - waitForHealth() - container is healthy');
          return;
        }
      } catch (error) {
        // Ignore inspection errors and keep trying
      }

      if (Date.now() - start > timeoutSec * 1000) {
        throw new Error(`Timed out waiting for ${this.containerName} to become healthy`);
      }

      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  private async createTopics(): Promise<void> {
    const topics = [
      'transfer-batch-prepare',
      'transfer-batch-fulfil',
      'notification-batch',
      'topic-transfer-prepare',
      'topic-transfer-position',
      'topic-transfer-fulfil',
      'topic-notification-event',
      'topic-transfer-get',
      'topic-admin-transfer',
      'topic-transfer-position-batch',
      'topic-deferredsettlement-close',
      'topic-quotes-post',
      'topic-quotes-put',
      'topic-quotes-get',
      'topic-bulkquotes-post',
      'topic-bulkquotes-put',
      'topic-bulkquotes-get',
      'topic-fx-quotes-post',
      'topic-fx-quotes-put',
      'topic-fx-quotes-get'
    ];

    logger.debug(`HarnessMessageBus - createTopics() - creating ${topics.length} kafka topics`);

    const commands = topics.map(topic =>
      `docker exec ${this.containerName} rpk topic create ${topic} --brokers localhost:9092`
    );

    await Promise.all(commands.map(cmd =>
      execAsync(cmd, { encoding: 'utf-8' }).catch(error => {
        // Ignore the "already created" error
        if (error.stdout && !error.stdout.includes('The topic has already been created')) {
          throw error;
        }
      })
    ));

    logger.debug('HarnessMessageBus - createTopics() - complete');
  }

  getContainerId(): string | null {
    return this.containerId;
  }

  getContainerName(): string {
    return this.containerName;
  }
}
