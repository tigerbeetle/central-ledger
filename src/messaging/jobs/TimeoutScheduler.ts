import { CronJob } from 'cron';
import { logger } from '../../shared/logger';

const log = logger.child({ context: 'TimeoutScheduler' });

export interface ITimeoutScheduler {
  start(): Promise<void>;
  stop(): Promise<void>;
  isRunning(): boolean;
}

export interface TimeoutSchedulerConfig {
  cronTime: string;
  timeZone?: string;
  onTick: () => Promise<any>;
}

export class TimeoutScheduler implements ITimeoutScheduler {
  private cronJob: CronJob | null = null;
  private isRegistered = false;

  constructor(private config: TimeoutSchedulerConfig) {}

  async start(): Promise<void> {
    try {
      if (this.isRegistered) {
        await this.stop();
      }

      this.cronJob = CronJob.from({
        cronTime: this.config.cronTime,
        onTick: async () => {
          try {
            await this.config.onTick();
          } catch (err) {
            log.error('Error in timeout job tick:', err);
          }
        },
        start: false,
        timeZone: this.config.timeZone || 'UTC'
      });

      this.isRegistered = true;
      await this.cronJob.start();
      log.info('TimeoutScheduler started successfully');
    } catch (err) {
      log.error('Error starting TimeoutScheduler:', err);
      throw err;
    }
  }

  async stop(): Promise<void> {
    if (this.isRegistered && this.cronJob) {
      await this.cronJob.stop();
      this.cronJob = null;
      this.isRegistered = false;
      log.info('TimeoutScheduler stopped successfully');
    }
  }

  isRunning(): boolean {
    return this.isRegistered;
  }

  private setupSignalHandlers(): void {
    const gracefulShutdown = async (signal: string) => {
      log.info(`Received ${signal}, stopping timeout scheduler gracefully...`);
      try {
        await this.stop();
        log.info('Timeout scheduler stopped successfully');
      } catch (err) {
        log.error('Error stopping timeout scheduler during shutdown:', err);
      }
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  }

  // Setup signal handlers when created (except in test environment)
  static createWithSignalHandlers(config: TimeoutSchedulerConfig): TimeoutScheduler {
    const scheduler = new TimeoutScheduler(config);
    if (!process.env.NODE_ENV || process.env.NODE_ENV !== 'test') {
      scheduler.setupSignalHandlers();
    }
    return scheduler;
  }
}