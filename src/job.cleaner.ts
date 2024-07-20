import { Executor } from './executor.js';
import { DateTime } from 'luxon';
import { ValidationException } from './exceptions/validation.exception.js';
import { logger } from './logger/logger.settings.js';

export interface JobCleanerOptions {
  cancelledJobRetentionPeriod: number;
  completedJobRetentionPeriod: number;
  discardedJobRetentionPeriod: number;
  interval: number;
  batchSize: number;
}

export const DEFAULT_BATCH_SIZE = 1_000;

export const DEFAULT_JOB_CLEANER_OPTIONS = {
  cancelledJobRetentionPeriod: 86_400_000, //1 day
  completedJobRetentionPeriod: 86_400_000, //1 day
  discardedJobRetentionPeriod: 86_400_000, //1 day
  interval: 600_000, // 10 minutes
  batchSize: DEFAULT_BATCH_SIZE,
};

export class JobCleaner {
  private cleanerIntervalTimout: NodeJS.Timeout;

  constructor(
    private executor: Executor,
    private options: JobCleanerOptions,
  ) {
    if (!this.executor) {
      throw new ValidationException('Executor is not supplied');
    }

    if (!this.options) {
      this.options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
    }

    if (this.options.cancelledJobRetentionPeriod <= 0) {
      throw new ValidationException(
        'cancelledJobRetentionPeriod is less then or equal to 0',
      );
    }

    if (this.options.completedJobRetentionPeriod <= 0) {
      throw new ValidationException(
        'completedJobRetentionPeriod is less then or equal to 0',
      );
    }

    if (this.options.discardedJobRetentionPeriod <= 0) {
      throw new ValidationException(
        'discardedJobRetentionPeriod is less then or equal to 0',
      );
    }

    if (this.options.interval <= 0) {
      throw new ValidationException('interval is less then or equal to 0');
    }

    if (this.options.batchSize <= 0) {
      throw new ValidationException('batchSize is less then or equal to 0');
    }
  }

  start() {
    logger.info(`Starting job cleaner`);
    if (!this.cleanerIntervalTimout) {
      this.cleanerIntervalTimout = setInterval(
        () => this.runOnce(),
        this.options.interval,
      );
    }
  }

  async runOnce(): Promise<number> {
    return this.executor.jobDeleteBefore({
      cancelledFinalizedAtHorizon: DateTime.utc()
        .minus(this.options.cancelledJobRetentionPeriod)
        .toJSDate(),
      completedFinalizedAtHorizon: DateTime.utc()
        .minus(this.options.completedJobRetentionPeriod)
        .toJSDate(),
      discardedFinalizedAtHorizon: DateTime.utc()
        .minus(this.options.discardedJobRetentionPeriod)
        .toJSDate(),
      max: this.options.batchSize,
    });
  }

  stop() {
    logger.info('Stopping job cleaner');
    if (this.cleanerIntervalTimout) {
      clearInterval(this.cleanerIntervalTimout);
      this.cleanerIntervalTimout = null;
    }
  }
}
