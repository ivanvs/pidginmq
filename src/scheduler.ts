import { DateTime } from 'luxon';
import { Executor } from './executor.js';
import { NotificationTopic } from './notifier.js';
import { Nullable } from './util/util.types.js';
import { ValidationException } from './exceptions/validation.exception.js';
import { logger } from './logger/logger.settings.js';

export interface SchedulerOptions {
  interval: number;
  limit: number;
}

export const DEFAULT_SCHEDULER_OPTIONS: SchedulerOptions = {
  interval: 1000,
  limit: 1000,
};

export class Scheduler {
  private schedulerTimer: Nullable<NodeJS.Timeout>;

  constructor(
    private options: SchedulerOptions,
    private executor: Executor,
  ) {
    if (!this.options) {
      this.options = { ...DEFAULT_SCHEDULER_OPTIONS };
    }

    if (this.options.interval <= 0) {
      throw new ValidationException('Interval is less or equal to 0');
    }

    if (this.options.limit <= 0) {
      throw new ValidationException('Limit is less or equal to 0');
    }

    if (!this.executor) {
      throw new ValidationException('Executor is not supplied');
    }
  }

  start() {
    logger.info('Starting scheduler');
    this.schedulerTimer = setInterval(
      async () => await this.runOnce(),
      this.options.interval,
    );
  }

  stop() {
    logger.info(`Stopping scheduler`);
    if (this.schedulerTimer) {
      clearInterval(this.schedulerTimer);
    }
  }

  private async runOnce(): Promise<void> {
    const numberOfScheduled = await this.executor.scheduleJob({
      insertTopic: NotificationTopic.NotificationTopicInsert,
      max: this.options.limit,
      now: DateTime.utc().toJSDate(),
    });

    logger.trace(
      `Scheduled number of jobs: ${JSON.stringify(numberOfScheduled)}`,
    );
  }
}
