import { DateTime } from 'luxon';
import { AttemptError, Job, JobStatistics } from './types/job.js';
import { PidginWorker } from './worker.js';
import { createTimeoutPromise } from './util/promise.js';
import { ValidationException } from './exceptions/validation.exception.js';
import { JobCompleter } from './job.completer.js';
import { ClientRetryPolicy } from './retry.policy.js';
import { CancelJobException } from './exceptions/cancel.job.exception.js';
import { SnoozeJobException } from './exceptions/snooze.job.exception.js';
import { Subject, Subscription } from 'rxjs';

export interface JobExecutorOptions {
  jobTimeout: number;
  scheduleInterval: number;
}

export type JobDoneHandler = (job: Job) => void;

export const DEFAULT_JOB_EXECUTOR_OPTIONS = {
  jobTimeout: 15_000,
  scheduleInterval: 1_000,
};

export class JobExecutor {
  private start: DateTime;
  private stats: JobStatistics;
  private abortController: AbortController;
  private jobDoneSubject: Subject<Job>;

  constructor(
    private job: Job,
    private worker: PidginWorker,
    private completer: JobCompleter,
    private clientRetryPolicy: ClientRetryPolicy,
    private options?: JobExecutorOptions,
  ) {
    if (!this.job) {
      throw new ValidationException('Job is not supplied');
    }

    if (!this.worker) {
      throw new ValidationException('Worker is not supplied');
    }

    if (!this.completer) {
      throw new ValidationException('Job completer is not supplied');
    }

    if (!this.options) {
      this.options = DEFAULT_JOB_EXECUTOR_OPTIONS;
    }

    if (this.options.jobTimeout <= 0) {
      throw new ValidationException('Job timeout is equal or less then 0');
    }

    if (this.options.scheduleInterval <= 0) {
      throw new ValidationException(
        'Schedule interval is equal or less then 0',
      );
    }

    if (!this.clientRetryPolicy) {
      throw new ValidationException('Client retry policy is not supplied');
    }

    this.jobDoneSubject = new Subject<Job>();
  }

  onJobDone(handler: JobDoneHandler): Subscription {
    return this.jobDoneSubject.subscribe(handler);
  }

  cancel() {
    if (this.abortController && this.job) {
      this.abortController.abort(
        new CancelJobException(
          this.job.id,
          `Job cancelled remotely, Job ID: ${this.job.id}`,
        ),
      );
    }
  }

  async execute(): Promise<void> {
    this.start = DateTime.utc();
    const scheduledAt = DateTime.fromJSDate(this.job.scheduledAt, {
      zone: 'UTC',
    });

    this.stats = {
      queueWaitDuration: this.start
        .diff(scheduledAt, 'milliseconds')
        .as('milliseconds'),
      completeDuration: 0,
      runDuration: 0,
    };

    try {
      const { promise, controller } = createTimeoutPromise(
        () => this.worker.work(this.job),
        this.options.jobTimeout,
      );
      this.abortController = controller;
      await promise;

      this.stats.runDuration = DateTime.utc()
        .diff(this.start, 'milliseconds')
        .as('milliseconds');

      await this.reportResult();
      this.emitJobDoneEvent();
    } catch (error) {
      await this.reportError(error);
    }
  }

  private emitJobDoneEvent() {
    this.jobDoneSubject.next(this.job);
    this.jobDoneSubject.complete();
    this.jobDoneSubject = null;
  }

  async reportResult() {
    try {
      await this.completer.jobSetStateIfRunning(this.stats, {
        id: this.job.id,
        finalizedAt: DateTime.utc().toJSDate(),
        state: 'completed',
      });
    } catch (err) {
      //TODO log error
    }
  }

  async reportError(error: Error) {
    this.jobDoneSubject?.complete();
    const attemptError = new AttemptError();
    attemptError.at = this.start.toJSDate();
    attemptError.attempt = this.job.attempt;
    attemptError.error = error.message;
    attemptError.trace = error.stack;

    if (error instanceof SnoozeJobException) {
      if (error.durationSeconds * 1_000 <= this.options.scheduleInterval) {
        await this.completer.jobSetStateIfRunning(this.stats, {
          id: this.job.id,
          maxAttempts: this.job.attempt + 1,
          state: 'scheduled',
          scheduledAt: DateTime.utc()
            .plus({ seconds: error.durationSeconds })
            .toJSDate(),
        });
        return;
      } else {
        await this.completer.jobSetStateIfRunning(this.stats, {
          id: this.job.id,
          scheduledAt: DateTime.utc()
            .plus({ seconds: error.durationSeconds })
            .toJSDate(),
          maxAttempts: this.job.attempt + 1,
          state: 'available',
        });
        return;
      }
    }

    if (error instanceof CancelJobException) {
      await this.completer.jobSetStateIfRunning(this.stats, {
        id: this.job.id,
        error: attemptError,
        finalizedAt: DateTime.utc().toJSDate(),
        state: 'cancelled',
      });
      return;
    }

    if (this.job.attempt >= this.job.maxAttempts) {
      await this.completer.jobSetStateIfRunning(this.stats, {
        id: this.job.id,
        finalizedAt: DateTime.utc().toJSDate(),
        error: attemptError,
        state: 'cancelled',
      });
      return;
    }
    const nextRetry = this.clientRetryPolicy(this.job);

    const nextRetryDiff = DateTime.fromJSDate(nextRetry, { zone: 'utc' })
      .diff(DateTime.utc(), 'millisecond')
      .as('millisecond');

    if (nextRetryDiff <= 0) {
      //TODO retry is not valid, log error
    }

    if (nextRetryDiff < this.options.scheduleInterval) {
      await this.completer.jobSetStateIfRunning(this.stats, {
        id: this.job.id,
        error: attemptError,
        scheduledAt: nextRetry,
        state: 'available',
      });
    } else {
      await this.completer.jobSetStateIfRunning(this.stats, {
        id: this.job.id,
        error: attemptError,
        scheduledAt: nextRetry,
        state: 'retryable',
      });
    }
  }
}
