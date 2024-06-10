import { Executor } from './executor.js';
import { DateTime } from 'luxon';
import { Job } from './types/job.js';
import { ClientRetryPolicy, RetryPolicies } from './retry.policy.js';
import { ValidationException } from './exceptions/validation.exception.js';

export interface JobRescuerOptions {
  interval: number;
  rescueAfter: number;
  batchSize: number;
  retryPolicy: ClientRetryPolicy;
}

export const DEFAULT_JOB_RESCUER_OPTIONS = {
  interval: 300_000, // 5 minutes
  rescueAfter: 15, // 15 minutes
  batchSize: 1_000,
  retryPolicy: RetryPolicies.builtinPolicies.fixed(1_000), // retry after 1s
};

export interface JobRescuerResult {
  numJobsCancelled: number;
  numJobsDiscarded: number;
  numJobsRetried: number;
}

interface JobRetryResult {
  shouldRetry: boolean;
  retryAt: Date;
}

export class JobRescuer {
  private rescuerIntervalTimeout: NodeJS.Timeout;

  constructor(
    private executor: Executor,
    private options: JobRescuerOptions,
  ) {
    if (!this.executor) {
      throw new ValidationException('Executor is not supplied');
    }

    if (!this.options) {
      this.options = DEFAULT_JOB_RESCUER_OPTIONS;
    }

    if (this.options.interval <= 0) {
      throw new ValidationException('Interval is less then or equal to 0');
    }

    if (this.options.rescueAfter <= 0) {
      throw new ValidationException('RescueAfter is equal or less then 0');
    }

    if (this.options.batchSize <= 0) {
      throw new ValidationException('BatchSize is equal or less then 0');
    }

    if (!this.options.retryPolicy) {
      throw new ValidationException('RetryPolicy is not specified');
    }
  }

  start() {
    if (!this.rescuerIntervalTimeout) {
      this.rescuerIntervalTimeout = setInterval(
        () => this.runOnce,
        this.options.interval,
      );
    }
  }

  async runOnce(): Promise<JobRescuerResult> {
    const stuckJobs = await this.executor.getStuckJob({
      max: this.options.batchSize,
      stuckHorizon: DateTime.utc().minus(this.options.rescueAfter).toJSDate(),
    });
    let numJobsCancelled = 0;
    let numJobsRetried = 0;
    let numJobsDiscarded = 0;

    if (stuckJobs.length > 0) {
      const additionalParams = stuckJobs.map((job) => {
        if (job.metadata.cancelAttemptedAt) {
          numJobsCancelled++;
          return {
            finalizedAt: DateTime.utc().toJSDate(),
            scheduledAt: job.scheduledAt,
            state: 'cancelled',
          };
        }

        const retryResult = this.makeRetryDecision(job);
        if (retryResult.shouldRetry) {
          numJobsRetried++;
          return {
            scheduledAt: retryResult.retryAt,
            state: 'retryable',
          };
        } else {
          numJobsDiscarded++;
          return {
            finalizedAt: DateTime.utc().toJSDate(),
            scheduledAt: job.scheduledAt,
            state: 'discarded',
          };
        }
      });
      const params = {
        ids: stuckJobs.map((x) => x.id),
        error: stuckJobs.map((x) => ({
          at: DateTime.utc().toJSDate(),
          attempt: Math.max(x.attempt, 0),
          error: 'Stuck job rescued by Rescuer',
          trace: 'TODO',
        })),
        finalizedAt: additionalParams.map((x) => x.finalizedAt),
        scheduledAt: additionalParams.map((x) => x.scheduledAt),
        state: additionalParams.map((x) => x.state),
      };

      await this.executor.rescueManyJobs(params);
    }

    return {
      numJobsCancelled,
      numJobsRetried,
      numJobsDiscarded,
    };
  }

  private makeRetryDecision(job: Job): JobRetryResult {
    const retryAt = this.options.retryPolicy(job);
    return { shouldRetry: job.attempt < Math.max(0, job.maxAttempts), retryAt };
  }

  stop() {
    if (this.rescuerIntervalTimeout) {
      clearInterval(this.rescuerIntervalTimeout);
      this.rescuerIntervalTimeout = null;
    }
  }
}
