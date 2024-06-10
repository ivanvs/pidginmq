import { Subject } from 'rxjs';
import { Executor, JobSetStateIfRunningParams } from './executor.js';
import { Job, JobStatistics } from './types/job.js';
import { DateTime } from 'luxon';
import { ValidationException } from './exceptions/validation.exception.js';
import { retry } from './util/promise.js';

export interface CompletedJobEvent {
  job: Job;
  jobStatistics: JobStatistics;
}

export type CompletedJobEventHandler = (handler: CompletedJobEvent) => void;

export class JobCompleter {
  private completedSubject: Subject<CompletedJobEvent>;

  constructor(private executor: Executor) {
    if (!this.executor) {
      throw new ValidationException('Executor is not supplied');
    }

    this.completedSubject = new Subject<CompletedJobEvent>();
  }

  async jobSetStateIfRunning(
    jobStatistics: JobStatistics,
    jobArgs: JobSetStateIfRunningParams,
  ): Promise<void> {
    const start = DateTime.utc();
    const job = await retry(
      async () => await this.executor.jobSetStateIfRunning(jobArgs),
      {
        retries: 3,
        retryIntervalMs: 500,
      },
    );

    jobStatistics.completeDuration = DateTime.utc()
      .diff(start, 'milliseconds')
      .as('milliseconds');

    this.completedSubject.next({
      job: job,
      jobStatistics: jobStatistics,
    });
  }

  subscribe(handler: CompletedJobEventHandler) {
    this.completedSubject.subscribe(handler);
  }
}
