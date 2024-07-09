import { Subscription } from 'rxjs';
import { Executor } from './executor.js';
import { DbNotification, Notifier } from './notifier.js';
import {
  ControlAction,
  InsertJobPayload,
  Job,
  JobControlPayload,
} from './types/job.js';
import { Nullable } from './util/util.types.js';
import { ClientRetryPolicy } from './retry.policy.js';
import { PidginWorker, Workers } from './worker.js';
import { JobCompleter } from './job.completer.js';
import { JobExecutor } from './job.executor.js';
import { ValidationException } from './exceptions/validation.exception.js';
import throttle from 'lodash.throttle';
import { sleep } from './util/promise.js';

export interface ProducerOptions {
  queue: string;
  fetchPollInterval: number;
  fetchCoolDown: number;
  clientId: string;
  maxWorkerCount: number;
  jobTimeout: number;
  scheduleInterval: number;
  paused: boolean;
}

export const DEFAULT_PRODUCER_OPTIONS: ProducerOptions = {
  queue: 'pidgin_mq',
  fetchPollInterval: 1_000,
  fetchCoolDown: 500,
  clientId: 'pidgin_mq_1',
  maxWorkerCount: 100,
  jobTimeout: 1_000,
  scheduleInterval: 1_000,
  paused: false,
};

export class Producer {
  private jobControlSubscription: Subscription;
  private insertJobSubcsription: Subscription;
  private fetchTimeout: Nullable<NodeJS.Timeout>;
  private activeJobs: Map<number, JobExecutor>;
  private throttleFetch;
  private paused;

  constructor(
    private notifier: Notifier,
    private retryPolicy: ClientRetryPolicy,
    private workers: Workers,
    private executor: Executor,
    private completer: JobCompleter,
    private options: ProducerOptions,
  ) {
    if (!this.notifier) {
      throw new ValidationException('Notifier is not supplied');
    }

    if (!this.retryPolicy) {
      throw new ValidationException('Retry policy is not supplied');
    }

    if (!this.workers) {
      throw new ValidationException('Workers are not supplied');
    }

    if (!this.executor) {
      throw new ValidationException('Executor is not supplied');
    }

    if (!this.completer) {
      throw new ValidationException('Completer is not supplied');
    }

    if (!this.options) {
      this.options = DEFAULT_PRODUCER_OPTIONS;
    }

    if (this.options.jobTimeout <= 0) {
      throw new ValidationException(
        'Job timeout should not be equal or less then 0',
      );
    }

    if (this.options.fetchCoolDown <= 0) {
      throw new ValidationException(
        'Fetch cool down should not be equal or less then 0',
      );
    }

    if (this.options.fetchPollInterval <= 0) {
      throw new ValidationException(
        'Fetch poll interval should not be equal or less then 0',
      );
    }

    if (this.options.maxWorkerCount <= 0) {
      throw new ValidationException(
        'Max worker count should not be equal or less then 0',
      );
    }

    if (this.options.scheduleInterval <= 0) {
      throw new ValidationException(
        'Schedule interval should not be equal or less then 0',
      );
    }

    if (!this.options.clientId) {
      throw new ValidationException('Client id cannot be empty');
    }

    if (!this.options.queue) {
      throw new ValidationException('Queue cannot be empty');
    }

    this.paused = this.options.paused;
    this.activeJobs = new Map<number, JobExecutor>();
    this.throttleFetch = throttle(
      () => this.fetch(),
      this.options.fetchCoolDown,
    );
  }

  async start(): Promise<void> {
    try {
      this.jobControlSubscription = this.notifier.onJobControl(
        this.onJobControl,
      );
      const queue = await this.executor.queueCreateOrSetUpdateAt({
        metadata: '{}',
        name: this.options.queue,
      });

      const initiallyPaused = queue && queue.pausedAt;
      this.paused = initiallyPaused;

      this.fetchLoop();
    } catch (e) {
      await this.stop();
    }
  }

  onJobControl(notification: DbNotification): void {
    const parsed: JobControlPayload = JSON.parse(notification.payload);

    switch (parsed.action) {
      case ControlAction.Cancel:
        if (parsed.jobId > 0 && this.options.queue === parsed.queue) {
          this.cancelJob(parsed.jobId);
        }
        break;
      case ControlAction.Pause:
        if (
          parsed.queue !== '*' &&
          parsed.queue !== this.options.queue &&
          !this.paused
        ) {
          this.paused = true;
        }
        break;
      case ControlAction.Resume:
        if (
          parsed.queue !== '*' &&
          parsed.queue !== this.options.queue &&
          this.paused
        ) {
          this.paused = false;
        }
        break;
    }
  }

  private addActiveJob(id: number, jobExecutor: JobExecutor) {
    if (!this.activeJobs.has(id)) {
      this.activeJobs.set(id, jobExecutor);
    }
  }

  private removeActiveJob(id: number) {
    if (this.activeJobs.has(id)) {
      this.activeJobs.delete(id);
    }
  }

  private cancelJob(id: number) {
    const jobExecutor = this.activeJobs.get(id);
    if (jobExecutor) {
      jobExecutor.cancel();
    }
  }

  private maxJobToFetch(): number {
    return Math.max(this.options.maxWorkerCount - this.activeJobs.size, 0);
  }

  private jobDoneHandler(job: Job) {
    this.removeActiveJob(job.id);
  }

  private jobFailHandler(job: Job) {
    this.removeActiveJob(job.id);
  }

  private fetchLoop() {
    if (!this.insertJobSubcsription) {
      this.insertJobSubcsription = this.notifier.onJobInsert((notification) =>
        this.onJobInsert(notification),
      );
    }
    if (!this.fetchTimeout) {
      this.fetchTimeout = setInterval(
        this.throttleFetch,
        this.options.fetchPollInterval,
      );
    }
  }

  private async fetch(): Promise<void> {
    try {
      if (this.paused) {
        // queue is paused skip processing;
        return;
      }

      const jobs = await this.executor.jobGetAvailable({
        attemptedBy: this.options.clientId,
        max: this.maxJobToFetch(),
        queue: this.options.queue,
      });
      jobs.forEach((job) => {
        const handler = this.workers.getWorker(job.kind);
        const worker = new PidginWorker(handler);
        const jobExecutor = new JobExecutor(
          job,
          worker,
          this.completer,
          this.retryPolicy,
          {
            jobTimeout: this.options.jobTimeout,
            scheduleInterval: this.options.scheduleInterval,
          },
        );
        jobExecutor.onJobDone((job: Job) => this.jobDoneHandler(job));
        jobExecutor.onJobStopped((job: Job) => this.jobFailHandler(job));
        jobExecutor.execute();
        this.addActiveJob(job.id, jobExecutor);
        return jobExecutor;
      });
    } catch (e) {
      console.error(e);
    }
  }

  onJobInsert(notification: DbNotification): void {
    const parsed: InsertJobPayload = JSON.parse(notification.payload);

    if (parsed.queue === this.options.queue) {
      this.throttleFetch();
    }
  }

  async stop(): Promise<void> {
    while (this.activeJobs.size) {
      await sleep(1000);
    }

    if (this.jobControlSubscription) {
      this.jobControlSubscription.unsubscribe();
      this.jobControlSubscription = null;
    }

    if (this.insertJobSubcsription) {
      this.insertJobSubcsription.unsubscribe();
      this.insertJobSubcsription = null;
    }

    if (this.fetchTimeout) {
      clearInterval(this.fetchTimeout);
      this.fetchTimeout = null;
    }

    if (this.throttleFetch) {
      this.throttleFetch.cancel();
    }
  }
}
