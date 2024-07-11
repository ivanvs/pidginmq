import { DateTime } from 'luxon';
import {
  Executor,
  InsertJobParams,
  InsertRepetableJobParams,
  JobQueryParams,
} from './executor.js';
import {
  PostgresDbDriver,
  PostgresDbOptions,
} from './postgres/pg.db.driver.js';
import { DbDriver } from './types/db.driver.js';
import {
  ControlAction,
  Job,
  JobControlPayload,
  JobStatistics,
} from './types/job.js';
import { Subject, Subscription } from 'rxjs';
import { Event, EventKind } from './types/event.js';
import { NotificationTopic, Notifier } from './notifier.js';
import { Workers } from './worker.js';
import { ClientRetryPolicy, RetryPolicies } from './retry.policy.js';
import { NoLogger, PidginMqLogger } from './logger/logger.js';
import { ValidationException } from './exceptions/validation.exception.js';
import { Producer } from './producer.js';
import { Elector, LeadershipEvent } from './elector.js';
import { CompletedJobEvent, JobCompleter } from './job.completer.js';
import { DEFAULT_BATCH_SIZE, JobCleaner } from './job.cleaner.js';
import { JobRescuer } from './job.rescuer.js';
import { Scheduler } from './scheduler.js';
import { CronExpression, parseExpression } from 'cron-parser';
import { SCHEDULED_JOB_WORKER } from './scheduled.job.worker.js';
import { Queue } from './types/queue.js';

export interface QueueConfig {
  maxWorkers: number;
}

export interface ClientOptions {
  dbConfig: PostgresDbOptions;
  cancelledJobRetentionPeriod: number;
  completedJobRetentionPeriod: number;
  discardedJobRetentionPeriod: number;
  fetchCoolDown: number;
  fetchPollInterval: number;
  id: string;
  jobTimeout: number;
  rescueStuckJobsAfter: number;
  retryPolicy: ClientRetryPolicy;
  schedulerInterval: number;
  workers: Workers;
  queues: Map<string, QueueConfig>;
  logger: PidginMqLogger;
}

export const DEFAULT_CLIENT_OPTIONS = {
  cancelledJobRetentionPeriod: 24 * 60 * 60 * 1_000, // 24h
  completedJobRetentionPeriod: 24 * 60 * 60 * 1_000, // 24h
  discardedJobRetentionPeriod: 24 * 60 * 60 * 1_000, // 24h
  fetchCoolDown: 200,
  fetchPollInterval: 500,
  id: 'default',
  jobTimeout: 60 * 1_000,
  rescueStuckJobsAfter: 60 * 60 * 1_000,
  retryPolicy: RetryPolicies.builtinPolicies.fixed(5_000),
  schedulerInterval: 1_000,
  logger: NoLogger,
};

export const SCHEDULED_JOB_QUEUE = '__pidginmq_scheduler';
export const SCHEDULED_JOB_KIND = '__pidginmq_scheduler_kind';

export interface ScheduledJobArgs {
  options: InsertRepetableJobParams;
  scheduledTimes: number;
  lastRun: string;
}

export type SubscriptionHandler = (event: Event) => void;
export type SubscriptionErrorHandler = (error: any) => void;
export type SubscriptionCompletedHandler = () => void;

export interface SubscribeOptions {
  onNext: SubscriptionHandler;
  onError?: SubscriptionErrorHandler;
  onCompleted?: SubscriptionCompletedHandler;
}

export class Client {
  private options: ClientOptions;
  private db: DbDriver;
  private executor: Executor;
  private eventSubject: Subject<Event>;
  private workers: Workers;
  private retryPolicy: ClientRetryPolicy;
  private logger: PidginMqLogger;
  private notifier: Notifier;
  private producersByName: Map<string, Producer>;
  private statsAggregate: JobStatistics;
  private statsNumJobs: number;
  private elector: Elector;
  private completer: JobCompleter;
  private jobCleaner: JobCleaner;
  private jobRescuer: JobRescuer;
  private jobScheduler: Scheduler;

  constructor(options: ClientOptions) {
    this.options = options;

    if (!this.options.id) {
      throw new ValidationException('ID is not supplied');
    }

    if (!this.options.logger) {
      this.logger = NoLogger;
    } else {
      this.logger = this.options.logger;
    }

    if (!this.options.retryPolicy) {
      this.retryPolicy = RetryPolicies.builtinPolicies.fixed(1_000);
    } else {
      this.retryPolicy = this.options.retryPolicy;
    }

    if (this.options.jobTimeout <= 0) {
      throw new ValidationException('Job timout is equal or less then 0');
    }

    if (this.options.rescueStuckJobsAfter <= 0) {
      throw new ValidationException(
        'Rescue stuck job after is equal or less then 0',
      );
    }

    if (this.options.schedulerInterval <= 0) {
      throw new ValidationException(
        'Scheduler interval is equal or less then 0',
      );
    }

    if (this.options.fetchPollInterval <= 0) {
      throw new ValidationException(
        'Fetch poll interval is equal or less then 0',
      );
    }

    if (this.options.fetchCoolDown <= 0) {
      throw new ValidationException(
        'Fetch cool down interval is equal or less then 0',
      );
    }

    if (this.options.cancelledJobRetentionPeriod <= 0) {
      throw new ValidationException(
        'Cancelled job retention period is equal or less then 0',
      );
    }

    if (this.options.completedJobRetentionPeriod <= 0) {
      throw new ValidationException(
        'Completed job retention period is equal or less then 0',
      );
    }

    if (this.options.discardedJobRetentionPeriod <= 0) {
      throw new ValidationException(
        'Discarded job retention period is equal or less then 0',
      );
    }

    this.options.queues.forEach((value) => {
      if (value?.maxWorkers < 1) {
        throw new ValidationException(
          `Invalid number of workers for queue: ${value?.maxWorkers}`,
        );
      }
    });
    this.options.queues.set(SCHEDULED_JOB_KIND, { maxWorkers: 1 });

    if (this.options.workers && !this.options.queues) {
      throw new ValidationException('Workers must be set if queues are set');
    } else {
      this.workers = this.options.workers;
    }

    this.db = new PostgresDbDriver(this.options.dbConfig, this.logger);
    this.executor = new Executor(this.db);

    this.workers.addWorker(
      SCHEDULED_JOB_KIND,
      SCHEDULED_JOB_WORKER(this.executor),
    );

    this.completer = new JobCompleter(this.executor);
    this.producersByName = new Map<string, Producer>();
    this.notifier = new Notifier(this.db);
    this.elector = new Elector(
      this.executor,
      this.options.id,
      'default',
      5_000,
      10_000,
      this.notifier,
    );

    this.jobCleaner = new JobCleaner(this.executor, {
      batchSize: DEFAULT_BATCH_SIZE,
      interval: this.options.schedulerInterval,
      cancelledJobRetentionPeriod: this.options.cancelledJobRetentionPeriod,
      completedJobRetentionPeriod: this.options.completedJobRetentionPeriod,
      discardedJobRetentionPeriod: this.options.discardedJobRetentionPeriod,
    });

    this.jobRescuer = new JobRescuer(this.executor, {
      batchSize: DEFAULT_BATCH_SIZE,
      interval: this.options.schedulerInterval,
      rescueAfter: this.options.rescueStuckJobsAfter,
      retryPolicy: this.retryPolicy,
    });

    this.jobScheduler = new Scheduler(
      { interval: this.options.schedulerInterval, limit: 10_000 },
      this.executor,
    );

    this.producersByName = new Map<string, Producer>();
    this.options.queues.forEach((value, key) => {
      this.producersByName.set(key, this.createProducer(key, value.maxWorkers));
    });
  }

  private createProducer(queueName: string, maxWorkers: number): Producer {
    return new Producer(
      this.notifier,
      this.retryPolicy,
      this.workers,
      this.executor,
      this.completer,
      {
        clientId: this.options.id,
        fetchCoolDown: this.options.fetchCoolDown,
        fetchPollInterval: this.options.fetchPollInterval,
        jobTimeout: this.options.jobTimeout,
        scheduleInterval: this.options.schedulerInterval,
        queue: queueName,
        maxWorkerCount: maxWorkers,
        paused: false,
      },
    );
  }

  async start() {
    this.statsAggregate = {
      completeDuration: 0,
      queueWaitDuration: 0,
      runDuration: 0,
    };
    this.statsNumJobs = 0;

    await this.db.open();

    if (!this.eventSubject) {
      this.eventSubject = new Subject<Event>();
    }

    this.completer.subscribe((event) => this.jobCompleterHandler(event));
    this.notifier.start();
    this.elector.subscribe(
      async (event) => await this.handleLeadershipChange(event),
    );
    this.elector.run();

    this.producersByName.forEach((value) => {
      value.start();
    });
  }

  private async handleLeadershipChange(event: LeadershipEvent) {
    this.logger.debug(`New leadership status: ${event.isLeader}`);

    if (event.isLeader) {
      this.jobRescuer.start();
      this.jobCleaner.start();
      this.producersByName.forEach((value) => {
        value.start();
      });
    } else {
      this.jobRescuer.stop();
      this.jobCleaner.stop();
      await this.stopProducers();
    }
  }

  private publishEvent(job: Job, stats: JobStatistics) {
    let kind = EventKind.JobCompleted;
    switch (job.state) {
      case 'cancelled':
        kind = EventKind.JobCancelled;
        break;
      case 'completed':
        kind = EventKind.JobCompleted;
        break;
      case 'scheduled':
        kind = EventKind.JobSnoozed;
        break;
      case 'available':
      case 'discarded':
      case 'retryable':
      case 'running':
        kind = EventKind.JobFailed;
        break;
      default:
        this.logger.error('Unhandled job state. Bug in PidginMQ');
        return;
    }
    if (this.eventSubject) {
      this.eventSubject.next({ job: job, kind, stats });
    }
  }

  private jobCompleterHandler(event: CompletedJobEvent) {
    const stats = event.jobStatistics;
    this.statsAggregate.completeDuration += stats.completeDuration;
    this.statsAggregate.queueWaitDuration += stats.queueWaitDuration;
    this.statsAggregate.runDuration += stats.runDuration;
    this.statsNumJobs++;

    this.publishEvent(event.job, stats);
  }

  private async stopProducers() {
    for (const [key, value] of this.producersByName) {
      this.logger.debug(`Stopping producer for queue: ${key}`);
      await value.stop();
    }
  }

  async stop() {
    this.notifier.stop();
    this.jobRescuer.stop();
    this.jobCleaner.stop();
    this.jobScheduler.stop();
    await this.stopProducers();
    await this.elector.stop();

    await this.db.close();

    this.eventSubject.complete();
    this.eventSubject = null;
  }

  addJob(options: InsertJobParams): Promise<Job> {
    return this.executor.insertJob(options);
  }

  deleteJob(id: number): Promise<Job> {
    return this.executor.jobDelete(id);
  }

  queryJobs(options: JobQueryParams): Promise<Job[]> {
    return this.executor.queryJobs(options);
  }

  retryJob(id: number): Promise<Job> {
    return this.executor.retryJob(id);
  }

  getJob(id: number): Promise<Job> {
    return this.executor.getJobById(id);
  }

  private parseCron(cron): CronExpression {
    try {
      return parseExpression(cron, { utc: true });
    } catch (e) {
      throw new ValidationException(e.toString());
    }
  }

  async scheduleJob(options: InsertRepetableJobParams): Promise<Job> {
    let scheduledTime: Date;
    if (options.repeat && options.repeat.cron) {
      const expression = this.parseCron(options.repeat.cron);
      scheduledTime = expression.next().toDate();
    } else if (options?.repeat?.every <= 0) {
      throw new ValidationException(
        'Repeat field every should be supplied since cron is not set',
      );
    } else {
      scheduledTime = DateTime.utc()
        .plus({ seconds: options.repeat.every })
        .toJSDate();
    }

    if (options?.repeat?.limit <= 0) {
      throw new ValidationException(
        'Repeat field limit cannot be less then or equal to 0',
      );
    }

    const scheduledArgs: ScheduledJobArgs = {
      options,
      scheduledTimes: 0,
      lastRun: DateTime.utc().toISOTime(),
    };
    const scheduledParams: InsertJobParams = {
      metadata: scheduledArgs,
      queue: SCHEDULED_JOB_QUEUE,
      maxAttempts: 3,
      priority: 1,
      scheduletAt: scheduledTime,
      kind: SCHEDULED_JOB_KIND,
      args: {},
      tags: [],
    };

    const scheduledJob = await this.executor.insertJob(scheduledParams);

    const jobMetadata = {
      scheduledJobId: scheduledJob.id,
      ...options.metadata,
    };
    const nextJobParams: InsertJobParams = {
      metadata: jobMetadata,
      queue: options.queue,
      maxAttempts: options.maxAttempts,
      priority: options.priority,
      scheduletAt: scheduledTime,
      kind: options.kind,
      args: options.args,
      tags: options.tags,
    };
    return this.executor.insertJob(nextJobParams);
  }

  cancelJob(id: number): Promise<Job> {
    return this.executor.cancelJob({
      id,
      cancelAttemtedAt: DateTime.utc().toJSDate(),
      jobControlTopic: NotificationTopic.NotificationTopicJobControl,
    });
  }

  listJobs(params: JobQueryParams) {
    return this.executor.queryJobs(params);
  }

  subscribe(options: SubscribeOptions): Subscription {
    return this.eventSubject.subscribe({
      next: options.onNext,
      error: options.onError,
      complete: options.onCompleted,
    });
  }

  addWorker(kind: string, handler: (job: Job) => void) {
    this.workers.addWorker(kind, handler);
  }

  addQueue(queueName: string, queueConfig: QueueConfig) {
    if (queueName.length < 1 && queueName.length > 512) {
      throw new ValidationException(
        'Queue name must have length between 1 and 512 characters',
      );
    }

    if (!queueConfig) {
      throw new ValidationException('Queue config is not supplied');
    }

    const producer = this.createProducer(queueName, queueConfig.maxWorkers);
    producer.start();
    this.producersByName.set(queueName, producer);
  }

  async resumeQueue(name: string): Promise<Queue> {
    const queue = await this.executor.queueResume(name);
    await this.notifyQueueStatus(ControlAction.Resume, name);
    return queue;
  }

  async pauseQueue(name: string): Promise<Queue> {
    const queue = await this.executor.queuePause(name);
    await this.notifyQueueStatus(ControlAction.Pause, name);
    return queue;
  }

  private notifyQueueStatus(action: ControlAction, queue: string) {
    const jobControlPayload: JobControlPayload = { action: action, queue };
    this.executor.pgNotify(
      NotificationTopic.NotificationTopicJobControl,
      jobControlPayload,
    );
  }

  async listQueues(limit: number): Promise<Queue[]> {
    return this.executor.queryQueues(limit);
  }

  async getQueue(name: string): Promise<Queue> {
    return this.executor.queueGet(name);
  }

  //TODO log statistics
}
