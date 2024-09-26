import { DateTime } from 'luxon';
import {
  Executor,
  InsertJobParams,
  InsertRepetableJobParams,
  JobQueryParams,
} from './executor.js';
import { PostgresDbDriver } from './postgres/pg.db.driver.js';
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
import { LogLevels, setLogLevel, logger } from './logger/logger.settings.js';
import { LogLevelDesc } from 'loglevel';

export interface QueueConfig {
  maxWorkers: number;
}

export interface ClientOptions {
  /**
   * Database connection URI.
   *
   * The general form of a connection URI in PostgreSQL: postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
   *
   * Valid examples:
   *
   * postgresql://localhost
   * postgresql://localhost:5433
   * postgresql://localhost/mydb
   * postgresql://user@localhost
   * postgresql://user:secret@localhost
   * postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp
   * postgresql://host1:123,host2:456/somedb?application_name=myapp
   *
   * Detailed description of format can be found here: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS
   */
  dbUri: string;
  /**
   * How long cancelled jobs are retained before removal - default 24 hours
   */
  cancelledJobRetentionPeriod?: number;
  /**
   * How long completed jobs are retained before removal - default 24 hours
   */
  completedJobRetentionPeriod?: number;
  /**
   * How long discarded jobs are retained before removal - default 24 hours
   */
  discardedJobRetentionPeriod?: number;
  /**
   * Minimum amount of time between two fetchs of jobs for execution. Default value is 200 miliseconds
   */
  fetchCoolDown?: number;
  /**
   * Amount of time between fetching jobs for execution. Default value is 500 miliseconds
   */
  fetchPollInterval?: number;
  /**
   * Unique identifier of particular PidginMQ client. If not set value will be `default`.
   * It is used for leader election and identification who attemted to execute the job
   */
  id?: string;
  /**
   * The maximum duration allowed for a job to execute. Default value is 1 minute
   */
  jobTimeout?: number;
  /**
   * The maximum amount of time that job can be in running state before it is being candiadate for state stuck. Default value is 1 hour
   */
  rescueStuckJobsAfter?: number;
  /**
   * Retry policy for jobs. There is two default retry policies: fixed and exponential.
   *
   * Additional retry policies could be implemented with method that satisfy type `ClientRetryPolicy`
   */
  retryPolicy?: ClientRetryPolicy;
  /**
   * Time interval between two runs of scheduler. Scheduler will move jobs from scheduled and retryable state to available state. Default value is 1 second
   */
  schedulerInterval?: number;
  /**
   * Collection of executor that will execute jobs of given kind
   */
  workers: Workers;
  /**
   * Map of queue names and configuration for specific queue. Currently only configuration is number of executors of queue
   */
  queues: Map<string, QueueConfig>;
  /**
   * Configuration which log should be written to console. Default is SILENT - nothing will be logged into console.
   */
  logLevel?: LogLevelDesc;
}

const DEFAULT_INSERT_JOB_PARAMS = {
  args: {},
  maxAttempts: 3,
  metadata: {},
  priority: 1,
  scheduletAt: DateTime.utc().toJSDate(),
  tags: [],
};

const DEFAULT_CLIENT_OPTIONS = {
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
  logger: LogLevels.SILENT,
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

/**
 * PidginMQ Client.
 *
 * This class will be used as API for exposing PidginMQ functionality.
 *
 * Communication with database and job processing will start only after `start` method is called.
 *
 * On your application shut down you should execut `stop` method.
 */
export class Client {
  private options: ClientOptions;
  private db: DbDriver;
  private executor: Executor;
  private eventSubject: Subject<Event>;
  private workers: Workers;
  private retryPolicy: ClientRetryPolicy;
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
    this.options = { ...DEFAULT_CLIENT_OPTIONS, ...options };

    if (!this.options.id) {
      throw new ValidationException('ID is not supplied');
    }

    setLogLevel(this?.options?.logLevel || LogLevels.SILENT);

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
    this.options.queues.set(SCHEDULED_JOB_QUEUE, { maxWorkers: 1 });

    if (this.options.workers && !this.options.queues) {
      throw new ValidationException('Workers must be set if queues are set');
    } else {
      this.workers = this.options.workers;
    }

    this.db = new PostgresDbDriver(this.options.dbUri);
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
      30,
      10,
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
    logger.info('Starting client');
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
  }

  private async handleLeadershipChange(event: LeadershipEvent) {
    logger.debug(`New leadership status: ${event.isLeader}`);

    if (event.isLeader) {
      this.jobRescuer.start();
      this.jobCleaner.start();
      this.jobScheduler.start();
      this.producersByName.forEach((value) => {
        value.start();
      });
    } else {
      this.jobRescuer.stop();
      this.jobCleaner.stop();
      this.jobScheduler.stop();
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
        logger.error(`Unhandled job state: ${job.state}. Bug in PidginMQ`);
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
      logger.debug(`Stopping producer for queue: ${key}`);
      await value.stop();
    }
  }

  async stop() {
    logger.info('Stopping client');
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
    const config = { ...DEFAULT_INSERT_JOB_PARAMS, ...options };
    logger.info(`Adding job: ${config}`);
    return this.executor.insertJob(config);
  }

  deleteJob(id: number): Promise<Job> {
    logger.info(`Deleting job: ${id}`);
    return this.executor.jobDelete(id);
  }

  queryJobs(options: JobQueryParams): Promise<Job[]> {
    logger.info(`Quering job: ${JSON.stringify(options)}`);
    return this.executor.queryJobs(options);
  }

  retryJob(id: number): Promise<Job> {
    logger.info(`Retry job: ${id}`);
    return this.executor.retryJob(id);
  }

  getJob(id: number): Promise<Job> {
    logger.info(`Get job: ${id}`);
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
    logger.info(`Schedule job: ${options}`);
    let scheduledTime: Date;
    if (options.repeat && options.repeat.cron) {
      const expression = this.parseCron(options.repeat.cron);
      scheduledTime = expression.next().toDate();
    } else {
      throw new ValidationException('Cron field should be supplied');
    }

    if (options?.repeat?.limit <= 0) {
      throw new ValidationException(
        'Repeat field limit cannot be less then or equal to 0',
      );
    }

    const scheduledArgs: ScheduledJobArgs = {
      options,
      scheduledTimes: 1,
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
      createorJobId: scheduledJob.id,
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
    logger.info(`Cancel job: ${id}`);
    return this.executor.cancelJob({
      id,
      cancelAttemtedAt: DateTime.utc().toJSDate(),
      jobControlTopic: NotificationTopic.NotificationTopicJobControl,
    });
  }

  subscribe(options: SubscribeOptions): Subscription {
    return this.eventSubject.subscribe({
      next: options.onNext,
      error: options.onError,
      complete: options.onCompleted,
    });
  }

  addWorker(kind: string, handler: (job: Job) => void) {
    logger.info(`Add worker for kind: ${kind}`);
    this.workers.addWorker(kind, handler);
  }

  async addQueue(queueName: string, queueConfig: QueueConfig) {
    logger.info(`Add queue: ${queueName}, with configuration: ${queueConfig}`);
    if (queueName.length < 1 && queueName.length > 512) {
      throw new ValidationException(
        'Queue name must have length between 1 and 512 characters',
      );
    }

    if (!queueConfig) {
      throw new ValidationException('Queue config is not supplied');
    }

    const producer = this.createProducer(queueName, queueConfig.maxWorkers);
    await producer.start();
    this.producersByName.set(queueName, producer);
  }

  async resumeQueue(name: string): Promise<Queue> {
    logger.info(`Resume queue: ${name}`);
    const queue = await this.executor.queueResume(name);
    await this.notifyQueueStatus(ControlAction.Resume, name);
    return queue;
  }

  async pauseQueue(name: string): Promise<Queue> {
    logger.info(`Pause queue: ${name}`);
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
    logger.info(`Search for queues. Limit: ${limit}`);
    return this.executor.queryQueues(limit);
  }

  async getQueue(name: string): Promise<Queue> {
    logger.info(`Get queue: ${name}`);
    return this.executor.queueGet(name);
  }

  //TODO log statistics
}
