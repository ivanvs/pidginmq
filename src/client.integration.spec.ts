import {
  Client,
  ClientOptions,
  DEFAULT_CLIENT_OPTIONS,
  QueueConfig,
} from './client';
import { SnoozeJobException } from './exceptions/snooze.job.exception';
import { sleep } from './util/promise';
import { getTestContainer } from './util/test.container';
import { Workers } from './worker';

describe('client integration tests', () => {
  jest.setTimeout(60000);
  let pidgin: Client;
  let postgresContainer;
  let options: ClientOptions;

  beforeEach(async () => {
    postgresContainer = await getTestContainer().start();

    options = {
      ...DEFAULT_CLIENT_OPTIONS,
      ...{
        workers: new Workers(),
        queues: new Map<string, QueueConfig>(),
        dbConfig: {
          host: postgresContainer.getHost(),
          port: postgresContainer.getPort(),
          user: postgresContainer.getUsername(),
          password: postgresContainer.getPassword(),
          database: postgresContainer.getDatabase(),
          ssl: false,
        },
      },
    };
    options.queues.set('test-queue', { maxWorkers: 1 });
  });

  afterEach(async () => {
    await pidgin.stop();
    await postgresContainer.stop();
  });

  it('should be able to add worker', async () => {
    let processed = 0;
    options.workers.addWorker('test', () => {
      processed++;
    });

    pidgin = new Client(options);
    await pidgin.start();

    const scheduledTime = new Date();
    scheduledTime.setSeconds(scheduledTime.getSeconds() + 2);

    await pidgin.addJob({
      kind: 'test',
      queue: 'test-queue',
      args: {},
      maxAttempts: 3,
      metadata: {},
      priority: 1,
      scheduletAt: scheduledTime,
      tags: ['test'],
    });

    await sleep(5_000);

    expect(processed).toBe(1);
  });

  it('should fail job if it takes more then timeout', async () => {
    options.workers.addWorker('test', async () => {
      await sleep(2_000);
    });
    options.jobTimeout = 1_000;

    pidgin = new Client(options);
    await pidgin.start();

    const scheduledTime = new Date();

    const createdJob = await pidgin.addJob({
      kind: 'test',
      queue: 'test-queue',
      args: {},
      maxAttempts: 3,
      metadata: {},
      priority: 1,
      scheduletAt: scheduledTime,
      tags: ['test'],
    });

    await sleep(10_000);

    const fetchedJob = await pidgin.getJob(createdJob.id);

    expect(fetchedJob).not.toBeNull();
    expect(fetchedJob.state).toBe('retryable');
    expect(fetchedJob.attempt).toBe(1);
    expect(fetchedJob.attemptedAt).not.toBeNull();
  });

  it('should be able to snooze job during execution', async () => {
    options.workers.addWorker('test', async (job) => {
      if (job.attempt === 1) {
        throw new SnoozeJobException(2, 'testing snoozed jobs');
      }
    });

    pidgin = new Client(options);
    await pidgin.start();

    const scheduledTime = new Date();

    const createdJob = await pidgin.addJob({
      kind: 'test',
      queue: 'test-queue',
      args: {},
      maxAttempts: 3,
      metadata: {},
      priority: 1,
      scheduletAt: scheduledTime,
      tags: ['test'],
    });

    await sleep(1_000);

    const fetchedJob = await pidgin.getJob(createdJob.id);

    expect(fetchedJob).not.toBeNull();
    expect(fetchedJob.state).toBe('available');
    expect(fetchedJob.attempt).toBe(1);
    expect(fetchedJob.attemptedAt).not.toBeNull();

    await sleep(5_000);

    const executedJob = await pidgin.getJob(createdJob.id);

    expect(executedJob).not.toBeNull();
    expect(executedJob.state).toBe('completed');
    expect(executedJob.finalizedAt).not.toBeNull();
    expect(executedJob.attempt).toBe(2);
    expect(executedJob.attemptedAt).not.toBeNull();
  });

  it('should be able to cancel job', async () => {
    pidgin = new Client(options);
    await pidgin.start();

    const scheduledTime = new Date();
    scheduledTime.setSeconds(scheduledTime.getSeconds() + 5);

    const createdJob = await pidgin.addJob({
      kind: 'test',
      queue: 'test-queue',
      args: {},
      maxAttempts: 3,
      metadata: {},
      priority: 1,
      scheduletAt: scheduledTime,
      tags: ['test'],
    });

    const fetchedJob = await pidgin.getJob(createdJob.id);

    expect(fetchedJob).not.toBeNull();
    expect(fetchedJob.state).toBe('available');
    expect(fetchedJob.attempt).toBe(0);
    expect(fetchedJob.attemptedAt).toBeNull();

    await pidgin.cancelJob(createdJob.id);

    const cancelledJob = await pidgin.getJob(createdJob.id);

    expect(cancelledJob).not.toBeNull();
    expect(cancelledJob.state).toBe('cancelled');
    expect(cancelledJob.finalizedAt).not.toBeNull();
    expect(cancelledJob.attempt).toBe(0);
    expect(cancelledJob.attemptedAt).toBeNull();
  });
});
