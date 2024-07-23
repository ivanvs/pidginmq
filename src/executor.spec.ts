import { Executor } from './executor';
import { PostgresDbDriver } from './postgres/pg.db.driver';
import { getTestContainer } from './util/test.container';
import { NotificationTopic } from './notifier';

describe('executor', () => {
  jest.setTimeout(60000);

  let postgresContainer;
  let executor: Executor;
  let driver: PostgresDbDriver;
  const TEST_PARAMS = {
    args: { name: 'test' },
    kind: 'test',
    maxAttempts: 1,
    metadata: { name: 'test' },
    priority: 1,
    queue: 'test',
    scheduletAt: new Date(),
    tags: ['test'],
  };

  beforeAll(async () => {
    postgresContainer = await getTestContainer().start();

    driver = new PostgresDbDriver({
      host: postgresContainer.getHost(),
      port: postgresContainer.getPort(),
      database: postgresContainer.getDatabase(),
      user: postgresContainer.getUsername(),
      password: postgresContainer.getPassword(),
      ssl: false,
    });

    await expect(driver.open()).resolves.not.toThrow();
    executor = new Executor(driver);
  });

  afterAll(async () => {
    await driver.close();
    await postgresContainer.stop();
  });

  it('should insert a job', async () => {
    const job = await executor.insertJob(TEST_PARAMS);

    expect(job).not.toBeNull();

    expect(job.finalizedAt).toBeNull();
    expect(job.kind).toBe('test');
    expect(job.maxAttempts).toBe(1);
    expect(job.metadata.name).toBe('test');
    expect(job.queue).toBe('test');
    expect(job.state).toBe('available');
    expect(job.tags.length).toBe(1);
    expect(job.tags[0]).toBe('test');
    expect(job.id).not.toBeNull();
  });

  it('should able to query jobs by state', async () => {
    const jobs = await executor.queryJobs({ limitCount: 20, state: 'running' });

    expect(jobs).not.toBeNull();
    expect(jobs.length).toBe(0);
  });

  it('shoud delete a job', async () => {
    const job = await executor.insertJob(TEST_PARAMS);

    expect(job).not.toBeNull();

    const deletedJob = await executor.jobDelete(job.id);

    expect(deletedJob).toBeTruthy();
    expect(deletedJob.finalizedAt).not.toBeTruthy();
    expect(deletedJob.kind).toBe('test');
    expect(deletedJob.maxAttempts).toBe(1);
    expect(deletedJob.metadata.name).toBe('test');
    expect(deletedJob.queue).toBe('test');
    expect(deletedJob.state).toBe('available');
    expect(deletedJob.tags.length).toBe(1);
    expect(deletedJob.tags[0]).toBe('test');
    expect(deletedJob.id).toBeTruthy();

    const foundJob = await executor.getJobById(job.id);

    expect(foundJob).toBeNull();
  });

  it(`should not be able to delete a job that doesn't exist`, async () => {
    const deletedJob = await executor.jobDelete(115);

    expect(deletedJob).toBeNull();
  });

  it(`should return null if job deos not exists`, async () => {
    const job = await executor.getJobById(115);

    expect(job).toBeNull();
  });

  it(`should be able to get a job`, async () => {
    const job = await executor.insertJob(TEST_PARAMS);

    expect(job).not.toBeNull();

    const foundJob = await executor.getJobById(job.id);

    expect(foundJob).toBeTruthy();
    expect(foundJob.finalizedAt).not.toBeTruthy();
    expect(foundJob.kind).toBe('test');
    expect(foundJob.maxAttempts).toBe(1);
    expect(foundJob.metadata.name).toBe('test');
    expect(foundJob.queue).toBe('test');
    expect(foundJob.state).toBe('available');
    expect(foundJob.tags.length).toBe(1);
    expect(foundJob.tags[0]).toBe('test');
    expect(foundJob.id).toBeTruthy();
  });

  it(`should not be able to cancel not existing job`, async () => {
    const cancelledJob = await executor.cancelJob({
      id: 1132,
      jobControlTopic: NotificationTopic.NotificationTopicJobControl,
      cancelAttemtedAt: new Date(),
    });

    expect(cancelledJob).toBeNull();
  });

  it(`should not be able to cancel not existing job`, async () => {
    const job = await executor.insertJob(TEST_PARAMS);

    expect(job).not.toBeNull();

    const cancelledJob = await executor.cancelJob({
      id: job.id,
      jobControlTopic: NotificationTopic.NotificationTopicJobControl,
      cancelAttemtedAt: new Date(),
    });

    expect(cancelledJob).toBeTruthy();
    expect(cancelledJob.finalizedAt).toBeTruthy();
    expect(cancelledJob.kind).toBe('test');
    expect(cancelledJob.maxAttempts).toBe(1);
    expect(cancelledJob.metadata.name).toBe('test');
    expect(cancelledJob.queue).toBe('test');
    expect(cancelledJob.state).toBe('cancelled');
    expect(cancelledJob.tags.length).toBe(1);
    expect(cancelledJob.tags[0]).toBe('test');
    expect(cancelledJob.id).toBe(job.id);
  });
});
