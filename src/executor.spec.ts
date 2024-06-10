import { Executor } from './executor.js';
import { PostgresDbDriver } from './postgres/pg.db.driver.js';
import { getTestContainer } from './util/test.container.js';

describe('executor', () => {
  jest.setTimeout(60000);

  let postgresContainer;
  let executor: Executor;
  let driver: PostgresDbDriver;

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
    const params = {
      args: { name: 'test' },
      kind: 'test',
      maxAttempts: 1,
      metadata: { name: 'test' },
      priority: 1,
      queue: 'test',
      scheduletAt: new Date(),
      tags: ['test'],
    };

    const job = await executor.insertJob(params);

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
});
