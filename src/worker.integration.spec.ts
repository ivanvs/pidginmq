import {
  Client,
  ClientOptions,
  DEFAULT_CLIENT_OPTIONS,
  QueueConfig,
} from './client';
import { sleep } from './util/promise';
import { getTestContainer } from './util/test.container';
import { Workers } from './worker';

describe('worker integration tests', () => {
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

  afterAll(async () => {
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

    await sleep(10_000);
    await pidgin.stop();

    expect(processed).toBe(1);
  });
});
