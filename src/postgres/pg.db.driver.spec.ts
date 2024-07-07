import { sleep } from '../util/promise.js';
import { getTestContainer } from '../util/test.container.js';
import { PostgresDbDriver } from './pg.db.driver.js';
import { Notification } from 'pg';
import { defer } from '../util/promise.js';
import { NoLogger } from '../logger/logger.js';

describe('database', () => {
  jest.setTimeout(30000);

  let postgresContainer;

  beforeAll(async () => {
    postgresContainer = await getTestContainer().start();
  });

  afterAll(async () => {
    await postgresContainer.stop();
  });

  const createDatabase = () => {
    return new PostgresDbDriver(
      {
        host: postgresContainer.getHost(),
        port: postgresContainer.getPort(),
        database: postgresContainer.getDatabase(),
        user: postgresContainer.getUsername(),
        password: postgresContainer.getPassword(),
        ssl: false,
      },
      NoLogger,
    );
  };

  it('should fail on invalid database host', async () => {
    const driver = new PostgresDbDriver(
      {
        host: 'local',
        port: postgresContainer.getPort(),
        database: postgresContainer.getDatabase(),
        user: postgresContainer.getUsername(),
        password: postgresContainer.getPassword(),
        ssl: false,
      },
      NoLogger,
    );

    await expect(async () => await driver.open()).rejects.toThrow();
  });

  it('should connect to database host', async () => {
    const driver = createDatabase();

    await expect(driver.open()).resolves.not.toThrow();
    await expect(driver.close()).resolves.not.toThrow();
  });

  it('should recive notification from database', async () => {
    const deferred = defer();
    const driver = createDatabase();
    await expect(driver.open()).resolves.not.toThrow();

    await driver.listen('foo');
    driver.onNotification(async (notification: Notification) => {
      expect(notification.channel).toBe('foo');
      expect(notification.payload).toBe('test');

      await expect(driver.close()).resolves.not.toThrow();
      deferred.resolve();
    });

    await driver.execute(`NOTIFY foo, 'test'`);

    return deferred.promise;
  });

  it('should not recive notification from database after unlist', async () => {
    const driver = createDatabase();

    const mockCallback = jest.fn((message) => console.info(message));

    await expect(driver.open()).resolves.not.toThrow();

    driver.onNotification(mockCallback);

    await driver.listen('foo');

    await driver.unlisten('foo');
    await driver.execute(`NOTIFY foo, 'test'`);

    await sleep(2000);
    expect(mockCallback).not.toHaveBeenCalled();
    await expect(driver.close()).resolves.not.toThrow();
  });
});
