import { MockProxy, mock } from 'jest-mock-extended';
import { Workers } from './worker';
import { Notifier } from './notifier';
import { Executor } from './executor';
import { JobCompleter } from './job.completer';
import { ClientRetryPolicy } from './retry.policy';
import { DEFAULT_PRODUCER_OPTIONS, Producer } from './producer';
import { ValidationException } from './exceptions/validation.exception';

describe('producer', () => {
  let workers: MockProxy<Workers>;
  let completer: MockProxy<JobCompleter>;
  let retryPolicy: ClientRetryPolicy;
  let notifier: MockProxy<Notifier>;
  let executor: MockProxy<Executor>;

  beforeEach(() => {
    workers = mock<Workers>();
    completer = mock<JobCompleter>();
    retryPolicy = () => new Date();
    notifier = mock<Notifier>();
    executor = mock<Executor>();
  });

  describe('constructor', () => {
    it('should throw execption if notifier is not supplied', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      expect(
        () =>
          new Producer(
            null,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if retry policy is not supplied', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      expect(
        () =>
          new Producer(notifier, null, workers, executor, completer, options),
      ).toThrow(ValidationException);
    });

    it('should throw execption if workers are not supplied', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            null,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if executor is not supplied', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            null,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if completer is not supplied', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      expect(
        () =>
          new Producer(notifier, retryPolicy, workers, executor, null, options),
      ).toThrow(ValidationException);
    });

    it('should throw execption if job timeout is 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.jobTimeout = 0;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if job timeout is less then 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.jobTimeout = -1;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if fetch cool down is 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.fetchCoolDown = 0;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if fetch cool down is less then 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.fetchCoolDown = -1;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if fetch poll interval is 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.fetchPollInterval = 0;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if fetch poll interval is less then 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.fetchPollInterval = -1;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if max worker count is 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.maxWorkerCount = 0;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if max worker count is less then 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.maxWorkerCount = -1;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if schedule interval is 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.scheduleInterval = 0;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if schedule interval is less then 0', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.scheduleInterval = -1;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if queue name is empty', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.queue = '';
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if client id is empty', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.clientId = '';
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if queue name null', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.queue = null;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });

    it('should throw execption if client id is null', () => {
      const options = { ...DEFAULT_PRODUCER_OPTIONS };
      options.queue = null;
      expect(
        () =>
          new Producer(
            notifier,
            retryPolicy,
            workers,
            executor,
            completer,
            options,
          ),
      ).toThrow(ValidationException);
    });
  });

  it('should be able to run and stop producer', async () => {
    const options = { ...DEFAULT_PRODUCER_OPTIONS };
    const producer = new Producer(
      notifier,
      retryPolicy,
      workers,
      executor,
      completer,
      options,
    );

    await expect(() => producer.start()).not.toThrow();
    await expect(producer.stop()).resolves.not.toThrow();
  });
});
