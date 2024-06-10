import { any, mock, MockProxy } from 'jest-mock-extended';
import { DEFAULT_JOB_EXECUTOR_OPTIONS, JobExecutor } from './job.executor';
import { PidginWorker } from './worker';
import { ValidationException } from './exceptions/validation.exception';
import { createFeakJob } from './util/test.helper';
import { JobCompleter } from './job.completer';
import { ClientRetryPolicy } from './retry.policy';
import { setTimeout } from 'timers/promises';
import { SnoozeJobException } from './exceptions/snooze.job.exception';
import { DateTime } from 'luxon';

describe('job executor', () => {
  let worker: MockProxy<PidginWorker>;
  let completer: MockProxy<JobCompleter>;
  let retryPolicy: ClientRetryPolicy;

  beforeEach(() => {
    worker = mock<PidginWorker>();
    completer = mock<JobCompleter>();
    retryPolicy = () => new Date();
  });

  describe('constructor', () => {
    it('should throw exception if job is not supplied', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      expect(
        () => new JobExecutor(null, worker, completer, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if worker is not supplied', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, null, completer, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if job completer is not supplied', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, worker, null, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if job timeout is 0', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      options.jobTimeout = 0;
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, worker, completer, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if job timeout is less then 0', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      options.jobTimeout = -1;
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, worker, completer, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if schedule interval is less then 0', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      options.scheduleInterval = -1;
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, worker, completer, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if schedule interval is 0', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      options.scheduleInterval = 0;
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, worker, completer, retryPolicy, options),
      ).toThrow(ValidationException);
    });

    it('should throw exception if retry policy is not supplied', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();
      expect(
        () => new JobExecutor(job, worker, completer, null, options),
      ).toThrow(ValidationException);
    });
  });

  describe('cancel', () => {
    it('should not cancel job if job is not started', () => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();
      const executor = new JobExecutor(
        job,
        worker,
        completer,
        retryPolicy,
        options,
      );

      expect(() => executor.cancel()).not.toThrow();

      expect(completer.jobSetStateIfRunning).not.toHaveBeenCalled();
    });

    it('should cancel job that is being executed', (done) => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();

      const executor = new JobExecutor(
        job,
        worker,
        completer,
        retryPolicy,
        options,
      );

      worker.work.calledWith(any()).mockImplementation(async () => {
        await setTimeout(5_000);
      });

      completer.jobSetStateIfRunning
        .calledWith(any(), any())
        .mockImplementation((stats, args) => {
          expect(stats).toBeTruthy();

          expect(args.id).toBe(job.id);
          expect(args.state).toBe('cancelled');
          expect(args.finalizedAt).toBeTruthy();
          expect(args.error).toBeTruthy();

          done();
          return Promise.resolve();
        });

      executor.execute();
      executor.cancel();
    });
  });

  it('should cancel job that reached max attempts', (done) => {
    const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
    options.jobTimeout = 100;
    const job = createFeakJob();
    job.attempt = 10;

    const executor = new JobExecutor(
      job,
      worker,
      completer,
      retryPolicy,
      options,
    );

    worker.work.calledWith(any()).mockImplementation(async () => {
      await setTimeout(5_000);
    });

    completer.jobSetStateIfRunning
      .calledWith(any(), any())
      .mockImplementation((stats, args) => {
        expect(stats).toBeTruthy();

        expect(args.id).toBe(job.id);
        expect(args.state).toBe('cancelled');
        expect(args.finalizedAt).toBeTruthy();
        expect(args.error).toBeTruthy();

        done();
        return Promise.resolve();
      });

    executor.execute();
  });

  it('should retry job that did not reached max attempts and retry durationis smaller then schedule interval', (done) => {
    const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
    options.jobTimeout = 100;
    options.scheduleInterval = 2000;
    const job = createFeakJob();

    const policy = () => DateTime.utc().plus({ seconds: 1 }).toJSDate();

    const executor = new JobExecutor(job, worker, completer, policy, options);

    worker.work.calledWith(any()).mockImplementation(async () => {
      await setTimeout(5_000);
    });

    completer.jobSetStateIfRunning
      .calledWith(any(), any())
      .mockImplementation((stats, args) => {
        expect(stats).toBeTruthy();

        expect(args.id).toBe(job.id);
        expect(args.state).toBe('available');
        expect(args.scheduledAt).toBeTruthy();
        expect(args.error).toBeTruthy();

        done();
        return Promise.resolve();
      });

    executor.execute();
  });

  it('should retry job that did not reached max attempts and retry durationis bigger then schedule interval', (done) => {
    const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
    options.jobTimeout = 100;
    options.scheduleInterval = 1000;
    const job = createFeakJob();

    const policy = () => DateTime.utc().plus({ seconds: 2 }).toJSDate();

    const executor = new JobExecutor(job, worker, completer, policy, options);

    worker.work.calledWith(any()).mockImplementation(async () => {
      await setTimeout(5_000);
    });

    completer.jobSetStateIfRunning
      .calledWith(any(), any())
      .mockImplementation((stats, args) => {
        expect(stats).toBeTruthy();

        expect(args.id).toBe(job.id);
        expect(args.state).toBe('retryable');
        expect(args.scheduledAt).toBeTruthy();
        expect(args.error).toBeTruthy();

        done();
        return Promise.resolve();
      });

    executor.execute();
  });

  it('should finish job that is done', (done) => {
    const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
    const job = createFeakJob();

    const executor = new JobExecutor(
      job,
      worker,
      completer,
      retryPolicy,
      options,
    );

    worker.work.calledWith(any()).mockImplementation(() => {
      return;
    });

    completer.jobSetStateIfRunning
      .calledWith(any(), any())
      .mockImplementation((stats, args) => {
        expect(stats).toBeTruthy();

        expect(args.id).toBe(job.id);
        expect(args.state).toBe('completed');
        expect(args.finalizedAt).toBeTruthy();
        expect(args.error).not.toBeTruthy();

        done();
        return Promise.resolve();
      });

    executor.execute();
  });

  describe('snooze', () => {
    it('should be able to snooze job with interval less then schedule interval', (done) => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();

      const executor = new JobExecutor(
        job,
        worker,
        completer,
        retryPolicy,
        options,
      );

      worker.work.calledWith(any()).mockImplementation(() => {
        throw new SnoozeJobException(1);
      });

      completer.jobSetStateIfRunning
        .calledWith(any(), any())
        .mockImplementation((stats, args) => {
          expect(stats).toBeTruthy();

          expect(args.id).toBe(job.id);
          expect(args.state).toBe('scheduled');
          expect(args.maxAttempts).toBe(job.attempt + 1);
          expect(args.error).not.toBeTruthy();

          done();
          return Promise.resolve();
        });

      executor.execute();
    });

    it('should be able to snooze job with interval bigger then schedule interval', (done) => {
      const options = { ...DEFAULT_JOB_EXECUTOR_OPTIONS };
      const job = createFeakJob();

      const executor = new JobExecutor(
        job,
        worker,
        completer,
        retryPolicy,
        options,
      );

      worker.work.calledWith(any()).mockImplementation(() => {
        throw new SnoozeJobException(100);
      });

      completer.jobSetStateIfRunning
        .calledWith(any(), any())
        .mockImplementation((stats, args) => {
          expect(stats).toBeTruthy();

          expect(args.id).toBe(job.id);
          expect(args.state).toBe('available');
          expect(args.maxAttempts).toBe(job.attempt + 1);
          expect(args.error).not.toBeTruthy();

          done();
          return Promise.resolve();
        });

      executor.execute();
    });
  });
});
