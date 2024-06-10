import { MockProxy, any, mock } from 'jest-mock-extended';
import { Executor } from './executor.js';
import { DEFAULT_JOB_CLEANER_OPTIONS, JobCleaner } from './job.cleaner.js';
import { ValidationException } from './exceptions/validation.exception.js';
import { DateTime } from 'luxon';

describe('job cleaner', () => {
  let executor: MockProxy<Executor>;

  beforeEach(() => {
    executor = mock<Executor>();
  });

  describe('constructor', () => {
    it('should throw exception if executor is null', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      expect(() => new JobCleaner(null, options)).toThrow(ValidationException);
    });

    it('should throw exception if interval is less then 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.interval = -1;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if interval is equal to 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.interval = 0;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if batch size is less then 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.batchSize = -1;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if batch size is equal to 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.batchSize = 0;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if discardedJobRetentionPeriod is less then 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.discardedJobRetentionPeriod = -1;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if discardedJobRetentionPeriod is equal to 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.discardedJobRetentionPeriod = 0;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if completedJobRetentionPeriod is less then 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.completedJobRetentionPeriod = -1;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if completedJobRetentionPeriod is equal to 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.completedJobRetentionPeriod = 0;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if cancelledJobRetentionPeriod is less then 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.cancelledJobRetentionPeriod = -1;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('should throw exception if cancelledJobRetentionPeriod is equal to 0', () => {
      const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
      options.cancelledJobRetentionPeriod = 0;
      expect(() => new JobCleaner(executor, options)).toThrow(
        ValidationException,
      );
    });
  });

  it('should be able to start and stop without any exception', () => {
    const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
    const jobCleaner = new JobCleaner(executor, options);
    expect(() => jobCleaner.start()).not.toThrow();
    expect(() => jobCleaner.stop()).not.toThrow();
  });

  it('should be able to clean jobs', (done) => {
    const options = { ...DEFAULT_JOB_CLEANER_OPTIONS };
    options.interval = 10;
    options.cancelledJobRetentionPeriod = 100;
    options.completedJobRetentionPeriod = 100;
    options.discardedJobRetentionPeriod = 100;

    const jobCleaner = new JobCleaner(executor, options);
    jobCleaner.start();

    executor.jobDeleteBefore.calledWith(any()).mockImplementation((params) => {
      const cancelledJobRetentionPeriodDiff = DateTime.utc()
        .diff(
          DateTime.fromJSDate(params.cancelledFinalizedAtHorizon, {
            zone: 'utc',
          }),
          'millisecond',
        )
        .as('milliseconds');

      const completedFinalizedAtHorizonDiff = DateTime.utc()
        .diff(
          DateTime.fromJSDate(params.completedFinalizedAtHorizon, {
            zone: 'utc',
          }),
          'millisecond',
        )
        .as('milliseconds');

      const discardedFinalizedAtHorizonDiff = DateTime.utc()
        .diff(
          DateTime.fromJSDate(params.discardedFinalizedAtHorizon, {
            zone: 'utc',
          }),
          'millisecond',
        )
        .as('milliseconds');

      expect(cancelledJobRetentionPeriodDiff).toBeGreaterThanOrEqual(100);
      expect(completedFinalizedAtHorizonDiff).toBeGreaterThanOrEqual(100);
      expect(discardedFinalizedAtHorizonDiff).toBeGreaterThanOrEqual(100);
      expect(cancelledJobRetentionPeriodDiff).toBeLessThan(110);
      expect(completedFinalizedAtHorizonDiff).toBeLessThan(110);
      expect(discardedFinalizedAtHorizonDiff).toBeLessThan(110);

      expect(params.max).toBe(1_000);

      jobCleaner.stop();
      done();
      return Promise.resolve(30);
    });
  });
});
