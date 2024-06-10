import { MockProxy, any, mock } from 'jest-mock-extended';
import { ValidationException } from './exceptions/validation.exception.js';
import { Executor } from './executor.js';
import { DEFAULT_SCHEDULER_OPTIONS, Scheduler } from './scheduler.js';
import { NotificationTopic } from './notifier.js';
import { DateTime } from 'luxon';

describe('scheduler', () => {
  let executor: MockProxy<Executor>;

  beforeEach(() => {
    executor = mock<Executor>();
  });

  it('should not be able to construct one if executor is null', () => {
    expect(() => new Scheduler(DEFAULT_SCHEDULER_OPTIONS, null)).toThrow(
      ValidationException,
    );
  });

  it('should not be able to construct one if limit is less then 0', () => {
    const options = { ...DEFAULT_SCHEDULER_OPTIONS };
    options.limit = -1;
    expect(() => new Scheduler(options, executor)).toThrow(ValidationException);
  });

  it('should not be able to construct one if limit is equal to 0', () => {
    const options = { ...DEFAULT_SCHEDULER_OPTIONS };
    options.limit = 0;
    expect(() => new Scheduler(options, executor)).toThrow(ValidationException);
  });

  it('should not be able to construct one if interval is less then 0', () => {
    const options = { ...DEFAULT_SCHEDULER_OPTIONS };
    options.interval = -1;
    expect(() => new Scheduler(options, executor)).toThrow(ValidationException);
  });

  it('should not be able to construct one if interval is equal to 0', () => {
    const options = { ...DEFAULT_SCHEDULER_OPTIONS };
    options.interval = 0;
    expect(() => new Scheduler(options, executor)).toThrow(ValidationException);
  });

  it('should be able to start and stop without any exception', () => {
    const options = { ...DEFAULT_SCHEDULER_OPTIONS };
    const scheduler = new Scheduler(options, executor);
    expect(() => scheduler.start()).not.toThrow();
    expect(() => scheduler.stop()).not.toThrow();
  });

  it('should be able to schedule jobs', (done) => {
    const options = { ...DEFAULT_SCHEDULER_OPTIONS };
    options.interval = 10;
    const scheduler = new Scheduler(options, executor);
    scheduler.start();

    executor.scheduleJob.calledWith(any()).mockImplementation((params) => {
      expect(params.insertTopic).toBe(
        NotificationTopic.NotificationTopicInsert,
      );
      expect(params.max).toBe(1000);
      const executionDiff = DateTime.utc()
        .diff(DateTime.fromJSDate(params.now, { zone: 'utc' }), 'milliseconds')
        .as('milliseconds');
      expect(executionDiff).toBeLessThanOrEqual(10);

      scheduler.stop();
      done();
      return Promise.resolve(30);
    });
  });
});
