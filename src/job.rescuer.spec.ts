import { MockProxy, any, mock } from 'jest-mock-extended';
import { ValidationException } from './exceptions/validation.exception';
import { DEFAULT_JOB_RESCUER_OPTIONS, JobRescuer } from './job.rescuer';
import { Executor } from './executor';
import { createFeakJob } from './util/test.helper';

describe('job rescuer', () => {
  let executor: MockProxy<Executor>;

  beforeEach(() => {
    executor = mock<Executor>();
  });

  describe('constructor', () => {
    it('shoul throw excpetion if executor is not supplied', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      expect(() => new JobRescuer(null, options)).toThrow(ValidationException);
    });

    it('shoul not throw exception if options are not supplied', () => {
      expect(() => new JobRescuer(executor, null)).not.toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if interval is equal to 0', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.interval = 0;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if interval is less then 0', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.interval = -1;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if batch size is equal to 0', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.batchSize = 0;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if batch size is less then 0', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.batchSize = -1;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if rescue after is equal to 0', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.rescueAfter = 0;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if rescue after is less then 0', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.rescueAfter = -1;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });

    it('shoul throw exception if retry policy is not supplied', () => {
      const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
      options.retryPolicy = null;
      expect(() => new JobRescuer(executor, options)).toThrow(
        ValidationException,
      );
    });
  });

  it('should not throw exception on start and stop', () => {
    const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
    const rescuer = new JobRescuer(executor, options);
    expect(() => rescuer.start()).not.toThrow();
    expect(() => rescuer.stop()).not.toThrow();
  });

  it('should not do anything if there is not stucked jobs', async () => {
    const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
    options.interval = 10;
    const rescuer = new JobRescuer(executor, options);

    executor.getStuckJob.calledWith(any()).mockReturnValue(Promise.resolve([]));

    const result = await rescuer.runOnce();
    expect(result).toBeTruthy();
    expect(result.numJobsCancelled).toBe(0);
    expect(result.numJobsDiscarded).toBe(0);
    expect(result.numJobsRetried).toBe(0);
  });

  it('should canceled jobs that are attempted to be canceled', async () => {
    const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
    options.interval = 10;
    const rescuer = new JobRescuer(executor, options);

    const job = createFeakJob();
    job.metadata = { cancelAttemptedAt: new Date() };
    executor.getStuckJob
      .calledWith(any())
      .mockReturnValue(Promise.resolve([job]));

    executor.rescueManyJobs.calledWith(any()).mockImplementation((params) => {
      expect(params.ids.length).toBe(1);
      expect(params.ids[0]).toBe(1);

      expect(params.state.length).toBe(1);
      expect(params.state[0]).toBe('cancelled');

      expect(params.scheduledAt[0]).toBeTruthy();
      expect(params.finalizedAt[0]).toBeTruthy();

      return Promise.resolve([]);
    });

    const result = await rescuer.runOnce();

    expect(result).toBeTruthy();
    expect(result.numJobsCancelled).toBe(1);
    expect(result.numJobsDiscarded).toBe(0);
    expect(result.numJobsRetried).toBe(0);
  });

  it('should retry job if max attempts is not reached yet', async () => {
    const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
    options.interval = 10;
    const rescuer = new JobRescuer(executor, options);

    const job = createFeakJob();
    executor.getStuckJob
      .calledWith(any())
      .mockReturnValue(Promise.resolve([job]));

    executor.rescueManyJobs.calledWith(any()).mockImplementation((params) => {
      expect(params.ids.length).toBe(1);
      expect(params.ids[0]).toBe(1);

      expect(params.state.length).toBe(1);
      expect(params.state[0]).toBe('retryable');

      expect(params.scheduledAt[0]).toBeTruthy();
      expect(params.finalizedAt[0]).not.toBeTruthy();

      return Promise.resolve([]);
    });

    const result = await rescuer.runOnce();

    expect(result).toBeTruthy();
    expect(result.numJobsCancelled).toBe(0);
    expect(result.numJobsDiscarded).toBe(0);
    expect(result.numJobsRetried).toBe(1);
  });

  it('should discard job if max attempts is reached', async () => {
    const options = { ...DEFAULT_JOB_RESCUER_OPTIONS };
    options.interval = 10;
    const rescuer = new JobRescuer(executor, options);

    const job = createFeakJob();
    job.attempt = 10;
    executor.getStuckJob
      .calledWith(any())
      .mockReturnValue(Promise.resolve([job]));

    executor.rescueManyJobs.calledWith(any()).mockImplementation((params) => {
      expect(params.ids.length).toBe(1);
      expect(params.ids[0]).toBe(1);

      expect(params.state.length).toBe(1);
      expect(params.state[0]).toBe('discarded');

      expect(params.scheduledAt[0]).toBeTruthy();
      expect(params.finalizedAt[0]).toBeTruthy();

      return Promise.resolve([]);
    });

    const result = await rescuer.runOnce();

    expect(result).toBeTruthy();
    expect(result.numJobsCancelled).toBe(0);
    expect(result.numJobsDiscarded).toBe(1);
    expect(result.numJobsRetried).toBe(0);
  });
});
