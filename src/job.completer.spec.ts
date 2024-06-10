import { MockProxy, any, mock } from 'jest-mock-extended';
import { ValidationException } from './exceptions/validation.exception.js';
import { CompletedJobEvent, JobCompleter } from './job.completer.js';
import { Executor } from './executor.js';
import {
  createFeakJob,
  createTestJobSetStateIfRunningParams,
} from './util/test.helper.js';
import { JobStatistics } from './types/job.js';
import { PidginException } from './exceptions/pidgin.exception.js';

describe('job completer', () => {
  let executor: MockProxy<Executor>;

  beforeEach(() => {
    executor = mock<Executor>();
  });

  it('should not be able to create job completer if executor is not supplied', () => {
    expect(() => new JobCompleter(null)).toThrow(ValidationException);
  });

  it('should be able to complete job and publish statistics', (done) => {
    const completer = new JobCompleter(executor);
    const job = createFeakJob();

    const jobStatistics: JobStatistics = {
      completeDuration: 0,
      queueWaitDuration: 0,
      runDuration: 0,
    };
    const params = createTestJobSetStateIfRunningParams();

    executor.jobSetStateIfRunning
      .calledWith(any())
      .mockImplementation((args) => {
        expect(args).toBeTruthy();
        expect(args).toStrictEqual(params);

        return Promise.resolve(job);
      });

    completer.subscribe((event: CompletedJobEvent) => {
      expect(event).toBeTruthy();
      expect(event.job).toStrictEqual(job);
      expect(event.jobStatistics).toBeTruthy();
      expect(event.jobStatistics.completeDuration).toBeGreaterThan(0);
      expect(event.jobStatistics.queueWaitDuration).toBe(0);
      expect(event.jobStatistics.runDuration).toBe(0);

      expect(executor.jobSetStateIfRunning).toHaveBeenCalledTimes(1);
      done();
    });
    completer.jobSetStateIfRunning(jobStatistics, params);
  });

  it('should retry on exception', async () => {
    const completer = new JobCompleter(executor);

    const jobStatistics: JobStatistics = {
      completeDuration: 0,
      queueWaitDuration: 0,
      runDuration: 0,
    };
    const params = createTestJobSetStateIfRunningParams();

    executor.jobSetStateIfRunning.calledWith(any()).mockImplementation(() => {
      throw new PidginException('Failed');
    });

    await expect(() =>
      completer.jobSetStateIfRunning(jobStatistics, params),
    ).rejects.toThrow(PidginException);
    expect(executor.jobSetStateIfRunning).toHaveBeenCalledTimes(4);
  });
});
