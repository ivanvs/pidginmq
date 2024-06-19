import { DateTime } from 'luxon';
import { ClientRetryPolicy, RetryPolicies } from './retry.policy.js';
import { Job } from './types/job.js';
import { createFeakJob } from './util/test.helper.js';

describe('retry policy', () => {
  const SECOND = 1000;
  let policy: ClientRetryPolicy;
  it('should return date with fixed delay', () => {
    policy = RetryPolicies.builtinPolicies.fixed(SECOND);

    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(null), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(1000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      1100,
    );
  });

  it('should return date with exponential delay', () => {
    policy = RetryPolicies.builtinPolicies.exponential(SECOND);

    const job: Job = createFeakJob();
    let now = DateTime.utc();
    let result = DateTime.fromJSDate(policy(job), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(1000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      1100,
    );

    job.attempt = 1;

    now = DateTime.utc();
    result = DateTime.fromJSDate(policy(job), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(2000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      2100,
    );

    job.attempt = 2;

    now = DateTime.utc();
    result = DateTime.fromJSDate(policy(job), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(4000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      4100,
    );
  });

  it('should default delay if delay is not supplied', () => {
    policy = RetryPolicies.builtinPolicies.fixed(null);

    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(null), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(1000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      1100,
    );
  });

  it('should return date with exponential default delay if delay is not supplied', () => {
    policy = RetryPolicies.builtinPolicies.exponential(null);

    const job: Job = createFeakJob();
    job.attempt = 3;
    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(job), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(8000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      8100,
    );
  });

  it('should default delay if delay is less then 0', () => {
    policy = RetryPolicies.builtinPolicies.fixed(-1);

    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(null), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(1000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      1100,
    );
  });

  it('should return date with exponential default delay if delay is less then 0', () => {
    policy = RetryPolicies.builtinPolicies.exponential(-1);

    const job: Job = createFeakJob();
    job.attempt = 3;
    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(job), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(8000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      8100,
    );
  });

  it('should default delay if delay is equal 0', () => {
    policy = RetryPolicies.builtinPolicies.fixed(-1);

    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(null), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(1000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      1100,
    );
  });

  it('should return date with exponential default delay if delay is equal 0', () => {
    policy = RetryPolicies.builtinPolicies.exponential(-1);

    const job: Job = createFeakJob();
    job.attempt = 3;
    const now = DateTime.utc();
    const result = DateTime.fromJSDate(policy(job), { zone: 'utc' });

    expect(
      result.diff(now, 'milliseconds').as('milliseconds'),
    ).toBeGreaterThanOrEqual(8000);
    expect(result.diff(now, 'milliseconds').as('milliseconds')).toBeLessThan(
      8100,
    );
  });
});
