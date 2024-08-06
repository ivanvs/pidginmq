import { Client, QueueConfig } from './client';
import { ValidationException } from './exceptions/validation.exception';
import { LogLevels } from './logger/logger.settings';
import { RetryPolicies } from './retry.policy';
import { Workers } from './worker';

const DEFAULT_CLIENT_OPTIONS = {
  cancelledJobRetentionPeriod: 24 * 60 * 60 * 1_000, // 24h
  completedJobRetentionPeriod: 24 * 60 * 60 * 1_000, // 24h
  discardedJobRetentionPeriod: 24 * 60 * 60 * 1_000, // 24h
  fetchCoolDown: 200,
  fetchPollInterval: 500,
  id: 'default',
  jobTimeout: 60 * 1_000,
  rescueStuckJobsAfter: 60 * 60 * 1_000,
  retryPolicy: RetryPolicies.builtinPolicies.fixed(5_000),
  schedulerInterval: 1_000,
  logger: LogLevels.SILENT,
  workers: new Workers(),
  queues: new Map<string, QueueConfig>(),
  dbConfig: {
    host: 'localhost',
    port: 5432,
    user: 'testuser',
    password: 'testpassword',
    database: 'testdatabase',
    ssl: true,
  },
};

describe('client', () => {
  describe('constructor', () => {
    it('should throw exception if id is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ id: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if id is empty string', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ id: '' },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if job timeout is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ jobTimeout: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if job timeout is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ jobTimeout: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if rescueStuckJobsAfter is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ rescueStuckJobsAfter: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if rescueStuckJobsAfter is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ rescueStuckJobsAfter: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if schedulerInterval is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ schedulerInterval: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if schedulerInterval is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ schedulerInterval: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if fetchPollInterval is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ fetchPollInterval: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if fetchPollInterval is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ fetchPollInterval: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if fetchCoolDown is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ fetchCoolDown: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if fetchCoolDown is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ fetchCoolDown: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if cancelledJobRetentionPeriod is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ cancelledJobRetentionPeriod: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if cancelledJobRetentionPeriod is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ cancelledJobRetentionPeriod: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if completedJobRetentionPeriod is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ completedJobRetentionPeriod: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if completedJobRetentionPeriod is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ completedJobRetentionPeriod: -100 },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if discardedJobRetentionPeriod is null', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ discardedJobRetentionPeriod: null },
          }),
      ).toThrow(ValidationException);
    });

    it('should throw exception if discardedJobRetentionPeriod is less then 0', () => {
      expect(
        () =>
          new Client({
            ...DEFAULT_CLIENT_OPTIONS,
            ...{ discardedJobRetentionPeriod: -100 },
          }),
      ).toThrow(ValidationException);
    });
  });
});
