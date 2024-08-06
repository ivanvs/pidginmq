import { MockProxy, mock } from 'jest-mock-extended';
import { Executor } from './executor.js';
import { Notifier } from './notifier.js';
import { Elector } from './elector.js';
import { ValidationException } from './exceptions/validation.exception.js';

describe('elector', () => {
  let executor: MockProxy<Executor>;
  let notifier: MockProxy<Notifier>;

  beforeEach(() => {
    executor = mock<Executor>();
    notifier = mock<Notifier>();
  });

  describe('constructor', () => {
    it('should throw exception if executor is null', () => {
      expect(
        () => new Elector(null, 'id', 'name', 1000, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if notifier is null', () => {
      expect(
        () => new Elector(executor, 'id', 'name', 1000, 1000, null),
      ).toThrow(ValidationException);
    });

    it('should throw exception if id is null', () => {
      expect(
        () => new Elector(executor, null, 'name', 1000, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if id is empty string', () => {
      expect(
        () => new Elector(executor, '', 'name', 1000, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if name is null', () => {
      expect(
        () => new Elector(executor, 'id', null, 1000, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if name is empty string', () => {
      expect(
        () => new Elector(executor, 'id', '', 1000, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if interval is null', () => {
      expect(
        () => new Elector(executor, 'id', 'name', null, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if interval is empty string', () => {
      expect(
        () => new Elector(executor, 'id', 'name', -10, 1000, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if ttl is null', () => {
      expect(
        () => new Elector(executor, 'id', 'name', 1000, null, notifier),
      ).toThrow(ValidationException);
    });

    it('should throw exception if ttl is empty string', () => {
      expect(
        () => new Elector(executor, 'id', 'name', 1000, -10, notifier),
      ).toThrow(ValidationException);
    });
  });
});
