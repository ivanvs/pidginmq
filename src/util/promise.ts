import { TimedoutJobException } from '../exceptions/timedout.job.exception.js';
import { PromiseOrDirect } from './util.types.js';

export interface TimeoutPromise<T> {
  controller: AbortController;
  promise: Promise<T>;
}

export const createTimeoutPromise = <T>(
  work: () => PromiseOrDirect<T>,
  timeout: number,
): TimeoutPromise<T> => {
  const controller = new AbortController();

  const timeoutId = setTimeout(
    () => controller.abort(new TimedoutJobException()),
    timeout,
  );

  const returnPromise = new Promise<T>((resolve, reject) => {
    const abortHandler = function () {
      reject(this.reason);
      clearTimeout(timeoutId);
    };
    controller.signal?.addEventListener('abort', abortHandler);
    const promise = (async () => await work())();
    promise
      .then((result) => {
        resolve(result);
      })
      .catch((error) => {
        reject(error);
      })
      .finally(() => {
        clearTimeout(timeoutId);
        controller.signal?.removeEventListener('abort', abortHandler);
      });
  });

  return { promise: returnPromise, controller };
};

/**
 * Runs the function `fn`
 * and retries automatically if it fails.
 *
 * Tries max `1 + retries` times
 * with `retryIntervalMs` milliseconds between retries.
 *
 * From https://mtsknn.fi/blog/js-retry-on-fail/
 */
export const retry = async <T>(
  fn: () => Promise<T> | T,
  { retries, retryIntervalMs }: { retries: number; retryIntervalMs: number },
): Promise<T> => {
  try {
    return await fn();
  } catch (error) {
    if (retries <= 0) {
      throw error;
    }
    await sleep(retryIntervalMs);
    return retry(fn, { retries: retries - 1, retryIntervalMs });
  }
};

export const sleep = (waitTimeInMs = 0) =>
  new Promise((resolve) => setTimeout(resolve, waitTimeInMs));

export interface Deferred<T> {
  promise: Promise<T>;
  resolve: (result?: any) => void;
  reject: (error?: any) => void;
}

export const defer = <T>(): Deferred<T> => {
  const deferred: Deferred<T> = {
    promise: null,
    resolve: null,
    reject: null,
  };

  deferred.promise = new Promise((resolve, reject) => {
    deferred.resolve = resolve;
    deferred.reject = reject;
  });

  return deferred;
};
