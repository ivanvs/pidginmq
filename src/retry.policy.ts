import { DateTime } from 'luxon';
import { Job } from './types/job.js';

const DEFAULT_DELAY = 1_000; // 1s

export type ClientRetryPolicy = (job: Job) => Date;

export interface BuiltInPolicies {
  [index: string]: (delay: number) => ClientRetryPolicy;
}

export class RetryPolicies {
  static builtinPolicies: BuiltInPolicies = {
    fixed: (delay: number) => () => {
      if (!delay || delay <= 0) {
        delay = DEFAULT_DELAY;
      }

      return DateTime.utc().plus({ milliseconds: delay }).toJSDate();
    },
    exponential: (delay: number) => {
      return function (job: Job): Date {
        if (!delay || delay <= 0) {
          delay = DEFAULT_DELAY;
        }

        return DateTime.utc()
          .plus({
            milliseconds: Math.round(Math.pow(2, job.attempt) * delay),
          })
          .toJSDate();
      };
    },
  };
}
