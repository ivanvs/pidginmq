import { DateTime } from 'luxon';
import { Job } from './types/job';
import { WorkerFunction } from './worker';
import { CronExpression, parseExpression } from 'cron-parser';
import {
  SCHEDULED_JOB_KIND,
  SCHEDULED_JOB_QUEUE,
  ScheduledJobArgs,
} from './client';
import { Executor, InsertJobParams } from './executor';

const nextRunTimeGenerator = (cron: CronExpression): DateTime | undefined => {
  if (cron.hasNext()) {
    return DateTime.fromJSDate(cron.next().toDate());
  }

  return undefined;
};

const missedRuns = (cron: CronExpression): DateTime[] => {
  const results: DateTime[] = [];

  while (cron.hasNext()) {
    const nextRun = cron.next();
    results.push(DateTime.fromJSDate(nextRun.toDate()));
  }
  return results;
};

export type ScheduledJobFactory = (executor: Executor) => WorkerFunction;

export const SCHEDULED_JOB_WORKER: ScheduledJobFactory =
  (executor: Executor) =>
  async (job: Job): Promise<void> => {
    const scheduledJobArgs: ScheduledJobArgs = job.metadata;
    const { cron, limit } = scheduledJobArgs.options.repeat;
    if (cron && scheduledJobArgs.scheduledTimes < limit) {
      const missedRunTimes = missedRuns(
        parseExpression(cron, {
          utc: true,
          startDate: DateTime.fromISO(scheduledJobArgs.lastRun).toJSDate(),
          currentDate: DateTime.utc().toJSDate(),
          endDate: DateTime.utc().toJSDate(),
        }),
      );
      const nextRunTime = nextRunTimeGenerator(
        parseExpression(cron, {
          utc: true,
          currentDate: DateTime.utc().toJSDate(),
        }),
      );

      let scheduledJob;
      if (nextRunTime) {
        const newScheduledJobArgs: ScheduledJobArgs = { ...scheduledJobArgs };
        newScheduledJobArgs.lastRun = DateTime.utc().toISOTime();
        newScheduledJobArgs.scheduledTimes++;

        const scheduledParams: InsertJobParams = {
          metadata: {
            ...newScheduledJobArgs,
            createorJobId: job.id,
          },
          queue: SCHEDULED_JOB_QUEUE,
          maxAttempts: 3,
          priority: 1,
          scheduletAt: nextRunTime.toJSDate(),
          kind: SCHEDULED_JOB_KIND,
          args: {},
          tags: [],
        };

        scheduledJob = await executor.insertJob(scheduledParams);

        const nextJobParams: InsertJobParams = {
          metadata: {
            ...newScheduledJobArgs.options.metadata,
            createorJobId: job.id,
          },
          queue: newScheduledJobArgs.options.queue,
          maxAttempts: newScheduledJobArgs.options.maxAttempts || 3,
          priority: newScheduledJobArgs.options.priority,
          scheduletAt: nextRunTime.toJSDate(),
          kind: newScheduledJobArgs.options.kind,
          args: newScheduledJobArgs.options.args,
          tags: newScheduledJobArgs.options.tags,
        };
        await executor.insertJob(nextJobParams);
      }

      missedRunTimes.forEach(async (scheduleAt) => {
        const { options } = scheduledJobArgs;
        const jobMetadata = {
          createorJobId: scheduledJob.id,
          ...options.metadata,
        };
        const nextJobParams: InsertJobParams = {
          metadata: jobMetadata,
          queue: options.queue,
          maxAttempts: options.maxAttempts || 3,
          priority: options.priority,
          scheduletAt: scheduleAt.toJSDate(),
          kind: options.kind,
          args: options.args,
          tags: options.tags,
        };
        await executor.insertJob(nextJobParams);
      });
    }
  };
