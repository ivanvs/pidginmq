import { Job, JobStatistics } from './job.js';

export enum EventKind {
  JobCancelled,
  JobCompleted,
  JobSnoozed,
  JobFailed,
}

export class Event {
  kind: EventKind;
  job: Job;
  stats: JobStatistics;
}
