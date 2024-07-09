export class AttemptError {
  at: Date;
  attempt: number;
  error: string;
  trace: string;
}

export type JobState =
  | 'available'
  | 'cancelled'
  | 'completed'
  | 'discarded'
  | 'retryable'
  | 'running'
  | 'scheduled';

// Job contains the properties of a job that are persisted to the database.
export interface Job {
  id: number;
  attempt: number;
  attemptedAt: Date;
  attemptedBy: string[];
  createdAt: Date;
  encodedArgs: any;
  finalizedAt: Date;
  kind: string;
  maxAttempts: number;
  metadata: any;
  priority: number;
  queue: string;
  state: JobState;
  tags: string[];
  errors: AttemptError[];
  scheduledAt: Date;
}

export enum ControlAction {
  Cancel = 'cancel',
  Pause = 'pause',
  Resume = 'resume',
}

export interface JobControlPayload {
  action: ControlAction;
  jobId?: number;
  queue: string;
}

export interface InsertJobPayload {
  queue: string;
}

export interface JobStatistics {
  completeDuration: number;
  queueWaitDuration: number;
  runDuration: number;
}
