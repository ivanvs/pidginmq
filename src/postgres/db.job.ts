import { AttemptError, JobState } from '../types/job.js';

export interface DbJob {
  id: number;
  attempt: number;
  attempted_at: Date;
  attempted_by: string[];
  created_at: Date;
  encoded_args: any;
  finalized_at: Date;
  kind: string;
  max_attempts: number;
  metadata: any;
  priority: number;
  queue: string;
  state: JobState;
  tags: string[];
  errors: AttemptError[];
  scheduled_at: Date;
}
