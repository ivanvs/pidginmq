import { JobState } from 'src/types/job.js';

export const createFeakJob = () => ({
  id: 1,
  attempt: 0,
  attemptedAt: null,
  attemptedBy: [],
  createdAt: new Date(),
  encodedArgs: {},
  finalizedAt: null,
  kind: 'test',
  maxAttempts: 10,
  metadata: {},
  priority: 1,
  queue: 'test-queue',
  state: 'scheduled' as JobState,
  tags: [],
  errors: [],
  scheduledAt: new Date(),
});

export const createTestJobSetStateIfRunningParams = () => ({
  state: 'scheduled' as JobState,
  id: 1,
  finalizedAtDoUpdate: false,
  finalizedAt: null,
  errorDoUpdate: false,
  error: null,
  maxAttemptsUpdate: true,
  maxAttempts: 3,
  scheduledAtDoUpdate: true,
  scheduledAt: new Date(),
});
