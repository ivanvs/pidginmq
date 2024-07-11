import { DbJob } from './postgres/db.job.js';
import { DbLeader } from './postgres/db.leader.js';
import { DbQueue } from './postgres/db.queue.js';
import {
  CANCEL_JOB,
  DELETE_EXPIRED_LEADER,
  GET_AVAILABLE_JOBS,
  GET_ELECTED_LEADER,
  GET_JOB_BY_KIND_MANY,
  GET_JOB_STUCK,
  GET_QUEUE,
  INSERT_LEADER,
  JOB_DELETE,
  JOB_DELETE_BEFORE,
  JOB_GET_BY_ID,
  JOB_GET_BY_ID_MANY,
  JOB_GET_BY_KIND_AND_UNIQUE_PROPERTIES,
  JOB_INSERT_FAST,
  JOB_INSERT_FULL,
  JOB_RESCUE_MANY,
  JOB_RETRY,
  JOB_SCHEDULE,
  JOB_SET_STATE_IF_RUNNING,
  JOB_UPDATE,
  LEADER_ATTEMPT_ELECT,
  LEADER_ATTEMPT_REELECT,
  LEADER_RESIGN,
  PAUSE_QUEUE,
  PG_NOTIFY,
  QUEUE_CREATE_OR_SET_UPDATE_AT,
  QUEUE_DELETE_EXPIRED,
  QUEUE_LIST,
  RESUME_QUEUE,
} from './postgres/querires.js';
import { DbDriver } from './types/db.driver.js';
import { Job, JobControlPayload, JobState } from './types/job.js';
import { Leader } from './types/leader.js';
import { Queue } from './types/queue.js';

export interface RepetableJobParams {
  cron: string;
  limit: number;
  every: number;
}

export interface InsertRepetableJobParams extends InsertJobParams {
  repeat: RepetableJobParams;
}

export interface InsertJobParams {
  args: any;
  kind: string;
  maxAttempts: number;
  metadata: any;
  priority: number;
  queue: string;
  scheduletAt: Date;
  tags: string[];
}

export interface CancelJobParams {
  id: number;
  jobControlTopic: string;
  cancelAttemtedAt: Date;
}

export interface JobDeleteBeforeParams {
  cancelledFinalizedAtHorizon: Date;
  completedFinalizedAtHorizon: Date;
  discardedFinalizedAtHorizon: Date;
  max: number;
}

export interface JobGetAvailableParams {
  attemptedBy: string;
  queue: string;
  max: number;
}

export interface JobGetByKindAndUniquePropertiesParams {
  kind: string;
  byArgs: boolean;
  args: any;
  byCreatedAt: boolean;
  createdAtBegin: Date;
  createdAtEnd: Date;
  byQueue: boolean;
  queue: string;
  byState: boolean;
  state: string[];
}

export interface GetJobStuckParams {
  stuckHorizon: Date;
  max: number;
}

export interface JobInsertFullParams {
  args: any;
  attempt: number;
  attemptedAt: Date;
  createdAt: Date;
  errors: any;
  finalizedAt: Date;
  kind: string;
  maxAttempts: number;
  metadata: any;
  priority: number;
  queue: string;
  scheduledAt: Date;
  state: JobState;
  tags: string[];
}

export interface JobRescueManyParams {
  ids: number[];
  error: any[];
  finalizedAt: Date[];
  scheduledAt: Date[];
  state: string[];
}

export interface JobScheduleParams {
  insertTopic: string;
  now: Date;
  max: number;
}

export interface JobSetStateIfRunningParams {
  state?: JobState;
  id: number;
  finalizedAtDoUpdate?: boolean;
  finalizedAt?: Date;
  errorDoUpdate?: boolean;
  error?: any;
  maxAttemptsUpdate?: boolean;
  maxAttempts?: number;
  scheduledAtDoUpdate?: boolean;
  scheduledAt?: Date;
}

export interface JobUpdateParams {
  attemptDoUpdate: boolean;
  attempt: number;
  attemptedAtDoUpdate: boolean;
  attemptedAt: Date;
  errorsDoUpdate: boolean;
  errors: any[];
  finalizedAtDoUpdate: boolean;
  finalizedAt: Date;
  stateDoUpdate: boolean;
  state: JobState;
  id: number;
}

export interface LeaderAttemptElectParams {
  name: string;
  leaderID: string;
  ttl: number;
}

export interface LeaderAttemptReelectParams {
  name: string;
  leaderID: string;
  ttl: number;
}

export interface LeaderInsertParams {
  electedAt: Date;
  expiresAt: Date;
  ttl: number;
  leaderID: string;
  name: string;
}

export interface LeaderResignParams {
  name: string;
  leaderID: string;
  leadershipTopic: string;
}

export interface JobQueryParams {
  kinds: string[];
  limitCount: number;
  skip: number;
  queues: string[];
  // sortField        JobListOrderByField
  // sortOrder        SortOrder
  state: JobState;
}

export interface QueueCreateOrSetUpdateAtParams {
  metadata: string;
  name: string;
  pausedAt?: Date;
  updatedAt?: Date;
}

export interface QueueDeleteExpiredParams {
  updatedAtHorizon: Date;
  max: number;
}

export class Executor {
  constructor(private db: DbDriver) {}

  async insertJob(params: InsertJobParams): Promise<Job> {
    const values = [
      params.args, //1
      null, //2
      params.kind, //3
      params.maxAttempts, //4
      params.metadata, //5
      params.priority, //6
      params.queue, //7
      params.scheduletAt, //8
      'available', //9
      params.tags, //10
    ];
    const jobs = await this.db.execute(JOB_INSERT_FAST, ...values);

    const dbJob = jobs.rows.length === 1 ? jobs.rows[0] : null;

    return this.toJob(dbJob);
  }

  private toJob(dbJob: DbJob): Job {
    if (dbJob) {
      return {
        id: parseInt(dbJob.id, 10),
        attempt: dbJob.attempt,
        attemptedAt: dbJob.attempted_at,
        attemptedBy: dbJob.attempted_by,
        createdAt: dbJob.created_at,
        encodedArgs: dbJob.encoded_args,
        finalizedAt: dbJob.finalized_at,
        kind: dbJob.kind,
        maxAttempts: dbJob.max_attempts,
        metadata: dbJob.metadata,
        priority: dbJob.priority,
        queue: dbJob.queue,
        state: dbJob.state,
        tags: dbJob.tags,
        errors: dbJob.errors,
        scheduledAt: dbJob.scheduled_at,
      };
    }

    return null;
  }

  async cancelJob(params: CancelJobParams): Promise<Job> {
    const values = [params.id, params.jobControlTopic, params.cancelAttemtedAt];
    const result = await this.db.execute(CANCEL_JOB, ...values);

    const dbJob = result.rows.length === 1 ? result.rows[0] : null;

    return this.toJob(dbJob);
  }

  async jobDelete(id: number): Promise<Job> {
    const result = await this.db.execute(JOB_DELETE, id);
    return result.rows.length === 1 ? result.rows[0] : 0;
  }

  async jobDeleteBefore(params: JobDeleteBeforeParams): Promise<number> {
    const values = [
      params.cancelledFinalizedAtHorizon,
      params.completedFinalizedAtHorizon,
      params.discardedFinalizedAtHorizon,
      params.max,
    ];

    const result = await this.db.execute(JOB_DELETE_BEFORE, ...values);

    return result.rows.length === 1 ? result.rows[0] : 0;
  }

  async jobGetAvailable(params: JobGetAvailableParams): Promise<Job[]> {
    const values = [params.attemptedBy, params.queue, params.max];

    const result = await this.db.execute(GET_AVAILABLE_JOBS, ...values);

    return result.rows.map((x) => this.toJob(x));
  }

  async getJobById(id: number): Promise<Job> {
    const result = await this.db.execute(JOB_GET_BY_ID, id);

    return result.rows.length === 1 ? this.toJob(result.rows[0]) : null;
  }

  async getJobByIdMultiple(ids: number[]): Promise<Job[]> {
    const result = await this.db.execute(JOB_GET_BY_ID_MANY, ids);

    return result.rows.map((x) => this.toJob(x));
  }

  async getJobByKindAndUniqueProperties(
    params: JobGetByKindAndUniquePropertiesParams,
  ): Promise<Job> {
    const values = [
      params.kind,
      params.byArgs,
      params.args,
      params.byCreatedAt,
      params.createdAtBegin,
      params.createdAtEnd,
      params.byQueue,
      params.queue,
      params.byState,
    ];
    const result = await this.db.execute(
      JOB_GET_BY_KIND_AND_UNIQUE_PROPERTIES,
      ...values,
    );

    return result.rows.length === 1 ? this.toJob(result.rows[0]) : null;
  }

  async getJobByKindMany(kinds: string[]): Promise<Job[]> {
    const result = await this.db.execute(GET_JOB_BY_KIND_MANY, kinds);

    return result.rows.map((x) => this.toJob(x));
  }

  async getStuckJob(params: GetJobStuckParams): Promise<Job[]> {
    const result = await this.db.execute(
      GET_JOB_STUCK,
      params.stuckHorizon,
      params.max,
    );

    return result.rows.map((x) => this.toJob(x));
  }

  async insertJobFull(params: JobInsertFullParams): Promise<Job> {
    const values = [
      params.args,
      params.attempt,
      params.attemptedAt,
      params.createdAt,
      params.errors,
      params.finalizedAt,
      params.kind,
      params.maxAttempts,
      params.metadata,
      params.priority,
      params.queue,
      params.scheduledAt,
      params.state,
      params.tags,
    ];
    const result = await this.db.execute(JOB_INSERT_FULL, ...values);
    return result.rows.length === 1 ? this.toJob(result.rows[0]) : null;
  }

  async rescueManyJobs(params: JobRescueManyParams): Promise<Job[]> {
    const values = [
      params.ids,
      params.error,
      params.finalizedAt,
      params.scheduledAt,
      params.state,
    ];
    const result = await this.db.execute(JOB_RESCUE_MANY, ...values);
    return result.rows.map((x) => this.toJob(x));
  }

  async retryJob(id: number): Promise<Job> {
    const result = await this.db.execute(JOB_RETRY, id);
    return result.rows.length === 1 ? this.toJob(result.rows[0]) : null;
  }

  async scheduleJob(params: JobScheduleParams): Promise<number> {
    const result = await this.db.execute(
      JOB_SCHEDULE,
      params.insertTopic,
      params.now,
      params.max,
    );
    return result.rows.length === 1 ? result.rows[0] : 0;
  }

  async jobSetStateIfRunning(params: JobSetStateIfRunningParams): Promise<Job> {
    const values = [
      params.state,
      params.id,
      params.finalizedAtDoUpdate,
      params.finalizedAt,
      params.errorDoUpdate,
      params.error,
      params.maxAttemptsUpdate,
      params.maxAttempts,
      params.scheduledAtDoUpdate,
      params.scheduledAt,
    ];
    const result = await this.db.execute(JOB_SET_STATE_IF_RUNNING, ...values);
    return result.rows.length === 1 ? this.toJob(result.rows[0]) : null;
  }

  async updateJob(params: JobUpdateParams): Promise<Job> {
    const values = [
      params.attemptDoUpdate,
      params.attempt,
      params.attemptedAtDoUpdate,
      params.attemptedAt,
      params.errorsDoUpdate,
      params.errors,
      params.finalizedAtDoUpdate,
      params.finalizedAt,
      params.stateDoUpdate,
      params.state,
      params.id,
    ];
    const result = await this.db.execute(JOB_UPDATE, ...values);
    return result.rows.length === 1 ? this.toJob(result.rows[0]) : null;
  }

  async attemptElectLeader(params: LeaderAttemptElectParams): Promise<number> {
    const values = [params.name, params.leaderID, params.ttl];
    const result = await this.db.execute(LEADER_ATTEMPT_ELECT, ...values);
    return result.rowCount;
  }

  async attemptReelectLeader(
    params: LeaderAttemptReelectParams,
  ): Promise<number> {
    const values = [params.name, params.leaderID, params.ttl];
    const result = await this.db.execute(LEADER_ATTEMPT_REELECT, ...values);
    return result.rowCount;
  }

  async deleteExpiredLeader(name: string): Promise<number> {
    const result = await this.db.execute(DELETE_EXPIRED_LEADER, name);
    return result.rowCount;
  }

  private toLeader(leader: DbLeader): Leader {
    return {
      electedAt: leader.elected_at,
      expiresAt: leader.expires_at,
      name: leader.name,
      leaderID: leader.leader_id,
    };
  }

  private toQueue(queue: DbQueue): Queue {
    return {
      createdAt: queue.created_at,
      updatedAt: queue.updated_at,
      name: queue.name,
      metadata: queue.metadata,
      pausedAt: queue.paused_at,
    };
  }

  async getElectedLeader(name: string): Promise<Leader> {
    const result = await this.db.execute(GET_ELECTED_LEADER, name);
    return result.rows.length === 1 ? this.toLeader(result.rows[0]) : null;
  }

  async insertLeader(params: LeaderInsertParams): Promise<Leader> {
    const values = [
      params.electedAt,
      params.expiresAt,
      params.ttl,
      params.leaderID,
      params.name,
    ];
    const result = await this.db.execute(INSERT_LEADER, ...values);
    return result.rows.length === 1 ? this.toLeader(result.rows[0]) : null;
  }

  async reasignLeader(params: LeaderResignParams): Promise<number> {
    const values = [params.name, params.leaderID, params.leadershipTopic];
    const result = await this.db.execute(LEADER_RESIGN, ...values);
    return result.rowCount;
  }

  async queryJobs(params: JobQueryParams): Promise<Job[]> {
    let queryJobs = `
                    SELECT
                      id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, 
                      metadata, priority, queue, state, scheduled_at, tags
                    FROM
                      pidginmq_job`;
    const values = [];
    let condition = '';

    const addWhereOrAnd = (condition) => {
      if (condition.length === 0) {
        return 'WHERE ';
      } else {
        return condition + ' AND ';
      }
    };

    if (params.kinds.length > 0) {
      condition =
        addWhereOrAnd(condition) + `kind = any(@${values.length + 1}::text[])`;
      values.push(params.kinds);
    }

    if (params.queues.length > 0) {
      condition =
        addWhereOrAnd(condition) + `queue = any(@${values.length + 1}::text[])`;
      values.push(params.queues);
    }

    if (params.state) {
      condition =
        addWhereOrAnd(condition) +
        `state = @${values.length + 1}::pdiginmq_job_state`;
      values.push(params.state);
    }

    queryJobs += condition + ` LIMIT @${values.length + 1}::integer`;
    values.push(params.limitCount);

    if (params.skip) {
      queryJobs += ` OFFSET @${values.length + 1}::integer`;
      values.push(params.skip);
    }

    const result = await this.db.execute(queryJobs, ...values);
    return result.rows.map((x) => this.toJob(x));
  }

  async queueResume(name: string): Promise<Queue> {
    const result = await this.db.execute(RESUME_QUEUE, name);
    return result.rows.length === 1 ? this.toQueue(result.rows[0]) : null;
  }

  async queuePause(name: string): Promise<Queue> {
    const result = await this.db.execute(PAUSE_QUEUE, name);
    return result.rows.length === 1 ? this.toQueue(result.rows[0]) : null;
  }

  async queryQueues(limit: number): Promise<Queue[]> {
    const result = await this.db.execute(QUEUE_LIST, limit);
    return result.rows.map((x) => this.toQueue(x));
  }

  async queueGet(name: string): Promise<Queue> {
    const result = await this.db.execute(GET_QUEUE, name);
    return result.rows.length === 1 ? this.toQueue(result.rows[0]) : null;
  }

  async queueDeleteExpired(params: QueueDeleteExpiredParams): Promise<Queue[]> {
    const values = [params.updatedAtHorizon, params.max];
    const result = await this.db.execute(QUEUE_DELETE_EXPIRED, ...values);
    return result.rows.map((x) => this.toJob(x));
  }

  async queueCreateOrSetUpdateAt(
    params: QueueCreateOrSetUpdateAtParams,
  ): Promise<Queue> {
    const values = [
      params.metadata,
      params.name,
      params.pausedAt,
      params.updatedAt,
    ];
    const result = await this.db.execute(
      QUEUE_CREATE_OR_SET_UPDATE_AT,
      ...values,
    );

    return result.rows.length === 1 ? this.toQueue(result.rows[0]) : null;
  }

  async pgNotify(topic: string, payload: JobControlPayload) {
    const values = [topic, JSON.stringify(payload)];
    await this.db.execute(PG_NOTIFY, ...values);
  }
}
