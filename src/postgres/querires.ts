/* eslint-disable max-len */
export const JOB_INSERT_FAST = `
INSERT INTO pidginmq_job(
    args,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) VALUES (
    $1::jsonb,
    $2,
    $3::text,
    $4::smallint,
    coalesce($5::jsonb, '{}'),
    $6::smallint,
    $7::text,
    coalesce($8::timestamptz, now()),
    $9::pidginmq_job_state,
    coalesce($10::varchar(255)[], '{}')
) RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`;

export const CANCEL_JOB = `
WITH locked_job AS (
    SELECT
        id, queue, state, finalized_at
    FROM pidginmq_job
    WHERE pidginmq_job.id = $1
    FOR UPDATE
),
notification AS (
    SELECT
        id,
        pg_notify($2, json_build_object('action', 'cancel', 'job_id', id, 'queue', queue)::text)
    FROM
        locked_job
    WHERE
        state NOT IN ('cancelled', 'completed', 'discarded')
        AND finalized_at IS NULL
),
updated_job AS (
    UPDATE pidginmq_job
    SET
        -- If the job is actively running, we want to let its current client and
        -- producer handle the cancellation. Otherwise, immediately cancel it.
        state = CASE WHEN state = 'running'::pidginmq_job_state THEN state ELSE 'cancelled'::pidginmq_job_state END,
        finalized_at = CASE WHEN state = 'running'::pidginmq_job_state THEN finalized_at ELSE now() END,
        -- Mark the job as cancelled by query so that the rescuer knows not to
        -- rescue it, even if it gets stuck in the running state:
        metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], $3::jsonb, true)
    FROM notification
    WHERE pidginmq_job.id = notification.id
    RETURNING pidginmq_job.id, pidginmq_job.args, pidginmq_job.attempt, pidginmq_job.attempted_at, pidginmq_job.attempted_by, pidginmq_job.created_at, pidginmq_job.errors, pidginmq_job.finalized_at, pidginmq_job.kind, pidginmq_job.max_attempts, pidginmq_job.metadata, pidginmq_job.priority, pidginmq_job.queue, pidginmq_job.state, pidginmq_job.scheduled_at, pidginmq_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`;

export const JOB_DELETE_BEFORE = `
WITH deleted_jobs AS (
    DELETE FROM pidginmq_job
    WHERE id IN (
        SELECT id
        FROM pidginmq_job
        WHERE
            (state = 'cancelled' AND finalized_at < $1::timestamptz) OR
            (state = 'completed' AND finalized_at < $2::timestamptz) OR
            (state = 'discarded' AND finalized_at < $3::timestamptz)
        ORDER BY id
        LIMIT $4::bigint
    )
    RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
)
SELECT count(*)
FROM deleted_jobs
`;

export const GET_AVAILABLE_JOBS = `
WITH locked_jobs AS (
    SELECT
        id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
    FROM
        pidginmq_job
    WHERE
        state = 'available'::pidginmq_job_state
        AND queue = $2::text
        AND scheduled_at <= now()
    ORDER BY
        priority ASC,
        scheduled_at ASC,
        id ASC
    LIMIT $3::integer
    FOR UPDATE
    SKIP LOCKED
)
UPDATE
    pidginmq_job
SET
    state = 'running'::pidginmq_job_state,
    attempt = pidginmq_job.attempt + 1,
    attempted_at = now(),
    attempted_by = array_append(pidginmq_job.attempted_by, $1::text)
FROM
    locked_jobs
WHERE
    pidginmq_job.id = locked_jobs.id
RETURNING
    pidginmq_job.id, pidginmq_job.args, pidginmq_job.attempt, pidginmq_job.attempted_at, pidginmq_job.attempted_by, pidginmq_job.created_at, pidginmq_job.errors, pidginmq_job.finalized_at, pidginmq_job.kind, pidginmq_job.max_attempts, pidginmq_job.metadata, pidginmq_job.priority, pidginmq_job.queue, pidginmq_job.state, pidginmq_job.scheduled_at, pidginmq_job.tags
`;

export const JOB_GET_BY_ID = `
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE id = $1
LIMIT 1
`;

export const JOB_GET_BY_ID_MANY = `
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM river_job
WHERE id = any($1::bigint[])
ORDER BY id
`;

export const JOB_GET_BY_KIND_AND_UNIQUE_PROPERTIES = `
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE kind = $1
    AND CASE WHEN $2::boolean THEN args = $3 ELSE true END
    AND CASE WHEN $4::boolean THEN tstzrange($5::timestamptz, $6::timestamptz, '[)') @> created_at ELSE true END
    AND CASE WHEN $7::boolean THEN queue = $8 ELSE true END
    AND CASE WHEN $9::boolean THEN state::text = any($10::text[]) ELSE true END
`;

export const GET_JOB_BY_KIND_MANY = `
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE kind = any($1::text[])
ORDER BY id
`;

export const GET_JOB_STUCK = `
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE state = 'running'::pidginmq_job_state
    AND attempted_at < $1::timestamptz
ORDER BY id
LIMIT $2
`;

export const JOB_INSERT_FULL = `
INSERT INTO pidginmq_job(
    args,
    attempt,
    attempted_at,
    created_at,
    errors,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) VALUES (
    $1::jsonb,
    coalesce($2::smallint, 0),
    $3,
    coalesce($4::timestamptz, now()),
    $5::jsonb[],
    $6,
    $7::text,
    $8::smallint,
    coalesce($9::jsonb, '{}'),
    $10::smallint,
    $11::text,
    coalesce($12::timestamptz, now()),
    $13::river_job_state,
    coalesce($14::varchar(255)[], '{}')
) RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`;

export const JOB_RESCUE_MANY = `
UPDATE pidginmq_job
SET
    errors = array_append(errors, updated_job.error),
    finalized_at = updated_job.finalized_at,
    scheduled_at = updated_job.scheduled_at,
    state = updated_job.state
FROM (
    SELECT
        unnest($1::bigint[]) AS id,
        unnest($2::jsonb[]) AS error,
        nullif(unnest($3::timestamptz[]), '0001-01-01 00:00:00 +0000') AS finalized_at,
        unnest($4::timestamptz[]) AS scheduled_at,
        unnest($5::text[])::pidginmq_job_state AS state
) AS updated_job
WHERE pidginmq_job.id = updated_job.id
`;

export const JOB_RETRY = `
WITH job_to_update AS (
    SELECT id
    FROM pidginmq_job
    WHERE pidginmq_job.id = $1
    FOR UPDATE
),
updated_job AS (
    UPDATE pidginmq_job
    SET
        state = 'available'::pidginmq_job_state,
        scheduled_at = now(),
        max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
        finalized_at = NULL
    FROM job_to_update
    WHERE pidginmq_job.id = job_to_update.id
        -- Do not touch running jobs:
        AND pidginmq_job.state != 'running'::pidginmq_job_state
        -- If the job is already available with a prior scheduled_at, leave it alone.
        AND NOT (pidginmq_job.state = 'available'::pidginmq_job_state AND pidginmq_job.scheduled_at < now())
    RETURNING pidginmq_job.id, pidginmq_job.args, pidginmq_job.attempt, pidginmq_job.attempted_at, pidginmq_job.attempted_by, pidginmq_job.created_at, pidginmq_job.errors, pidginmq_job.finalized_at, pidginmq_job.kind, pidginmq_job.max_attempts, pidginmq_job.metadata, pidginmq_job.priority, pidginmq_job.queue, pidginmq_job.state, pidginmq_job.scheduled_at, pidginmq_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE id = $1::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`;

export const JOB_SCHEDULE = `
WITH jobs_to_schedule AS (
    SELECT id
    FROM pidginmq_job
    WHERE
        state IN ('retryable', 'scheduled')
        AND queue IS NOT NULL
        AND priority >= 0
        AND scheduled_at <= $2::timestamptz
    ORDER BY
        priority,
        scheduled_at,
        id
    LIMIT $3::bigint
    FOR UPDATE
),
pidginmq_job_scheduled AS (
    UPDATE pidginmq_job
    SET state = 'available'::pidginmq_job_state
    FROM jobs_to_schedule
    WHERE pidginmq_job.id = jobs_to_schedule.id
    RETURNING jobs_to_schedule.id, pidginmq_job.id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
)
SELECT count(*)
FROM (
    SELECT pg_notify($1, json_build_object('queue', queue)::text)
    FROM pidginmq_job_scheduled
) AS notifications_sent
`;

export const JOB_SET_STATE_IF_RUNNING = `
WITH job_to_update AS (
    SELECT
      id,
      $1::pidginmq_job_state IN ('retryable'::pidginmq_job_state, 'scheduled'::pidginmq_job_state) AND metadata ? 'cancel_attempted_at' AS should_cancel
    FROM pidginmq_job
    WHERE id = $2::bigint
    FOR UPDATE
),
updated_job AS (
    UPDATE pidginmq_job
    SET
      state        = CASE WHEN should_cancel                                          THEN 'cancelled'::pidginmq_job_state
                          ELSE $1::pidginmq_job_state END,
      finalized_at = CASE WHEN should_cancel                                          THEN now()
                          WHEN $3::boolean                       THEN $4
                          ELSE finalized_at END,
      errors       = CASE WHEN $5::boolean                              THEN array_append(errors, $6::jsonb)
                          ELSE errors       END,
      max_attempts = CASE WHEN NOT should_cancel AND $7::boolean    THEN $8
                          ELSE max_attempts END,
      scheduled_at = CASE WHEN NOT should_cancel AND $9::boolean THEN $10::timestamptz
                          ELSE scheduled_at END
    FROM job_to_update
    WHERE pidginmq_job.id = job_to_update.id
        AND pidginmq_job.state = 'running'::pidginmq_job_state
    RETURNING pidginmq_job.id, pidginmq_job.args, pidginmq_job.attempt, pidginmq_job.attempted_at, pidginmq_job.attempted_by, pidginmq_job.created_at, pidginmq_job.errors, pidginmq_job.finalized_at, pidginmq_job.kind, pidginmq_job.max_attempts, pidginmq_job.metadata, pidginmq_job.priority, pidginmq_job.queue, pidginmq_job.state, pidginmq_job.scheduled_at, pidginmq_job.tags
)
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM pidginmq_job
WHERE id = $2::bigint
    AND id NOT IN (SELECT id FROM updated_job)
UNION
SELECT id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
FROM updated_job
`;

export const JOB_UPDATE = `
UPDATE pidginmq_job
SET
    attempt = CASE WHEN $1::boolean THEN $2 ELSE attempt END,
    attempted_at = CASE WHEN $3::boolean THEN $4 ELSE attempted_at END,
    errors = CASE WHEN $5::boolean THEN $6::jsonb[] ELSE errors END,
    finalized_at = CASE WHEN $7::boolean THEN $8 ELSE finalized_at END,
    state = CASE WHEN $9::boolean THEN $10 ELSE state END
WHERE id = $11
RETURNING id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags
`;

export const LEADER_ATTEMPT_ELECT = `
INSERT INTO pidginmq_leader(name, leader_id, elected_at, expires_at)
    VALUES ($1::text, $2::text, now(), now() + $3::interval)
ON CONFLICT (name)
    DO NOTHING
`;

export const LEADER_ATTEMPT_REELECT = `
INSERT INTO pidginmq_leader(name, leader_id, elected_at, expires_at)
    VALUES ($1::text, $2::text, now(), now() + $3::interval)
ON CONFLICT (name)
    DO UPDATE SET
        expires_at = now() + $3::interval
    WHERE
        pidginmq_leader.leader_id = $2::text
`;

export const DELETE_EXPIRED_LEADER = `
DELETE FROM pidginmq_leader
WHERE name = $1::text
    AND expires_at < now()
`;

export const GET_ELECTED_LEADER = `
SELECT elected_at, expires_at, leader_id, name
FROM pidginmq_leader
WHERE name = $1
`;

export const INSERT_LEADER = `
INSERT INTO pidginmq_leader(
    elected_at,
    expires_at,
    leader_id,
    name
) VALUES (
    coalesce($1::timestamptz, now()),
    coalesce($2::timestamptz, now() + $3::interval),
    $4,
    $5
) RETURNING elected_at, expires_at, leader_id, name
`;

export const LEADER_RESIGN = `
WITH currently_held_leaders AS (
  SELECT elected_at, expires_at, leader_id, name
  FROM pidginmq_leader
  WHERE
      name = $1::text
      AND leader_id = $2::text
  FOR UPDATE
),
notified_resignations AS (
  SELECT
      pg_notify($3, json_build_object('name', name, 'leader_id', leader_id, 'action', 'resigned')::text),
      currently_held_leaders.name
  FROM currently_held_leaders
)
DELETE FROM pidginmq_leader USING notified_resignations
WHERE pidginmq_leader.name = notified_resignations.name
`;
