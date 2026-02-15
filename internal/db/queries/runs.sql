-- name: CreateRun :one
INSERT INTO runs (task_id, attempt, scheduled_at)
VALUES ($1, $2, $3)
RETURNING *;

-- name: ClaimRuns :many
UPDATE runs
SET status = 'claimed',
    claimed_at = now(),
    claim_expires_at = now() + make_interval(secs => @claim_timeout_seconds::int)
WHERE id IN (
    SELECT r.id
    FROM runs r
    JOIN tasks t ON t.id = r.task_id
    WHERE t.queue_id = @queue_id
      AND (
          r.status = 'pending'
          OR (r.status = 'claimed' AND r.claim_expires_at <= now())
      )
      AND (r.scheduled_at IS NULL OR r.scheduled_at <= now())
    ORDER BY r.created_at ASC
    FOR UPDATE OF r SKIP LOCKED
    LIMIT sqlc.arg('limit')
)
RETURNING runs.*,
    (SELECT t.task_name FROM tasks t WHERE t.id = runs.task_id) AS task_name,
    (SELECT t.params FROM tasks t WHERE t.id = runs.task_id) AS task_params,
    (SELECT t.headers FROM tasks t WHERE t.id = runs.task_id) AS task_headers,
    (SELECT t.retry_strategy FROM tasks t WHERE t.id = runs.task_id) AS task_retry_strategy,
    (SELECT t.max_attempts FROM tasks t WHERE t.id = runs.task_id) AS task_max_attempts;

-- name: CompleteRun :exec
UPDATE runs
SET status = 'completed', result = $2, completed_at = now()
WHERE id = $1;

-- name: FailRun :exec
UPDATE runs
SET status = 'failed', error = $2, completed_at = now()
WHERE id = $1;

-- name: ScheduleRun :exec
UPDATE runs
SET scheduled_at = $2
WHERE id = $1;

-- name: SetRunSleeping :exec
UPDATE runs
SET status = 'sleeping',
    waiting_event_name = $2,
    waiting_step_name = $3,
    waiting_timeout_at = $4
WHERE id = $1;

-- name: WakeRunsByEvent :many
UPDATE runs
SET status = 'pending',
    waiting_event_name = NULL,
    waiting_step_name = NULL,
    waiting_timeout_at = NULL
WHERE status = 'sleeping' AND waiting_event_name = $1
RETURNING *;

-- name: GetRun :one
SELECT * FROM runs WHERE id = $1;

-- name: GetRunsByTask :many
SELECT * FROM runs WHERE task_id = $1 ORDER BY attempt ASC;

-- name: ListRuns :many
SELECT r.id, r.task_id, r.attempt, r.status, r.error, r.created_at, r.completed_at
FROM runs r
WHERE (r.task_id = sqlc.narg('task_id') OR sqlc.narg('task_id') IS NULL)
  AND (r.status::text = sqlc.narg('status') OR sqlc.narg('status') IS NULL)
  AND (sqlc.narg('cursor_id')::uuid IS NULL OR r.id < sqlc.narg('cursor_id')::uuid)
ORDER BY r.id DESC
LIMIT sqlc.arg('lim');

-- name: ExpireTimedOutWaiters :many
UPDATE runs
SET status = 'failed', error = 'wait timeout expired', completed_at = now()
WHERE status = 'sleeping' AND waiting_timeout_at <= now()
RETURNING *;

-- name: CleanupCompletedRuns :exec
DELETE FROM runs
WHERE status IN ('completed', 'failed')
  AND completed_at < now() - make_interval(secs => @ttl_seconds::int);
