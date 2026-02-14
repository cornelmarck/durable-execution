-- name: UpsertCheckpoint :one
INSERT INTO checkpoints (task_id, step_name, state, owner_run_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (task_id, step_name) DO UPDATE
SET state = EXCLUDED.state,
    owner_run_id = EXCLUDED.owner_run_id,
    updated_at = now()
RETURNING *;

-- name: GetCheckpoint :one
SELECT * FROM checkpoints
WHERE task_id = $1 AND step_name = $2;

-- name: ExtendRunClaim :exec
UPDATE runs
SET claim_expires_at = now() + make_interval(secs => @extend_by_seconds::int)
WHERE id = $1;
