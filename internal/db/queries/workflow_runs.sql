-- name: CreateWorkflowRun :one
INSERT INTO workflow_runs (workflow_name, workflow_version, inputs, created_by, tags)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: UpdateWorkflowRun :one
UPDATE workflow_runs
SET status = COALESCE(sqlc.narg('status'), status),
    result = COALESCE(sqlc.narg('result'), result),
    started_at = COALESCE(sqlc.narg('started_at'), started_at),
    completed_at = COALESCE(sqlc.narg('completed_at'), completed_at),
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: GetWorkflowRun :one
SELECT * FROM workflow_runs WHERE id = $1;
