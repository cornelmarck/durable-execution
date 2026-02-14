-- name: CreateTask :one
INSERT INTO tasks (
    queue_id, task_name, params, headers,
    retry_strategy, max_attempts, start_timeout,
    execution_timeout, workflow_run_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;

-- name: GetTask :one
SELECT * FROM tasks WHERE id = $1;

-- name: UpdateTaskStatus :exec
UPDATE tasks
SET status = $2, completed_at = $3
WHERE id = $1;
