-- name: CreateTask :one
INSERT INTO tasks (
    queue_id, task_name, params, headers,
    retry_strategy, max_attempts, start_timeout,
    execution_timeout, workflow_run_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;

-- name: GetTask :one
SELECT * FROM tasks WHERE id = $1;

-- name: ListTasks :many
SELECT t.id, t.task_name, t.status, t.max_attempts, t.created_at, t.completed_at,
       q.name as queue_name
FROM tasks t
JOIN queues q ON q.id = t.queue_id
WHERE (q.name = sqlc.narg('queue_name') OR sqlc.narg('queue_name') IS NULL)
  AND (t.status::text = sqlc.narg('status') OR sqlc.narg('status') IS NULL)
  AND (t.task_name = sqlc.narg('task_name') OR sqlc.narg('task_name') IS NULL)
  AND (sqlc.narg('cursor_id')::uuid IS NULL OR t.id < sqlc.narg('cursor_id')::uuid)
ORDER BY t.id DESC
LIMIT sqlc.arg('lim');

-- name: UpdateTaskStatus :exec
UPDATE tasks
SET status = $2, completed_at = $3
WHERE id = $1;
