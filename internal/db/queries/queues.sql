-- name: CreateQueue :one
INSERT INTO queues (name, task_ttl_seconds, event_ttl_seconds)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetQueueByName :one
SELECT * FROM queues WHERE name = $1;
