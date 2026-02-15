-- name: CreateQueue :one
INSERT INTO queues (name, task_ttl_seconds, event_ttl_seconds)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetQueueByName :one
SELECT * FROM queues WHERE name = $1;

-- name: GetQueueStats :one
SELECT
    COUNT(*) FILTER (WHERE r.status = 'pending') AS pending_runs,
    COUNT(*) FILTER (WHERE r.status = 'claimed') AS claimed_runs,
    COUNT(*) FILTER (WHERE r.status = 'completed') AS completed_runs,
    EXTRACT(EPOCH FROM (now() - MIN(r.created_at) FILTER (WHERE r.status = 'pending'))) AS oldest_pending_age_seconds
FROM runs r
JOIN tasks t ON t.id = r.task_id
WHERE t.queue_id = @queue_id;
