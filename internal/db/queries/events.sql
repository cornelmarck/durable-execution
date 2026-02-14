-- name: CreateEvent :one
INSERT INTO events (event_name, payload)
VALUES ($1, $2)
RETURNING *;

-- name: GetEventByName :one
SELECT * FROM events
WHERE event_name = $1
ORDER BY created_at DESC
LIMIT 1;

-- name: CleanupEvents :exec
DELETE FROM events
WHERE created_at < now() - make_interval(secs => @ttl_seconds::int);
