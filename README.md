# Durable Execution

A minimalistic durable task queue backed by PostgreSQL.

Conceptually based on [Absurd](https://github.com/earendil-works/absurd), but implemented as an HTTP server instead of an embedded library, with additional features like long polling and queue monitoring. The implementation is my own, with help from Claude.

Note that this is experimental software. For production workloads, please use an actual message queue such as SQS instead.

## Features

- **Durable tasks with retries** — tasks survive process crashes. Failed attempts are retried automatically with configurable fixed or exponential backoff.
- **Checkpointing** — persist intermediate state keyed by (task, step). Retried tasks resume from the last checkpoint instead of starting over.
- **Event coordination** — runs can sleep until a named event arrives, enabling cross-workflow synchronization without polling.
- **Long polling** — claim requests hold the connection open until work is available, using PostgreSQL LISTEN/NOTIFY for near-instant wake-up.
- **Workflow tracking** — group related tasks under a workflow run for end-to-end observability.
- **Queue monitoring** — per-queue stats (pending, claimed, completed runs and consumer lag) for alerting and dashboards.
- **Claim timeouts** — if a worker doesn't complete or fail a run before the claim expires, the run becomes claimable again. No stuck tasks.
- **Scheduling** — defer runs to a future time.

## Architecture

All state lives in PostgreSQL. There is no separate message broker — the database is the queue.

- **Claiming** uses `SELECT ... FOR UPDATE SKIP LOCKED` for contention-free, exactly-once delivery.
- **Notifications** use a trigger on the `runs` table that fires `pg_notify` with the queue name, waking only the relevant long-polling consumers.
- **Transactions** ensure that state transitions (claim, complete, fail, sleep, wake) are atomic.

## API

The server exposes a REST API on port 8080. Interactive documentation is available at `/docs` (Swagger UI).

See [`api/openapi.yaml`](api/openapi.yaml) for the full specification.

## Quick start

```bash
docker compose up
```

This starts PostgreSQL (with migrations applied automatically) and the server on port 8080.

```bash
# Spawn and claim a task
curl -s localhost:8080/api/v1/queues/demo/tasks \
  -d '{"task_name":"hello","params":{"msg":"world"}}'

curl -s localhost:8080/api/v1/queues/demo/tasks/claim \
  -d '{"limit":1,"claim_timeout":60}'
```

## CLI

A command-line tool for interacting with the server.

```bash
go install ./cmd/cli
```

### Commands

```
durable queues create --name <queue>    Create a queue
durable queues stats <queue>            Get queue statistics
durable tasks spawn --queue <q> --name <task>   Spawn a task
durable tasks claim --queue <q>         Claim tasks
durable tasks list                      List tasks
durable runs complete <run_id>          Complete a run
durable runs fail <run_id> --error <msg>  Fail a run
durable events emit --name <event>      Emit an event
```

### Example: queue stats

```
$ durable queues stats demo
{
  "queue_name": "demo",
  "pending_runs": 12,
  "claimed_runs": 3,
  "completed_runs": 47,
  "oldest_pending_run_age_seconds": 4.82
}
```

Use `--server` or `DURABLE_SERVER` to point at a different host:

```bash
durable --server https://prod:8080 queues stats demo
```

## Clients

- **Go** — `clients/go`
- **Python** — `clients/python` (sync, based on httpx)
- **CLI** — `cmd/cli`

## Project structure

```
api/                  OpenAPI spec and Go API types
clients/go/           Go HTTP client
clients/python/       Python HTTP client
cmd/cli/              CLI tool
examples/             Example workflows
internal/
  db/                 Store, listener, migrations
  db/gen/             sqlc-generated code
  db/queries/         SQL queries
  server/             HTTP handlers and routing
  service/            Business logic
```