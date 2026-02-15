-- Enum types
CREATE TYPE task_status AS ENUM ('pending', 'completed', 'failed', 'canceled');
CREATE TYPE run_status AS ENUM ('pending', 'claimed', 'completed', 'failed', 'sleeping');
CREATE TYPE workflow_run_status AS ENUM ('pending', 'running', 'completed', 'failed', 'canceled');

-- Queues
CREATE TABLE IF NOT EXISTS queues (
    id               UUID PRIMARY KEY DEFAULT uuidv7(),
    name             TEXT UNIQUE NOT NULL,
    task_ttl_seconds INT  NOT NULL,
    event_ttl_seconds INT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Workflow runs (created before tasks so tasks can FK to it)
CREATE TABLE IF NOT EXISTS workflow_runs (
    id               UUID PRIMARY KEY DEFAULT uuidv7(),
    workflow_name    TEXT NOT NULL,
    workflow_version TEXT,
    status           workflow_run_status NOT NULL DEFAULT 'pending',
    inputs           JSONB,
    result           JSONB,
    created_by       TEXT,
    tags             JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Tasks
CREATE TABLE IF NOT EXISTS tasks (
    id                UUID PRIMARY KEY DEFAULT uuidv7(),
    queue_id          UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    task_name         TEXT NOT NULL,
    params            JSONB,
    headers           JSONB,
    retry_strategy    JSONB,
    max_attempts      INT  NOT NULL DEFAULT 1,
    start_timeout     INT  NOT NULL DEFAULT 0,
    execution_timeout INT  NOT NULL DEFAULT 0,
    workflow_run_id   UUID REFERENCES workflow_runs(id),
    status            task_status NOT NULL DEFAULT 'pending',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tasks_queue_status ON tasks (queue_id, status);

-- Runs
CREATE TABLE IF NOT EXISTS runs (
    id                 UUID PRIMARY KEY DEFAULT uuidv7(),
    task_id            UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt            INT  NOT NULL DEFAULT 1,
    status             run_status NOT NULL DEFAULT 'pending',
    result             JSONB,
    error              TEXT,
    claimed_at         TIMESTAMPTZ,
    claim_expires_at   TIMESTAMPTZ,
    scheduled_at       TIMESTAMPTZ,
    waiting_event_name TEXT,
    waiting_step_name  TEXT,
    waiting_timeout_at TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runs_task_id ON runs (task_id);
CREATE INDEX IF NOT EXISTS idx_runs_claim ON runs (status, scheduled_at, claim_expires_at);
CREATE INDEX IF NOT EXISTS idx_runs_waiting ON runs (status, waiting_event_name);

-- Checkpoints
CREATE TABLE IF NOT EXISTS checkpoints (
    id           UUID PRIMARY KEY DEFAULT uuidv7(),
    task_id      UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    step_name    TEXT NOT NULL,
    state        JSONB NOT NULL,
    owner_run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (task_id, step_name)
);

-- Events
CREATE TABLE IF NOT EXISTS events (
    id         UUID PRIMARY KEY DEFAULT uuidv7(),
    event_name TEXT NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_name_created ON events (event_name, created_at);
