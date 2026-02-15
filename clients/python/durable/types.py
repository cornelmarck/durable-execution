from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


# --- Enums ---


class TaskStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class RunStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"
    SLEEPING = "sleeping"


class WorkflowRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class WaitResult(StrEnum):
    RESOLVED = "resolved"
    SLEEPING = "sleeping"
    TIMEOUT = "timeout"


class RetryStrategyKind(StrEnum):
    FIXED = "fixed"
    EXPONENTIAL = "exponential"


# --- Shared types ---


@dataclass
class RetryStrategy:
    kind: RetryStrategyKind
    base_seconds: float
    factor: float | None = None
    max_seconds: float | None = None


@dataclass
class CleanupPolicy:
    task_ttl_seconds: int | None = None
    event_ttl_seconds: int | None = None


# --- Error ---


@dataclass
class ErrorResponse:
    error: str = ""
    code: str = ""


# --- Queue ---


@dataclass
class CreateQueueRequest:
    name: str
    cleanup: CleanupPolicy | None = None


@dataclass
class CreateQueueResponse:
    name: str
    created_at: str
    cleanup: CleanupPolicy | None = None


# --- Task ---


@dataclass
class SpawnTaskRequest:
    task_name: str
    params: Any | None = None
    headers: dict[str, str] | None = None
    retry_strategy: RetryStrategy | None = None
    max_attempts: int | None = None
    start_timeout: int | None = None
    execution_timeout: int | None = None
    workflow_run_id: str | None = None


@dataclass
class SpawnTaskResponse:
    task_id: str
    run_id: str
    workflow_run_id: str | None = None


@dataclass
class ClaimTasksRequest:
    limit: int
    claim_timeout: int
    long_poll_seconds: int | None = None


@dataclass
class ClaimedTask:
    run_id: str
    task_id: str
    attempt: int
    task_name: str
    max_attempts: int
    params: Any | None = None
    headers: dict[str, str] | None = None
    retry_strategy: RetryStrategy | None = None


@dataclass
class ClaimTasksResponse:
    tasks: list[ClaimedTask] = field(default_factory=list)


@dataclass
class TaskSummary:
    id: str
    task_name: str
    status: TaskStatus
    queue_name: str
    max_attempts: int
    created_at: str
    completed_at: str | None = None


@dataclass
class ListTasksResponse:
    tasks: list[TaskSummary] = field(default_factory=list)
    next_cursor: str | None = None


# --- Run ---


@dataclass
class CompleteRunRequest:
    result: Any | None = None


@dataclass
class CompleteRunResponse:
    run_id: str
    status: RunStatus


@dataclass
class FailRunRequest:
    error: str


@dataclass
class FailRunResponse:
    run_id: str
    status: RunStatus
    attempt: int
    next_run_id: str | None = None
    next_attempt_at: str | None = None


@dataclass
class ScheduleRunRequest:
    run_at: str


@dataclass
class ScheduleRunResponse:
    run_id: str
    scheduled_at: str


@dataclass
class WaitForEventRequest:
    event_name: str
    timeout_seconds: int
    task_id: str
    step_name: str


@dataclass
class WaitForEventResponse:
    status: WaitResult
    payload: Any | None = None
    message: str | None = None


# --- Checkpoint ---


@dataclass
class SetCheckpointRequest:
    state: Any
    owner_run: str
    extend_claim_by: int | None = None


@dataclass
class SetCheckpointResponse:
    task_id: str
    step_name: str
    updated_at: str


@dataclass
class GetCheckpointResponse:
    task_id: str
    step_name: str
    state: Any
    updated_at: str


# --- Event ---


@dataclass
class EmitEventRequest:
    event_name: str
    payload: Any | None = None


@dataclass
class EmitEventResponse:
    event_id: str
    event_name: str
    created_at: str


# --- Workflow Run ---


@dataclass
class CreateWorkflowRunRequest:
    workflow_name: str
    workflow_version: str | None = None
    inputs: Any | None = None
    created_by: str | None = None
    tags: dict[str, str] | None = None


@dataclass
class CreateWorkflowRunResponse:
    workflow_run_id: str
    workflow_name: str
    status: WorkflowRunStatus
    created_at: str


@dataclass
class UpdateWorkflowRunRequest:
    status: WorkflowRunStatus | None = None
    result: Any | None = None
    started_at: str | None = None
    completed_at: str | None = None


@dataclass
class UpdateWorkflowRunResponse:
    workflow_run_id: str
    status: WorkflowRunStatus
    updated_at: str
