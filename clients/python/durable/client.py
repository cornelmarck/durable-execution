from __future__ import annotations

from dataclasses import asdict, fields
from typing import Any

import httpx

from .types import (
    ClaimTasksRequest,
    ClaimTasksResponse,
    ClaimedTask,
    CleanupPolicy,
    CompleteRunRequest,
    CompleteRunResponse,
    CreateQueueRequest,
    CreateQueueResponse,
    CreateWorkflowRunRequest,
    CreateWorkflowRunResponse,
    EmitEventRequest,
    EmitEventResponse,
    ErrorResponse,
    FailRunRequest,
    FailRunResponse,
    GetCheckpointResponse,
    ListTasksResponse,
    RetryStrategy,
    RetryStrategyKind,
    ScheduleRunRequest,
    ScheduleRunResponse,
    SetCheckpointRequest,
    SetCheckpointResponse,
    SpawnTaskRequest,
    SpawnTaskResponse,
    TaskSummary,
    UpdateWorkflowRunRequest,
    UpdateWorkflowRunResponse,
    WaitForEventRequest,
    WaitForEventResponse,
)


class DurableError(Exception):
    """Raised when the server responds with a non-2xx status."""

    def __init__(self, status_code: int, body: ErrorResponse) -> None:
        self.status_code = status_code
        self.body = body
        if body.error:
            msg = f"{status_code}: {body.error}"
        else:
            msg = f"unexpected status {status_code}"
        super().__init__(msg)


def _strip_none(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively remove keys whose value is None."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, dict):
            out[k] = _strip_none(v)
        else:
            out[k] = v
    return out


def _to_json(obj: Any) -> dict[str, Any]:
    return _strip_none(asdict(obj))


def _from_dict(cls: type, data: dict[str, Any]) -> Any:
    """Instantiate a dataclass from a dict, handling nested types."""
    if not data:
        return cls()

    field_map = {f.name: f for f in fields(cls)}
    kwargs: dict[str, Any] = {}
    for name, f in field_map.items():
        if name not in data:
            continue
        val = data[name]
        if val is None:
            kwargs[name] = None
            continue

        ft = f.type
        if ft in ("RetryStrategy | None", "RetryStrategy"):
            kwargs[name] = _from_dict(RetryStrategy, val)
        elif ft in ("CleanupPolicy | None", "CleanupPolicy"):
            kwargs[name] = _from_dict(CleanupPolicy, val)
        elif ft == "list[ClaimedTask]":
            kwargs[name] = [_from_dict(ClaimedTask, t) for t in val]
        elif ft == "list[TaskSummary]":
            kwargs[name] = [_from_dict(TaskSummary, t) for t in val]
        elif name == "kind" and cls is RetryStrategy:
            kwargs[name] = RetryStrategyKind(val)
        elif name == "status":
            # Resolve the enum from the string representation
            kwargs[name] = val
        else:
            kwargs[name] = val
    return cls(**kwargs)


class DurableClient:
    """Synchronous HTTP client for the durable-execution API."""

    def __init__(self, base_url: str = "http://localhost:8080") -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(base_url=self.base_url)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> DurableClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        resp = self._http.request(method, path, json=json, params=params)
        if resp.status_code >= 400:
            body = ErrorResponse()
            try:
                d = resp.json()
                body = ErrorResponse(error=d.get("error", ""), code=d.get("code", ""))
            except Exception:
                pass
            raise DurableError(resp.status_code, body)
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    # ------------------------------------------------------------------
    # Queues
    # ------------------------------------------------------------------

    def create_queue(self, req: CreateQueueRequest) -> CreateQueueResponse:
        data = self._request("POST", "/api/v1/queues", json=_to_json(req))
        return _from_dict(CreateQueueResponse, data)

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    def spawn_task(self, queue: str, req: SpawnTaskRequest) -> SpawnTaskResponse:
        data = self._request("POST", f"/api/v1/queues/{queue}/tasks", json=_to_json(req))
        return _from_dict(SpawnTaskResponse, data)

    def claim_tasks(self, queue: str, req: ClaimTasksRequest) -> ClaimTasksResponse:
        data = self._request("POST", f"/api/v1/queues/{queue}/tasks/claim", json=_to_json(req))
        return _from_dict(ClaimTasksResponse, data)

    def list_tasks(
        self,
        *,
        queue_name: str | None = None,
        status: str | None = None,
        task_name: str | None = None,
        cursor: str | None = None,
        limit: int = 0,
    ) -> ListTasksResponse:
        params: dict[str, str] = {}
        if queue_name is not None:
            params["queue_name"] = queue_name
        if status is not None:
            params["status"] = status
        if task_name is not None:
            params["task_name"] = task_name
        if cursor is not None:
            params["cursor"] = cursor
        if limit > 0:
            params["limit"] = str(limit)
        data = self._request("GET", "/api/v1/tasks", params=params or None)
        return _from_dict(ListTasksResponse, data)

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def complete_run(self, run_id: str, req: CompleteRunRequest) -> CompleteRunResponse:
        data = self._request("POST", f"/api/v1/runs/{run_id}/complete", json=_to_json(req))
        return _from_dict(CompleteRunResponse, data)

    def fail_run(self, run_id: str, req: FailRunRequest) -> FailRunResponse:
        data = self._request("POST", f"/api/v1/runs/{run_id}/fail", json=_to_json(req))
        return _from_dict(FailRunResponse, data)

    def schedule_run(self, run_id: str, req: ScheduleRunRequest) -> ScheduleRunResponse:
        data = self._request("POST", f"/api/v1/runs/{run_id}/schedule", json=_to_json(req))
        return _from_dict(ScheduleRunResponse, data)

    def wait_for_event(self, run_id: str, req: WaitForEventRequest) -> WaitForEventResponse:
        data = self._request("POST", f"/api/v1/runs/{run_id}/wait-for-event", json=_to_json(req))
        return _from_dict(WaitForEventResponse, data)

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def set_checkpoint(
        self, task_id: str, step: str, req: SetCheckpointRequest
    ) -> SetCheckpointResponse:
        data = self._request(
            "PUT", f"/api/v1/tasks/{task_id}/checkpoints/{step}", json=_to_json(req)
        )
        return _from_dict(SetCheckpointResponse, data)

    def get_checkpoint(self, task_id: str, step: str) -> GetCheckpointResponse:
        data = self._request("GET", f"/api/v1/tasks/{task_id}/checkpoints/{step}")
        return _from_dict(GetCheckpointResponse, data)

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def emit_event(self, req: EmitEventRequest) -> EmitEventResponse:
        data = self._request("POST", "/api/v1/events", json=_to_json(req))
        return _from_dict(EmitEventResponse, data)

    # ------------------------------------------------------------------
    # Workflow Runs
    # ------------------------------------------------------------------

    def create_workflow_run(
        self, req: CreateWorkflowRunRequest
    ) -> CreateWorkflowRunResponse:
        data = self._request("POST", "/api/v1/workflow-runs", json=_to_json(req))
        return _from_dict(CreateWorkflowRunResponse, data)

    def update_workflow_run(
        self, workflow_run_id: str, req: UpdateWorkflowRunRequest
    ) -> UpdateWorkflowRunResponse:
        data = self._request(
            "PATCH", f"/api/v1/workflow-runs/{workflow_run_id}", json=_to_json(req)
        )
        return _from_dict(UpdateWorkflowRunResponse, data)
