"""Checkpointed steps with durable execution.

Each step is checkpointed after completion. If the worker crashes or the
run is retried, already-completed steps are skipped.

Usage:
    uv run python checkpointed_agent.py spawn
    uv run python checkpointed_agent.py work
"""

import signal
import sys
import time

from durable import DurableClient, DurableError
from durable.types import (
    ClaimTasksRequest,
    CompleteRunRequest,
    CreateQueueRequest,
    FailRunRequest,
    SetCheckpointRequest,
    SpawnTaskRequest,
)

QUEUE = "demo"
STEPS = ["step1", "step2", "step3"]


def execute_step(name: str) -> str:
    print(f"  executing {name}...")
    time.sleep(2)
    return f"{name}_result"


def process_task(client: DurableClient, task) -> None:
    print(f"\ntask={task.task_id} run={task.run_id} attempt={task.attempt}")

    state: dict = {}
    completed: list[str] = []

    # Resume from checkpoint if one exists
    try:
        cp = client.get_checkpoint(task.task_id, "progress")
        state = cp.state
        completed = state.get("completed", [])
        print(f"  resumed: {completed}")
    except DurableError as e:
        if e.status_code != 404:
            raise

    for step in STEPS:
        if step in completed:
            print(f"  skipping {step} (already done)")
            continue

        state[step] = execute_step(step)
        completed.append(step)
        state["completed"] = completed

        client.set_checkpoint(
            task.task_id,
            "progress",
            SetCheckpointRequest(state=state, owner_run=task.run_id),
        )

    client.complete_run(task.run_id, CompleteRunRequest(result=state))
    print(f"  done: {state}")


def ensure_queue(client: DurableClient) -> None:
    try:
        client.create_queue(CreateQueueRequest(name=QUEUE))
    except DurableError as e:
        if e.status_code != 409:
            raise


def spawn(client: DurableClient) -> None:
    ensure_queue(client)
    resp = client.spawn_task(QUEUE, SpawnTaskRequest(task_name="demo", max_attempts=3))
    print(f"spawned task={resp.task_id} run={resp.run_id}")


def work(client: DurableClient) -> None:
    ensure_queue(client)
    print(f"polling {QUEUE}...")
    current_run_id: str | None = None

    def on_interrupt(sig, frame):
        if current_run_id:
            print(f"\n  interrupted — failing run {current_run_id}")
            try:
                client.fail_run(current_run_id, FailRunRequest(error="worker interrupted"))
            except DurableError:
                pass
        sys.exit(1)

    signal.signal(signal.SIGINT, on_interrupt)

    while True:
        resp = client.claim_tasks(
            QUEUE,
            ClaimTasksRequest(limit=1, claim_timeout=30, long_poll_seconds=30),
        )
        if not resp.tasks:
            continue
        task = resp.tasks[0]
        current_run_id = task.run_id
        try:
            process_task(client, task)
        except DurableError as e:
            print(f"  error: {e}")
            try:
                client.fail_run(task.run_id, FailRunRequest(error=str(e)))
            except DurableError:
                pass
        current_run_id = None


if __name__ == "__main__":
    client = DurableClient()

    if len(sys.argv) < 2 or sys.argv[1] not in ("spawn", "work"):
        print("Usage: python checkpointed_agent.py <spawn|work>")
        sys.exit(1)

    if sys.argv[1] == "spawn":
        spawn(client)
    else:
        work(client)
