"""Event-driven approval workflow.

A task does some initial work, then sleeps until an external "approval"
event arrives. A separate process (or a human via the CLI) emits the event,
which wakes the task so it can finish.

Events are scoped to a task by including the task ID in the event name,
so approving one task doesn't accidentally wake another.

Terminal 1 — start the worker:
    uv run python event.py work

Terminal 2 — create a task and watch it go to sleep:
    uv run python event.py create

Terminal 3 — approve it by task ID (the worker wakes up and completes):
    uv run python event.py approve <task_id>
"""

import signal
import sys

from durable import DurableClient, DurableError
from durable.types import (
    ClaimTasksRequest,
    CompleteRunRequest,
    CreateQueueRequest,
    CreateTaskRequest,
    EmitEventRequest,
    FailRunRequest,
    WaitForEventRequest,
)

QUEUE = "approvals"


def event_name(task_id: str) -> str:
    return f"approved:{task_id}"


def ensure_queue(client: DurableClient) -> None:
    try:
        client.create_queue(CreateQueueRequest(name=QUEUE))
    except DurableError as e:
        if e.status_code != 409:
            raise


def process_task(client: DurableClient, task) -> None:
    print(f"\ntask={task.task_id} run={task.run_id} attempt={task.attempt}")

    # Step 1: do some initial work
    print("  processing order...")

    # Step 2: wait for a task-specific approval event (timeout after 5 minutes)
    evt = event_name(task.task_id)
    print(f"  waiting for event '{evt}'...")
    resp = client.wait_for_event(
        task.run_id,
        WaitForEventRequest(
            event_name=evt,
            timeout_seconds=300,
            task_id=task.task_id,
            step_name="await_approval",
        ),
    )

    if resp.status == "resolved":
        # Event already existed — continue immediately
        print(f"  approved with payload: {resp.payload}")
    elif resp.status == "sleeping":
        # Run is now sleeping — stop processing, the server will
        # re-queue this run when the event arrives
        print(f"  sleeping — {resp.message}")
        return
    elif resp.status == "timeout":
        print("  timed out waiting for approval")
        client.fail_run(task.run_id, FailRunRequest(error="approval timeout"))
        return

    # Step 3: finish the work after approval
    print("  finalizing order...")
    client.complete_run(task.run_id, CompleteRunRequest(result={"approved": True}))
    print("  done!")


def create(client: DurableClient) -> None:
    ensure_queue(client)
    resp = client.create_task(QUEUE, CreateTaskRequest(task_name="process_order"))
    print(f"created task={resp.task_id} run={resp.run_id}")
    print(f"approve with: python event.py approve {resp.task_id}")


def approve(client: DurableClient, task_id: str) -> None:
    evt = event_name(task_id)
    resp = client.emit_event(
        EmitEventRequest(
            event_name=evt,
            payload={"approved_by": "admin"},
        )
    )
    print(f"emitted event '{evt}' id={resp.event_id}")


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
            ClaimTasksRequest(limit=1, claim_timeout=60, long_poll_seconds=30),
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

    if len(sys.argv) < 2 or sys.argv[1] not in ("create", "work", "approve"):
        print("Usage: python event.py <create|work|approve> [task_id]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "create":
        create(client)
    elif cmd == "approve":
        if len(sys.argv) < 3:
            print("Usage: python event.py approve <task_id>")
            sys.exit(1)
        approve(client, sys.argv[2])
    else:
        work(client)
