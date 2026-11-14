import { Link, useLocation, useParams } from "react-router";
import type { Run, Task } from "../api/client";
import { useTaskRuns } from "../api/hooks";
import { Card, Empty, ErrorState, Loading, NeedsEndpoint, StatusPill } from "../components/ui";
import { between, formatDateTime, timeAgo } from "../lib/format";

export function TaskDetail() {
  const id = useParams().id ?? "";
  // Set when arriving from a task list; absent on a reload or a pasted link,
  // until the API has GET /tasks/{id}.
  const task = (useLocation().state as { task?: Task } | null)?.task;
  const runs = useTaskRuns(id);
  const latest = runs.data?.[0];

  return (
    <>
      <div className="crumbs">
        <Link to="/tasks">Tasks</Link> / {task?.task_name ?? "Task"}
      </div>

      <div className="page-head">
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 12 }}>
            <h1>{task?.task_name ?? "Task"}</h1>
            {task && <StatusPill status={task.status} />}
            {latest && (
              <StatusPill
                status={latest.status}
                label={`run ${latest.status} · attempt ${latest.attempt}${task ? ` of ${task.max_attempts}` : ""}`}
              />
            )}
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 16, fontSize: 13 }} className="muted">
            <span className="mono">{id}</span>
            {task && (
              <span>
                queue <Link to={`/queues/${encodeURIComponent(task.queue_name)}`}>{task.queue_name}</Link>
              </span>
            )}
            {task && <span title={formatDateTime(task.created_at)}>created {timeAgo(task.created_at)}</span>}
          </div>
        </div>
      </div>

      <div className="row">
        <div style={{ flex: "2 1 520px", minWidth: 0, display: "flex", flexDirection: "column", gap: 20 }}>
          <Card title="Attempts" aside={runs.data ? `${runs.data.length} run${runs.data.length === 1 ? "" : "s"}` : undefined}>
            <div className="card-body">
              {runs.isPending ? (
                <Loading />
              ) : runs.isError ? (
                <ErrorState error={runs.error} />
              ) : runs.data.length === 0 ? (
                <Empty>No runs found for this task ID.</Empty>
              ) : (
                <div className="timeline">
                  {runs.data.map((run, i) => (
                    <Attempt key={run.id} run={run} last={i === runs.data.length - 1} />
                  ))}
                </div>
              )}
            </div>
          </Card>
          <NeedsEndpoint title="Checkpoints" endpoint="GET /tasks/{task_id}/checkpoints">
            Today a checkpoint can only be read by step name, and the step names aren’t listed anywhere.
          </NeedsEndpoint>
        </div>

        <aside style={{ flex: "1 1 280px", minWidth: 0, display: "flex", flexDirection: "column", gap: 20 }}>
          {task ? (
            <Card title="Task">
              <div className="card-body" style={{ paddingTop: 4 }}>
                <dl className="props">
                  <dt>Status</dt>
                  <dd>{task.status}</dd>
                  <dt>Max attempts</dt>
                  <dd>
                    {task.max_attempts}
                    {runs.data ? ` (${runs.data.length} used)` : ""}
                  </dd>
                  <dt>Created</dt>
                  <dd>{formatDateTime(task.created_at)}</dd>
                  {task.completed_at && (
                    <>
                      <dt>Finished</dt>
                      <dd>
                        {formatDateTime(task.completed_at)} · took {between(task.created_at, task.completed_at)}
                      </dd>
                    </>
                  )}
                </dl>
              </div>
            </Card>
          ) : (
            <NeedsEndpoint title="Task summary" endpoint="GET /tasks/{task_id}">
              Open the task from a list to see its name, queue and status.
            </NeedsEndpoint>
          )}
          <NeedsEndpoint title="Params, headers, retry strategy and timeouts" endpoint="GET /tasks/{task_id}" />
        </aside>
      </div>
    </>
  );
}

function Attempt({ run, last }: { run: Run; last: boolean }) {
  const duration = between(run.created_at, run.completed_at);
  return (
    <div className="attempt">
      <div className="attempt-rail" aria-hidden="true">
        <span className="dot" style={{ background: `var(--st-${run.status}-fg)` }} />
        {!last && <span className="line" />}
      </div>
      <div className="attempt-body" style={last ? { paddingBottom: 0 } : undefined}>
        <div className="attempt-head">
          <strong>Run #{run.attempt}</strong>
          <StatusPill status={run.status} />
          <span className="mono muted">{run.id}</span>
        </div>
        <div className="attempt-meta">
          <span title={formatDateTime(run.created_at)}>created {timeAgo(run.created_at)}</span>
          {run.completed_at && <span title={formatDateTime(run.completed_at)}>finished {timeAgo(run.completed_at)}</span>}
          {duration && <span>took {duration} (incl. queue time)</span>}
        </div>
        {run.error && <pre className="error-block">{run.error}</pre>}
      </div>
    </div>
  );
}
