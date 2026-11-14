import { Link, useParams } from "react-router";
import { ApiError } from "../api/client";
import { useQueues, useQueueStats, useTasks } from "../api/hooks";
import { LAG_WARN_SECONDS } from "../components/QueuesTable";
import { TasksTable } from "../components/TasksTable";
import { Card, Empty, ErrorState, Loading, NeedsEndpoint, StatTile } from "../components/ui";
import { formatDateTime, formatDuration, formatNumber, formatTtl } from "../lib/format";

export function QueueDetail() {
  const name = useParams().name ?? "";
  const stats = useQueueStats(name);
  const queue = useQueues().data?.find((q) => q.name === name);
  const tasks = useTasks({ queue_name: name }, 20);

  if (stats.error instanceof ApiError && stats.error.status === 404) {
    return (
      <>
        <div className="crumbs">
          <Link to="/queues">Queues</Link> / {name}
        </div>
        <Empty>Queue “{name}” doesn’t exist.</Empty>
      </>
    );
  }

  const s = stats.data;
  const lag = s?.oldest_pending_run_age_seconds ?? undefined;

  return (
    <>
      <div className="crumbs">
        <Link to="/queues">Queues</Link> / {name}
      </div>
      <div className="page-head">
        <div>
          <h1>{name}</h1>
          <p>{queue ? `Created ${formatDateTime(queue.created_at)}` : " "}</p>
        </div>
        <Link className="btn" to={`/tasks?queue=${encodeURIComponent(name)}`}>
          Browse tasks
        </Link>
      </div>

      {stats.isError ? (
        <Card>
          <ErrorState error={stats.error} />
        </Card>
      ) : (
        <div className="tiles">
          <StatTile label="Pending runs" value={s ? formatNumber(s.pending_runs) : "…"} />
          <StatTile label="Claimed runs" value={s ? formatNumber(s.claimed_runs) : "…"} />
          <StatTile label="Completed runs" value={s ? formatNumber(s.completed_runs) : "…"} />
          <StatTile
            label="Oldest pending"
            value={!s ? "…" : lag === undefined ? "—" : formatDuration(lag)}
            sub="consumer lag"
            tone={lag !== undefined && lag > LAG_WARN_SECONDS ? "warn" : undefined}
          />
        </div>
      )}

      <div className="row">
        <div style={{ flex: "2 1 520px", minWidth: 0 }}>
          <Card title="Recent tasks" aside={<Link to={`/tasks?queue=${encodeURIComponent(name)}`}>All tasks →</Link>}>
            {tasks.isPending ? (
              <Loading />
            ) : tasks.isError ? (
              <ErrorState error={tasks.error} />
            ) : tasks.data.pages[0]?.tasks.length ? (
              <TasksTable tasks={tasks.data.pages[0].tasks} showQueue={false} />
            ) : (
              <Empty>No tasks in this queue.</Empty>
            )}
          </Card>
        </div>
        <div style={{ flex: "1 1 260px", minWidth: 0, display: "flex", flexDirection: "column", gap: 20 }}>
          <Card title="Settings">
            <div className="card-body" style={{ paddingTop: 4 }}>
              <dl className="props">
                <dt>Task TTL</dt>
                <dd>{queue ? formatTtl(queue.cleanup?.task_ttl_seconds) : "…"}</dd>
                <dt>Event TTL</dt>
                <dd>{queue ? formatTtl(queue.cleanup?.event_ttl_seconds) : "…"}</dd>
              </dl>
            </div>
          </Card>
          <NeedsEndpoint title="Sleeping, failed and in-flight runs" endpoint="more fields on /queues/{name}/stats">
            Stats only count pending, claimed and completed runs today.
          </NeedsEndpoint>
        </div>
      </div>
    </>
  );
}
