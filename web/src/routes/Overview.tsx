import { Link } from "react-router";
import { useAllQueueStats, useQueues, useTasks } from "../api/hooks";
import { LAG_WARN_SECONDS, lagOf, QueuesTable } from "../components/QueuesTable";
import { TasksTable } from "../components/TasksTable";
import { Card, Empty, ErrorState, Loading, StatTile } from "../components/ui";
import { formatDuration, formatNumber } from "../lib/format";

export function Overview() {
  const queues = useQueues();
  const names = queues.data?.map((q) => q.name) ?? [];
  const stats = useAllQueueStats(names);
  const failures = useTasks({ status: "failed" }, 5);

  const all = Object.values(stats.byName);
  const sum = (pick: (s: (typeof all)[number]) => number) => all.reduce((n, s) => n + pick(s), 0);
  const worst = all.reduce<{ name: string; lag: number } | undefined>((acc, s) => {
    const lag = lagOf(s);
    return lag !== undefined && (!acc || lag > acc.lag) ? { name: s.queue_name, lag } : acc;
  }, undefined);
  const ready = queues.isSuccess && !stats.isPending;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Overview</h1>
          <p>All queues · refreshes every 5 seconds</p>
        </div>
      </div>

      <div className="tiles">
        <StatTile label="Pending runs" value={ready ? formatNumber(sum((s) => s.pending_runs)) : "…"} sub={`across ${names.length} queues`} />
        <StatTile label="Claimed runs" value={ready ? formatNumber(sum((s) => s.claimed_runs)) : "…"} sub="being worked on" />
        <StatTile label="Completed runs" value={ready ? formatNumber(sum((s) => s.completed_runs)) : "…"} sub="within retention" />
        <StatTile
          label="Max consumer lag"
          value={!ready ? "…" : worst ? formatDuration(worst.lag) : "—"}
          sub={worst ? worst.name : "nothing pending"}
          tone={worst && worst.lag > LAG_WARN_SECONDS ? "warn" : undefined}
        />
      </div>

      <Card title="Queues" aside={<Link to="/queues">All queues →</Link>}>
        {queues.isPending ? (
          <Loading />
        ) : queues.isError ? (
          <ErrorState error={queues.error} />
        ) : names.length === 0 ? (
          <Empty>No queues yet. Create one with <code>durablectl queues create --name demo</code>.</Empty>
        ) : (
          <QueuesTable queues={queues.data} stats={stats.byName} />
        )}
      </Card>

      <Card title="Recent failures" aside={<Link to="/tasks?status=failed">All failed tasks →</Link>}>
        {failures.isPending ? (
          <Loading />
        ) : failures.isError ? (
          <ErrorState error={failures.error} />
        ) : failures.data.pages[0]?.tasks.length ? (
          <TasksTable tasks={failures.data.pages[0].tasks} />
        ) : (
          <Empty>No failed tasks.</Empty>
        )}
      </Card>
    </>
  );
}
