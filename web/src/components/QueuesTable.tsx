import { Link, useNavigate } from "react-router";
import type { Queue, QueueStats } from "../api/client";
import { formatDuration, formatNumber, formatTtl } from "../lib/format";

// Consumer lag above this is highlighted.
export const LAG_WARN_SECONDS = 60;

export function lagOf(stats?: QueueStats): number | undefined {
  return stats?.oldest_pending_run_age_seconds ?? undefined;
}

export function QueuesTable({ queues, stats }: { queues: Queue[]; stats: Record<string, QueueStats> }) {
  const navigate = useNavigate();
  const maxLag = Math.max(LAG_WARN_SECONDS, ...queues.map((q) => lagOf(stats[q.name]) ?? 0));

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Queue</th>
            <th>Pending</th>
            <th>Claimed</th>
            <th>Completed</th>
            <th>Oldest pending (lag)</th>
            <th>Task TTL</th>
            <th>Event TTL</th>
          </tr>
        </thead>
        <tbody>
          {queues.map((q) => {
            const s = stats[q.name];
            const lag = lagOf(s);
            const warn = lag !== undefined && lag > LAG_WARN_SECONDS;
            return (
              <tr key={q.name} className="clickable" onClick={() => navigate(`/queues/${encodeURIComponent(q.name)}`)}>
                <td>
                  <Link to={`/queues/${encodeURIComponent(q.name)}`} style={{ fontWeight: 600 }} onClick={(e) => e.stopPropagation()}>
                    {q.name}
                  </Link>
                </td>
                <td>{s ? formatNumber(s.pending_runs) : "…"}</td>
                <td>{s ? formatNumber(s.claimed_runs) : "…"}</td>
                <td>{s ? formatNumber(s.completed_runs) : "…"}</td>
                <td>
                  <div className="lag">
                    <div className="lag-bar" aria-hidden="true">
                      <div
                        style={{
                          width: `${lag ? Math.max(2, (lag / maxLag) * 100) : 0}%`,
                          background: warn ? "var(--warn)" : "var(--accent)",
                        }}
                      />
                    </div>
                    <span style={warn ? { color: "var(--warn)", fontWeight: 600 } : undefined}>
                      {lag === undefined ? "—" : formatDuration(lag)}
                    </span>
                  </div>
                </td>
                <td className="muted">{formatTtl(q.cleanup?.task_ttl_seconds)}</td>
                <td className="muted">{formatTtl(q.cleanup?.event_ttl_seconds)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
