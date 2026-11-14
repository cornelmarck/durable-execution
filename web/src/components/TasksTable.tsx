import { Link, useNavigate } from "react-router";
import type { Task } from "../api/client";
import { between, formatDateTime, shortId, timeAgo } from "../lib/format";
import { StatusPill } from "./ui";

export function taskPath(task: Task) {
  return `/tasks/${task.id}`;
}

// Rows pass the task summary along as router state, because the API has no
// GET /tasks/{id} yet; the detail page uses it for its header.
export function TasksTable({ tasks, showQueue = true }: { tasks: Task[]; showQueue?: boolean }) {
  const navigate = useNavigate();
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Task</th>
            <th>Task ID</th>
            {showQueue && <th>Queue</th>}
            <th>Status</th>
            <th>Max attempts</th>
            <th>Created</th>
            <th>Duration</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((t) => (
            <tr key={t.id} className="clickable" onClick={() => navigate(taskPath(t), { state: { task: t } })}>
              <td>
                <Link to={taskPath(t)} state={{ task: t }} style={{ fontWeight: 600 }} onClick={(e) => e.stopPropagation()}>
                  {t.task_name}
                </Link>
              </td>
              <td className="mono muted" title={t.id}>
                {shortId(t.id)}
              </td>
              {showQueue && <td>{t.queue_name}</td>}
              <td>
                <StatusPill status={t.status} />
              </td>
              <td>{t.max_attempts}</td>
              <td className="muted" title={formatDateTime(t.created_at)}>
                {timeAgo(t.created_at)}
              </td>
              <td className="muted">{between(t.created_at, t.completed_at) ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
