import { useState, type FormEvent } from "react";
import { useSearchParams } from "react-router";
import type { TaskStatus } from "../api/client";
import { useQueues, useTasks, type TaskFilters } from "../api/hooks";
import { TasksTable } from "../components/TasksTable";
import { Card, Empty, ErrorState, Loading } from "../components/ui";

const statuses: (TaskStatus | undefined)[] = [undefined, "pending", "completed", "failed", "canceled"];

// Filters live in the URL, so a filtered view can be bookmarked or shared.
export function Tasks() {
  const [params, setParams] = useSearchParams();
  const filters: TaskFilters = {
    queue_name: params.get("queue") || undefined,
    status: (params.get("status") as TaskStatus | null) || undefined,
    task_name: params.get("name") || undefined,
  };
  const [nameDraft, setNameDraft] = useState(filters.task_name ?? "");
  const queues = useQueues();
  const tasks = useTasks(filters);

  const setParam = (key: string, value?: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    setParams(next, { replace: true });
  };

  const applyName = (e: FormEvent) => {
    e.preventDefault();
    setParam("name", nameDraft.trim() || undefined);
  };

  const rows = tasks.data?.pages.flatMap((p) => p.tasks) ?? [];

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Tasks</h1>
          <p>Every task across queues, newest first. Open one to see its attempts.</p>
        </div>
      </div>

      <div className="card card-body" style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        <form className="filters" onSubmit={applyName}>
          <label className="field" style={{ flex: "2 1 220px" }}>
            Task name (exact)
            <input value={nameDraft} onChange={(e) => setNameDraft(e.target.value)} placeholder="e.g. charge-invoice" />
          </label>
          <label className="field" style={{ flex: "1 1 160px" }}>
            Queue
            <select value={filters.queue_name ?? ""} onChange={(e) => setParam("queue", e.target.value || undefined)}>
              <option value="">All queues</option>
              {queues.data?.map((q) => (
                <option key={q.name} value={q.name}>
                  {q.name}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" type="submit">
            Apply
          </button>
        </form>
        <div className="chips" role="group" aria-label="Status">
          {statuses.map((s) => (
            <button
              key={s ?? "all"}
              type="button"
              className="chip"
              aria-pressed={filters.status === s}
              onClick={() => setParam("status", s)}
            >
              {s ? s[0]!.toUpperCase() + s.slice(1) : "All"}
            </button>
          ))}
        </div>
      </div>

      <Card>
        {tasks.isPending ? (
          <Loading />
        ) : tasks.isError ? (
          <ErrorState error={tasks.error} />
        ) : rows.length === 0 ? (
          <Empty>No tasks match these filters.</Empty>
        ) : (
          <>
            <TasksTable tasks={rows} />
            <div className="card-head" style={{ paddingBottom: 14 }}>
              <span className="muted" style={{ fontSize: 13 }}>
                Showing {rows.length}
              </span>
              {tasks.hasNextPage && (
                <button className="btn" type="button" onClick={() => tasks.fetchNextPage()} disabled={tasks.isFetchingNextPage}>
                  {tasks.isFetchingNextPage ? "Loading…" : "Load more"}
                </button>
              )}
            </div>
          </>
        )}
      </Card>
    </>
  );
}
