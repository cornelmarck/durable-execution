import { useInfiniteQuery, useQueries, useQuery } from "@tanstack/react-query";
import { api, unwrap, type QueueStats, type TaskStatus } from "./client";

export function useQueues() {
  return useQuery({
    queryKey: ["queues"],
    queryFn: async () => unwrap(await api.GET("/queues")).queues,
  });
}

function statsQuery(queueName: string) {
  return {
    queryKey: ["queues", queueName, "stats"],
    queryFn: async (): Promise<QueueStats> =>
      unwrap(await api.GET("/queues/{queue_name}/stats", { params: { path: { queue_name: queueName } } })),
  };
}

export function useQueueStats(queueName: string) {
  return useQuery(statsQuery(queueName));
}

// Stats for many queues at once, keyed by queue name.
export function useAllQueueStats(queueNames: string[]) {
  return useQueries({
    queries: queueNames.map(statsQuery),
    combine: (results) => ({
      byName: Object.fromEntries(
        results.flatMap((r, i) => (r.data ? [[queueNames[i] as string, r.data]] : [])),
      ) as Record<string, QueueStats>,
      isPending: results.some((r) => r.isPending),
      error: results.find((r) => r.error)?.error ?? null,
    }),
  });
}

export type TaskFilters = {
  queue_name?: string;
  status?: TaskStatus;
  task_name?: string;
};

export function useTasks(filters: TaskFilters, pageSize = 50) {
  return useInfiniteQuery({
    queryKey: ["tasks", filters, pageSize],
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) =>
      unwrap(await api.GET("/tasks", { params: { query: { ...filters, cursor: pageParam, limit: pageSize } } })),
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
}

// All runs (attempts) for one task, newest attempt first.
export function useTaskRuns(taskId: string) {
  return useQuery({
    queryKey: ["runs", { taskId }],
    queryFn: async () => {
      const res = unwrap(await api.GET("/runs", { params: { query: { task_id: taskId, limit: 200 } } }));
      return [...res.runs].sort((a, b) => b.attempt - a.attempt);
    },
  });
}
