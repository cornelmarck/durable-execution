import createClient from "openapi-fetch";
import type { components, paths } from "./schema";

// Relative base URL: in dev Vite proxies /api to the Go server; in production the
// console and API are served from the same origin, so no CORS is needed.
export const api = createClient<paths>({ baseUrl: "/api/v1" });

export type Queue = NonNullable<
  paths["/queues"]["get"]["responses"]["200"]["content"]["application/json"]["queues"]
>[number];
export type QueueStats =
  paths["/queues/{queue_name}/stats"]["get"]["responses"]["200"]["content"]["application/json"];
export type Task = components["schemas"]["TaskSummary"];
export type TaskStatus = Task["status"];
export type Run = components["schemas"]["RunSummary"];
export type RunStatus = Run["status"];

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
  }
}

type Result<T> = { data?: T; error?: unknown; response: Response };

// Turns an openapi-fetch result into data-or-throw, so TanStack Query sees failures.
export function unwrap<T>({ data, error, response }: Result<T>): T {
  if (error !== undefined || data === undefined) {
    const body = (error ?? {}) as { error?: string; code?: string };
    throw new ApiError(body.error ?? `${response.status} ${response.statusText}`, response.status, body.code);
  }
  return data;
}
