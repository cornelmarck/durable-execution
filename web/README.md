# Durable Console

Read-only web UI for the durable execution API: queue health, task explorer and per-task attempt history.

Stack: React, TypeScript, Vite, TanStack Query (5s auto-refresh), React Router, and a typed client generated from `../api/openapi.yaml` with `openapi-typescript` + `openapi-fetch`.

## Develop

```bash
pnpm install
pnpm dev            # http://localhost:5173, proxies /api to localhost:8080
```

Point the proxy at another server, e.g. a `kubectl port-forward`:

```bash
DURABLE_API=http://localhost:18080 pnpm dev
```

After changing `api/openapi.yaml`, regenerate the client types:

```bash
pnpm gen:api
```

## Build

```bash
pnpm build          # type-checks, then writes static files to dist/
```

The app calls the API at the relative path `/api/v1`, so in production serve `dist/` from the same origin as the API (e.g. an Ingress routing `/api` to the server and everything else to the console), with a fallback to `index.html` for client-side routes.

## Not built yet

These screens or panels wait for read endpoints the API doesn't have yet, and show a "needs endpoint" placeholder:

- Workflow runs (`GET /workflow-runs`, `GET /workflow-runs/{id}`)
- Events (`GET /events`, sleeping runs by event name)
- Checkpoints on the task page (`GET /tasks/{id}/checkpoints`)
- Task params, headers, retry strategy and timeouts (`GET /tasks/{id}`)
- Sleeping, failed and in-flight counts per queue (more fields on `/queues/{name}/stats`)
