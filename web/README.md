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

## Bazel

The console builds with the rest of the repo (`rules_js`, npm packages from `pnpm-lock.yaml`):

```bash
bazel build //web:dist        # static site in bazel-bin/web/dist
bazel test //web:all          # type-check, and check src/api/schema.d.ts matches the spec
bazel run //web:schema        # regenerate src/api/schema.d.ts after changing api/openapi.yaml
bazel run //web:dev           # dev server, same as `pnpm dev`
bazel run //web:image_load    # load the durable-console:dev image into Docker
```

After adding or upgrading a package, run `pnpm install` here so `pnpm-lock.yaml` changes; Bazel reads the lockfile.

## Image

`//web:image` is nginx (unprivileged, port 8080) serving the built files, with no runtime configuration:

- It doesn't proxy the API. Route `/api` to the API server in front of it (e.g. the Ingress), so the browser sees one origin and needs no CORS. `/api/*` requests that reach the console get a 404.
- Hashed `/assets/*` are cached forever; `index.html` is never cached; unknown paths fall back to `index.html` for client-side routes.
- Sends a strict Content-Security-Policy; `/healthz` returns 200 for probes.

For local development use `pnpm dev` or `bazel run //web:dev`, which proxy `/api` to `localhost:8080`. `//web:image_push` is picked up by the "Push images" workflow like the other images.

## Not built yet

These screens or panels wait for read endpoints the API doesn't have yet, and show a "needs endpoint" placeholder:

- Workflow runs (`GET /workflow-runs`, `GET /workflow-runs/{id}`)
- Events (`GET /events`, sleeping runs by event name)
- Checkpoints on the task page (`GET /tasks/{id}/checkpoints`)
- Task params, headers, retry strategy and timeouts (`GET /tasks/{id}`)
- Sleeping, failed and in-flight counts per queue (more fields on `/queues/{name}/stats`)
