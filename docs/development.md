# Developing JobFlow

## Backend

Run these commands from the repository root with Go 1.24 or later.

```sh
make fmt
make build
make vet
make test
make race
# Uses a real PostgreSQL instance; every test creates and drops its own random schema:
TEST_DATABASE_URL='postgres://jobflow:jobflow@localhost:5432/jobflow?sslmode=disable' make integration
```

The integration database role needs schema-creation permission. Tests never truncate existing tables; cleanup drops only their own generated schemas. Run `docker compose up -d postgres` to provide the development database. Ordinary unit tests do not require PostgreSQL; integration tests are build-tagged and fail with instructions if `TEST_DATABASE_URL` is absent.

SQL query sources live in [queries/jobs.sql](../queries/jobs.sql). The generated pgx bindings live in `internal/store/db/`. After changing schema or queries, run `make generate` (pinned sqlc v1.30.0; first use downloads the tool). Run migrations explicitly with `docker compose run --rm migrate`. Startup does not silently migrate from application processes.

For host processes, set `DATABASE_URL` and optionally `API_ADDR`, `WORKER_CONCURRENCY`, `WORKER_POLL_INTERVAL`, `WORKER_SHUTDOWN_TIMEOUT`, and `WORKER_LEASE_DURATION`, then use `go run ./cmd/api` and `go run ./cmd/worker` in separate terminals. Start more worker processes in additional terminals; each receives a random process-lifetime worker ID. The `.env` file is consumed by Compose, not automatically loaded by Go.

## Dashboard

For UI development, use Node.js 22.13 or newer in the Node.js 22 series and keep the Go API and workers running. Run `npm ci` in `web/`, then start the development server with `JOBFLOW_API_URL=http://127.0.0.1:8080 npm run dev`. Run checks in a separate terminal from `web/`:

```sh
npm run lint
npm run typecheck
npm run build
npx playwright install chromium --only-shell
npm test
# Run against an isolated live stack; the browser test creates job history.
JOBFLOW_E2E_URL=http://127.0.0.1:5173 npm test -- tests/live.spec.ts
```

## Python SDK

From the repository root, install the SDK into a virtual environment and run its tests.

```sh
python -m pip install -e sdk/python
python -m unittest discover -s sdk/python/tests -v
```

See the [CI workflow](../.github/workflows/ci.yml) for automated checks and the [benchmark guide](benchmarks.md) for the Compose crash scenario and load tests.
