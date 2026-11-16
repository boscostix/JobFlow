# JobFlow

A fault-tolerant, horizontally scalable distributed job execution platform built in Go. JobFlow will let developers submit, schedule, execute, retry, and monitor asynchronous background tasks across multiple workers through a REST API and lightweight Python SDK.

**Status: Milestones 1 and 2 implemented.** The API and PostgreSQL queue support durable submission, multiple independent workers, bounded local concurrency, execution history, and result retrieval. Reliability and recovery remain future milestones. The complete MVP capabilities and performance targets below remain a roadmap, not achieved results.

## Run locally

Requirements: Docker with Compose. Go 1.24 or newer is needed for local Go commands. The container build pins Go 1.24.9. No host PostgreSQL client or migration tool is required.

```sh
cp .env.example .env
docker compose up --build -d
```

Compose waits for PostgreSQL, runs the versioned migrations as a separate one-shot service, then starts the API and three independent worker processes. Each worker defaults to four execution slots (12 total). PostgreSQL data lives in a persistent named volume. Edit `.env` before startup to change `POSTGRES_PORT` or `API_PORT` if their defaults (5432 and 8080) are in use. Credentials are for local development; use URL-safe credentials with this Compose configuration.

```sh
curl -sS http://localhost:8080/health
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22}}'
# Copy job_id from the response:
curl -sS http://localhost:8080/api/v1/jobs/JOB_ID
```

Submission returns `202 Accepted` with `job_id` and `status: QUEUED` after persistence. Retrieval returns `id`, `type`, `payload`, `status`, timestamps, and the result or `last_error`. The example reaches `COMPLETED` with `result: {"sum":42}`. `add_numbers` accepts exactly two signed 64-bit integers, `a` and `b`. Addition overflow is an execution failure and produces a persisted `FAILED` job.

The implemented lifecycle is `QUEUED -> RUNNING -> COMPLETED` or `FAILED`. Invalid JSON, unknown tasks/fields, case aliases, duplicate keys, invalid payloads, and unsupported options return `400`; bodies over 64 KiB return `413`; unknown job IDs return `404`; database errors return `503`. `/health` checks database connectivity and returns `503` when unavailable. `Idempotency-Key` is explicitly rejected until deduplication exists.

```sh
docker compose logs -f api worker
docker compose stop worker            # Stops all worker replicas; submissions remain queued
docker compose restart api           # Persisted jobs survive
docker compose start worker          # Starts all worker replicas to drain queued jobs
docker compose down                  # Keeps the database volume
```

**Current limitation:** a hard worker crash after claiming, an exceeded shutdown window, or a failed outcome write can leave a job `RUNNING`. There is no lease-based reclamation or automatic retry yet. An outcome-persistence failure stops new claims in that worker and drains its other in-flight jobs before returning an error; it never reruns the handler. Other worker processes continue normally. Graceful shutdown stops claiming and allows all concurrent in-flight work up to `WORKER_SHUTDOWN_TIMEOUT` (10 seconds by default). Keep Compose's `stop_grace_period` longer than that setting. Milestone 2 does not yet deliver the full MVP's at-least-once recovery guarantees.

### Development and checks

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

SQL queries live in `queries/jobs.sql`; generated pgx code is checked into `internal/store/db/`. After changing schema or queries, run `make generate` (pinned sqlc v1.30.0; first use downloads the tool). Run migrations explicitly with `docker compose run --rm migrate`. Startup does not silently migrate from application processes.

For host processes, set `DATABASE_URL` and optionally `API_ADDR`, `WORKER_CONCURRENCY`, `WORKER_POLL_INTERVAL`, and `WORKER_SHUTDOWN_TIMEOUT`, then use `go run ./cmd/api` and `go run ./cmd/worker` in separate terminals. Start more worker processes in additional terminals; each receives a random process-lifetime worker ID. The `.env` file is consumed by Compose, not automatically loaded by Go.

Real-PostgreSQL tests cover durable submission across API recreation, successful and failed execution, competing claims, skipping locked jobs, terminal state protection, rejection without insertion, claim-lock release during execution, bounded concurrency across independent worker pools, concurrent graceful shutdown, atomic attempt-write rollback, and migration backfill. Crash recovery, leases, fencing, retries, scheduling, idempotency, cancellation, the SDK, dashboard, metrics, and CI remain pending.

### Distributed workers and attempt history

`WORKER_CONCURRENCY` accepts integers from 1 through 256 (default 4). Each fixed goroutine slot claims one job and retains its slot through handler execution and outcome persistence; there is no local prefetch queue. Task handlers and their registry must be safe for concurrent use. Database pool capacity can constrain claim/write throughput independently of the execution limit. Claims use the existing `FOR UPDATE SKIP LOCKED` statement inside a short transaction that also inserts a `job_attempts` row. Task execution holds no claim transaction open. Completion or failure updates both the job and its attempt in one transaction.

Migration 2 adds `job_attempts` with job ID, attempt number, worker ID, state, timestamps, result, and error. There is one attempt (number 1) per claimed job at this milestone; retries remain deferred. Existing nonqueued jobs are backfilled with worker ID `legacy-unknown`. Stop existing API/worker processes before upgrading, run migrations, then restart the new binaries. Old workers cannot maintain attempt history. Attempt history is currently inspected through SQL; the attempts REST endpoint remains planned.

```sh
# Three workers by default; override replica count or local slots:
WORKER_CONCURRENCY=2 docker compose up -d --scale worker=5
# Inspect per-process execution history:
docker compose exec postgres psql -U jobflow -d jobflow -c \
  'SELECT job_id, attempt_number, worker_id, status, started_at, completed_at, result, last_error FROM job_attempts ORDER BY started_at;'
# Race instrumentation for the PostgreSQL concurrency tests:
TEST_DATABASE_URL='postgres://jobflow:jobflow@localhost:5432/jobflow?sslmode=disable' \
  go test -race -tags=integration -count=1 ./tests/integration
```

Shutdown cancels claims, including claims waiting for a database connection, and waits for active slots to save outcomes. A claim committed immediately before cancellation may still execute during draining. The executable enforces the shutdown deadline for the whole process; Go handlers are cooperative and cannot be forcibly terminated individually. Without a shutdown signal, draining after an outcome-write error waits for the other handlers to return. Abrupt termination or an ambiguous database commit can strand a running job and attempt. This milestone provides neither automatic recovery nor exactly-once execution.

### Milestone 2 verification record

Verified November 15, 2026:

- `make generate`, `make fmt`, `make build`, `make vet`, `make test`, and `make race` passed.
- Real PostgreSQL 17.6 integration tests, also run under the race detector, passed. Three independent connection pools each reached their two-slot limit, processed 30 distinct jobs once in this controlled run, and persisted matching success/failure histories. A separate shutdown test drained three in-flight jobs and left three queued jobs untouched.
- One integration rerun timed out waiting for execution slots to fill. Ten targeted repetitions, the final complete integration suite, and its race-detector run then passed; the timeout was not reproduced.
- Independent read-only review found no confirmed P0–P2 defects; no fix pass was required.
- A separate Compose project used the Go 1.24.9 image, migration service, API, and three four-slot worker containers. With workers stopped, 300 submitted jobs stayed queued across API restart. After worker startup, all 300 reached their expected outcomes (270 completed, 30 overflow failures), with one matching attempt per job and execution distributed across all three worker IDs.

Reproduce the isolated Compose setup with `POSTGRES_PORT=55432 API_PORT=18080 docker compose -p jobflow-m2 up --build -d`. Use that project name and port overrides for subsequent stop/start/log/exec commands, and port 18080 for API submissions. `docker compose -p jobflow-m2 down` removes the verification containers while retaining their database volume. These are controlled correctness checks, not throughput benchmarks or proof of crash recovery.


### Milestone 1 verification record

Initial verification on November 14, 2026; review-fix regression checks on November 15, 2026:

- Go formatting, `go build ./...`, `go vet ./...`, `go test ./...`, and `go test -race ./...` passed.
- All four real-PostgreSQL integration tests passed against PostgreSQL 17.6, using isolated schemas.
- The Go 1.24.9 container build, migration service, API, and worker ran successfully through Docker Compose in a dedicated verification project.
- A job submitted while the worker was stopped remained queued across API restart, then completed with sum 42 after worker startup.
- Overflow produced a persisted failure; completed and failed outcomes survived API/worker restart.
- Invalid requests, unsupported retry options and idempotency keys, and missing IDs returned the documented errors. Case aliases and duplicate keys are rejected before persistence; PostgreSQL JSONB normalization cannot change accepted task inputs.
- Shutdown regression tests confirm that a blocked outcome write does not make pool cleanup exceed the configured grace deadline; normal graceful completion still closes the pool.
- A process-level check blocked a real PostgreSQL outcome write, sent `SIGTERM`, and observed worker exit in 1.00 seconds with a one-second shutdown window.
- Stopping PostgreSQL caused health and submission to return `503`; restoring it allowed reconnection with stored outcomes intact.

These are correctness checks for Milestone 1, not throughput benchmarks or proof of automatic crash recovery.

The primary objective is to keep accepted jobs durable, execute them reliably, and recover automatically from worker crashes, transient failures, and service restarts. Distributed coordination will be implemented directly using PostgreSQL transactions.

## Planned full MVP capabilities

- Durable job submission and storage in PostgreSQL.
- Distributed execution across independent workers with configurable local concurrency.
- Atomic job claiming, ownership leases, lease renewal, and automatic crash recovery.
- Fencing tokens that prevent stale attempts from overwriting newer job state.
- Configurable retries with exponential backoff and jitter, plus terminal dead-letter handling and manual retry.
- Immediate and scheduled execution, execution timeouts, and cancellation requests.
- Submission idempotency and stable job identifiers for handler-side deduplication.
- Execution histories, errors, results, worker health, structured logs, and operational metrics.
- An operator-focused React dashboard and a Python client for the REST API.
- Automated reliability tests, fault injection, and reproducible benchmarks.

### Execution guarantees

The full MVP will provide **at-least-once execution**. A task may run more than once after a crash or lost lease. Fencing protects JobFlow's stored state; task handlers must separately make external side effects safe to repeat.

Submission idempotency deduplicates repeated requests with the same key and equivalent payload. It does not by itself prevent duplicate external side effects during execution.

Timeouts use Go context cancellation for cooperative handlers. Arbitrary Go goroutines cannot be forcibly terminated safely; hard execution isolation would require a separate execution boundary.

## Architecture

REST clients and the Python SDK submit jobs to the Go API, which persists them in PostgreSQL. Workers poll for eligible jobs, claim them transactionally using `SELECT ... FOR UPDATE SKIP LOCKED`, execute registered handlers, renew leases, and record outcomes. Scheduling and recovery logic also coordinate through PostgreSQL; the recovery controller is not a separate execution queue.

The codebase will be modular, with separate API, worker, and scheduler entry points. Docker Compose will run the local system with PostgreSQL and several independent workers. The React dashboard will expose job lifecycles and operational health through the API.

| Layer | Planned technology |
| --- | --- |
| Backend | Go, `net/http`, Chi |
| Database and queue | PostgreSQL |
| Database access | pgx + sqlc |
| Dashboard | React + TypeScript, Vite, Tailwind CSS |
| Local deployment | Docker + Docker Compose |
| Python SDK | Python + httpx |
| Observability | Prometheus metrics, Go `slog` |
| Testing | Go testing, integration tests, k6 |
| CI/CD | GitHub Actions |

## Planned API

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/api/v1/jobs` | Submit a job |
| GET | `/api/v1/jobs/{id}` | Retrieve a job |
| GET | `/api/v1/jobs` | List, search, and filter jobs |
| GET | `/api/v1/jobs/{id}/attempts` | Inspect execution history |
| POST | `/api/v1/jobs/{id}/retry` | Request a new execution for a terminal failed job |
| POST | `/api/v1/jobs/{id}/cancel` | Request cancellation |
| GET | `/api/v1/workers` | Retrieve worker status |
| GET | `/metrics` | Export Prometheus metrics |
| GET | `/health` | Check API health |

Future MVP submission contract (retry and timeout options are not supported in Milestone 2):

```http
POST /api/v1/jobs
Content-Type: application/json
Idempotency-Key: report-customer-123-oct

{
  "type": "generate_report",
  "payload": {
    "customer_id": 123,
    "month": "2026-10"
  },
  "max_retries": 3,
  "timeout_seconds": 300
}
```

After validation and durable persistence, the API responds without waiting for execution:

```json
{
  "job_id": "job_8f29a1",
  "status": "QUEUED"
}
```

Use `scheduled_at` with a timestamp such as `2026-12-01T09:00:00Z` for future execution. Workers must not claim a job before its eligible run time.

The planned Python SDK wraps this API; it is a convenience client, not a separate execution engine.

## Data model

| Table | Responsibility |
| --- | --- |
| `jobs` | Configuration, payload, current state, result, scheduling, and lease ownership |
| `job_attempts` | Per-attempt execution history, failures, and timing |
| `workers` | Worker registration, heartbeats, and health |
| `idempotency_keys` | Deduplicated submission keys linked to jobs |

Planned `jobs` fields: `id`, `task_type`, `payload`, `status`, `created_at`, `scheduled_at`, `started_at`, `completed_at`, `attempt_count`, `max_retries`, `timeout_seconds`, `next_run_at`, `lease_owner`, `lease_expires_at`, `fencing_token`, `result`, and `last_error`.

Indexes will support job eligibility and scheduling, lease expiration, and status queries. Status transitions must be explicit and transactionally enforced. Separate attempt records preserve earlier outcomes when a job retries.

## Planned repository layout

```text
cmd/
  api/
  worker/
  scheduler/
internal/
  api/
  jobs/
  queue/
  worker/
  scheduler/
  recovery/
  store/
  tasks/
  metrics/
migrations/
sdk/python/
web/
tests/
  integration/
  reliability/
  load/
deploy/docker/
docs/
docker-compose.yml
README.md
CLAUDE.md
AGENTS.md
```

## Development roadmap

1. **Basic execution engine (implemented):** Go project, PostgreSQL schema, submission/retrieval API, and one worker with registered handlers.
2. **Distributed concurrency (implemented):** Transactional claiming, multiple worker processes, bounded goroutine concurrency, and attempt tracking. PostgreSQL integration tests verify competing claims and consistent outcomes.
3. **Reliability and recovery:** Add leases, heartbeats, fencing, retries, backoff, timeouts, idempotency, and dead-letter handling. Recover a killed worker's job without corrupting execution state.
4. **Developer experience:** Add scheduling, cancellation, the remaining REST endpoints, Python SDK, and basic React dashboard. Manage jobs without direct database access.
5. **Observability and fault testing:** Add structured logs, Prometheus metrics, integration tests, and a reproducible fault-injection harness. Demonstrate recovery under controlled failures.
6. **Benchmarking and release:** Measure throughput and latency, optimize observed bottlenecks, add CI pipelines, and document architecture and measured results.

## Reliability and performance validation

Required fault scenarios include killing a worker during execution, restarting the API with thousands of queued jobs, forcing lease expiration, transient handler failures, stale completion after reclamation, concurrent claims, and service restarts that preserve queued jobs.

Track queue latency, throughput, retry rates, recovery time, and scaling as worker count increases.

| Proposed benchmark target | Value |
| --- | --- |
| Throughput | 10,000+ jobs/hour |
| Concurrent workers | 5-10 |
| Lost accepted jobs | 0 |
| Recovery time | Under 30 seconds |

These are targets, not achieved results. Throughput depends on workload and hardware; recovery depends on lease duration and detection intervals. Accepted jobs must remain accounted for even when duplicate execution occurs.

### Definition of done

1. Start PostgreSQL, one API instance, and five independent workers with Docker Compose.
2. Submit thousands of jobs through the REST API and observe distributed execution.
3. Terminate two workers during processing and verify that expired leases make unfinished jobs eligible for re-execution.
4. Restart the workers and observe the queue draining.
5. Account for accepted jobs as completed, legitimately pending, cancelled, or terminally failed.
6. Verify that stale attempts cannot overwrite newer committed results.
7. Inspect attempts, retries, errors, and worker health through the dashboard.
8. Run the automated load and reliability suite and publish measured results.

## Out of scope for the MVP

- DAG workflows, priority queues, and fair scheduling (V2).
- Multi-tenant authentication and quotas (V2/V3).
- Kubernetes deployment and autoscaling, durable workflow checkpoints, and Redis/Kafka queue implementations if benchmarks justify them (V3).
- Multi-region coordination (future).

Grafana, OpenTelemetry, and Terraform can be considered after the core engine is validated. Keep interfaces extensible without implementing these capabilities prematurely.

## Source

This plan is derived from *JobFlow - Complete MVP Specification*, supplied as `JobFlow — Complete MVP Specification (1).pdf`. See [CLAUDE.md](CLAUDE.md) for development guidance.
