# JobFlow

A fault-tolerant, horizontally scalable distributed job execution platform built in Go. JobFlow will let developers submit, schedule, execute, retry, and monitor asynchronous background tasks across multiple workers through a REST API and lightweight Python SDK.

**Status: Milestones 1–3 implemented.** The API and PostgreSQL queue support durable, idempotent submission, distributed execution, bounded concurrency, fenced leases, automatic recovery, bounded retries, cooperative timeouts, dead letters, and atomic attempt history. The complete MVP capabilities and performance targets below remain a roadmap, not achieved results.

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

Submission returns `202 Accepted` with `job_id` and `status: QUEUED` after persistence. Retrieval returns `id`, `type`, `payload`, `status`, timestamps, and the result or `last_error`. The example reaches `COMPLETED` with `result: {"sum":42}`. `add_numbers` accepts exactly two signed 64-bit integers, `a` and `b`. Addition overflow is an execution failure and, with the default zero retries, produces a persisted `DEAD_LETTER` job.

The lifecycle is `QUEUED -> RUNNING -> COMPLETED`, or `RUNNING -> QUEUED` after a retryable failure/lease expiry, or `RUNNING -> DEAD_LETTER` on exhaustion. Invalid JSON, unknown tasks/fields, case aliases, duplicate keys, invalid payloads/options, and invalid idempotency keys return `400`; bodies over 64 KiB return `413`; unknown job IDs return `404`; database errors return `503`. `/health` checks database connectivity. Idempotency conflicts and invalid manual retries return `409`.

```sh
docker compose logs -f api worker
docker compose stop worker            # Stops all worker replicas; submissions remain queued
docker compose restart api           # Persisted jobs survive
docker compose start worker          # Starts all worker replicas to drain queued jobs
docker compose down                  # Keeps the database volume
```

Workers renew leases during execution and any live worker automatically recovers expired leases. Graceful shutdown stops claiming and drains concurrent work up to `WORKER_SHUTDOWN_TIMEOUT` (10 seconds by default), retaining renewal and recovery during draining. Keep Compose's `stop_grace_period` longer than that setting. Abrupt termination and failed outcome writes leave recoverable leases rather than permanently stranded jobs. An outcome-write error stops new claims in that process and drains its other handlers; another worker resolves the abandoned attempt after expiry. Recovery needs at least one live worker and an available database.

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

For host processes, set `DATABASE_URL` and optionally `API_ADDR`, `WORKER_CONCURRENCY`, `WORKER_POLL_INTERVAL`, `WORKER_SHUTDOWN_TIMEOUT`, and `WORKER_LEASE_DURATION`, then use `go run ./cmd/api` and `go run ./cmd/worker` in separate terminals. Start more worker processes in additional terminals; each receives a random process-lifetime worker ID. The `.env` file is consumed by Compose, not automatically loaded by Go.

Real-PostgreSQL tests cover durable submission, competing claims, locked-row skipping, bounded distributed concurrency, shutdown draining, atomic history rollback, migration backfill, lease renewal/recovery races, fencing against stale owners/tokens, retry limits and eligibility, timeouts (including noncooperative handlers), concurrent idempotency, and an actual killed worker process. Scheduling, cancellation, SDK, dashboard, broad metrics, benchmarks, and CI remain deferred.

### Reliability contracts

- Submission accepts `max_retries` (integer 0–100, default 0) and `timeout_seconds` (integer 1–86400, default 300). Omitted or JSON `null` options use defaults. Every execution failure, panic, invalid result, timeout, or expired lease consumes an attempt. There is one initial attempt plus at most `max_retries` retries per cycle; crashes do not bypass that limit.
- Failure with remaining budget returns to `QUEUED`. Retry number r (first retry r=1) waits a random delay in `[cap/2, cap]`, where `cap = min(60s, 1s * 2^(r-1))`. The selected `next_run_at` is persisted using database time. Workers cannot claim before it. This is internal retry eligibility; user scheduling remains deferred.
- Exhaustion enters terminal `DEAD_LETTER` with an inspectable `last_error`. `POST /api/v1/jobs/{id}/retry` returns 202 and starts a fresh retry cycle using the original configuration. Only dead letters and legacy `FAILED` jobs qualify; missing IDs return 404 and other states return 409. Concurrent retry requests can only perform one transition. Total `attempt_count` and fencing tokens continue increasing; no history is deleted. Old `FAILED` jobs remain inspectable until explicitly retried.
- Each claim increments a per-job fencing token, persists the worker owner and expiry, and inserts a `RUNNING` attempt in one transaction. Outcome/recovery commits the job and attempt together. Attempts remain `RUNNING`, `COMPLETED`, or `FAILED`; a dead letter's final attempt is `FAILED`. Lease loss records `lease expired`. Previously failed attempts retain their original errors/results/timestamps even after retry or manual retry. Job-level `started_at` describes its current/latest attempt while running or terminal, is cleared when requeued, and `completed_at` is set only for terminal outcomes.
- `WORKER_LEASE_DURATION` defaults to 10s (executable minimum 30ms). Each active attempt renews every lease/3; renewal operations have a lease/3 deadline. Owner, token, running state, and unexpired lease must all match. Renewal and outcome paths lock the job before evaluating expiry, so waiting for a database lock cannot revive an expired lease. Recovery locks expired jobs with `SKIP LOCKED` and processes up to 100 per maintenance cycle. Database time is authoritative; validity is checked under the row lock when changing state. A transaction holding the row can delay recovery until it commits/rolls back.
- Every worker maintains a `workers` row and heartbeat about every lease/3, including while draining. Heartbeats are diagnostic and never authorize ownership or recovery. A new process has a new worker ID. Rows are retained; a stopped worker is inferred from its old heartbeat. No worker-health API or dashboard exists yet. Heartbeat/renewal/recovery database contention can cause lease loss, so choose lease and connection-pool capacity for the deployment rather than treating very short test leases as production settings.
- Handler execution receives a context deadline and stable `tasks.ExecutionFromContext(ctx).JobID` plus its fencing token. Deadlines/renewal failures signal cancellation; the worker checks cancellation even if a handler returns apparent success. Go cannot forcibly terminate arbitrary goroutines. A noncooperative handler retains its local slot, stops renewing at its deadline, and may overlap a recovered attempt elsewhere. The process shutdown deadline still bounds process lifetime. All slots can be occupied by such handlers until they return or the process is replaced.
- **At-least-once execution:** failures may cause duplicate execution, subject to retry limits. Fencing protects JobFlow database state, not arbitrary external effects. Handlers must use stable operation keys/unique constraints or external fencing to make repeated effects safe. The process crash test demonstrates handler-side effect deduplication using the stable job ID; JobFlow does not provide exactly-once external effects.

### Submission idempotency

`Idempotency-Key` is optional, global to this deployment, case-sensitive, and retained indefinitely with its job. It must be a single header value of 1–200 visible ASCII characters without whitespace. Without a key every submission creates a new job. There is no authentication/tenant namespace or key cleanup yet.

Equivalent submissions have the same validated task type, PostgreSQL JSONB-equal payload (object key order/whitespace do not matter), effective `max_retries`, and effective `timeout_seconds`. A concurrent or later equivalent request returns 202, the **original ID and current status**, and the same Location, including after the job becomes terminal or is manually retried. Conflicting valid submissions return 409 without creating a job. Validation still runs before deduplication. Job creation and key association commit atomically; a database outage/ambiguous HTTP response can be retried safely with the same key. Equivalent means effective values, so omitted options and explicit defaults are equivalent.

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: addition-42' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22},"max_retries":3,"timeout_seconds":30}'
curl -sS -X POST http://localhost:8080/api/v1/jobs/JOB_ID/retry
```

### Distributed workers, history, and upgrades

`WORKER_CONCURRENCY` accepts integers 1–256 (default 4). Each fixed goroutine slot claims only when it can execute and remains occupied through handler execution and outcome persistence. There is no prefetch queue. Handlers and registries must be safe for concurrent use. Claims use `FOR UPDATE SKIP LOCKED` in short transactions; execution holds no claim transaction open. Lease maintenance uses additional database connections independently of slots.

**Upgrade with old API/worker binaries stopped**, run migrations explicitly, then restart new binaries. Old workers do not enforce fencing and cannot maintain the new lifecycle. Migration 3 preserves all existing attempts and terminal jobs, and gives pre-upgrade `RUNNING` jobs an already-expired legacy lease. The ordinary recovery path closes those attempts; with their legacy default zero retry budget they become dead letters and may be manually retried. Migration 3 intentionally refuses downgrade because old schemas cannot represent reliability state/history; restore a pre-upgrade backup for rollback.

```sh
# Five independent processes with two local execution slots each:
WORKER_CONCURRENCY=2 docker compose up -d --scale worker=5
# Inspect history and heartbeats (REST endpoints remain deferred):
docker compose exec postgres psql -U jobflow -d jobflow -c \
  'SELECT job_id, attempt_number, fencing_token, worker_id, status, started_at, completed_at, result, last_error FROM job_attempts ORDER BY started_at;'
docker compose exec postgres psql -U jobflow -d jobflow -c \
  'SELECT id,started_at,heartbeat_at,clock_timestamp()-heartbeat_at AS heartbeat_age FROM workers ORDER BY started_at;'
# PostgreSQL suite including SIGKILL recovery, also under the race detector:
TEST_DATABASE_URL='postgres://jobflow:jobflow@localhost:5432/jobflow?sslmode=disable' \
  go test -race -tags=integration -count=1 ./tests/integration
# Isolated Compose scenario; defaults to ports 55433 and 18081:
python3 tests/reliability/compose_recovery.py
# After stopping the previous project, use a fresh project/volume for a rerun:
RELIABILITY_PROJECT=jobflow-m3-rerun python3 tests/reliability/compose_recovery.py
```

The Compose script requires Python 3 and Docker Compose. It builds a dedicated project, refuses existing containers or a nonempty jobs table, submits 1,000 jobs, restarts the API, starts five workers, kills two during deliberately delayed outcome transactions, removes its test-only database trigger, and verifies recovery, results, histories, and fresh heartbeats. It retains containers/volume for inspection and prints cleanup commands. On failure, inspect that project's logs and remove its trigger or stop its containers before rerunning with a fresh project. No production handler or binary has a fault-injection switch. `RELIABILITY_DB_PORT` and `RELIABILITY_API_PORT` override script ports. This is a correctness scenario, not a benchmark or dashboard validation.

### Milestone 3 verification record

Verified November 15, 2026:

- `make generate`, `make fmt`, `make build`, `make vet`, `make test`, and `make race` passed. The complete real-PostgreSQL 17.6 integration suite passed both normally and under `go test -race -tags=integration -count=1 ./tests/integration`.
- PostgreSQL checks cover competing claims, ownership/token rejection, expired leases, competing recovery controllers, renewal/outcome waits crossing expiry, bounded retry timing and manual retry cycles, rollback on failed attempt writes, cooperative/noncooperative deadlines, heartbeat freshness, and concurrent equivalent/conflicting submissions. A separate OS worker process was killed after recording an effect; another worker recovered it, with both attempts preserved and the handler's unique operation key preventing a duplicate effect.
- A blocked-row regression test initially exposed renewal accepting a lease after a lock wait crossed expiry. Renewal now explicitly acquires the row lock before checking database time; the regression and full suites passed afterward.
- The Go 1.24.9 Compose images, explicit migrations, API, and five single-slot worker containers passed `RELIABILITY_PROJECT=jobflow-m3-final python3 tests/reliability/compose_recovery.py`. All 1,000 jobs persisted across API restart before worker startup. Two workers were killed during delayed outcome transactions. All 1,000 completed with correct sums, exactly two jobs recovered, all 1,002 attempts were accounted for with matching final tokens/results, and five fresh worker heartbeats were observed after restart.
- The first Compose harness used a 20s sleep and 3s leases; removing its test trigger blocked unrelated outcome writes and caused five legitimate lease recoveries instead of the expected two. All jobs still completed. The harness now uses a 4s outcome delay and 10s leases to isolate the intended crashes; the fresh final run passed. PostgreSQL can retain a running statement/row lock briefly after its client process dies, so recovery is not promised at the instant of expiry.
- Independent read-only review found no confirmed P0–P2 defects and independently passed the targeted reliability integration tests. No review fix pass was needed.

The verification project remains available for SQL/log inspection; `docker compose -p jobflow-m3-final down` stops it while retaining its volume. The earlier `jobflow-m3-check` containers were removed; its retained volume contains the initial harness run. These controlled checks establish the tested correctness scenarios, not benchmark targets. Recovery still needs a live worker/database; external effect deduplication and cooperative handlers remain application responsibilities.

The historical records that follow describe earlier binaries and their limitations at those milestones.

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

JobFlow provides **at-least-once execution**, subject to configured retry limits. A task may run more than once after a crash or lost lease. Fencing protects JobFlow's stored state; task handlers must separately make external side effects safe to repeat.

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

Submission contract (retry and timeout options implemented in Milestone 3; `generate_report` is an illustrative future handler):

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

In the planned scheduling milestone, use `scheduled_at` with a timestamp such as `2026-12-01T09:00:00Z` for future execution. Workers must not claim a job before its eligible run time.

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
3. **Reliability and recovery (implemented):** Renewable leases, heartbeats, fencing, automatic recovery, bounded retries/backoff, cooperative timeouts, idempotent submission, dead letters, and manual retry. PostgreSQL and process/Compose checks cover recovery and preserved history.
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
