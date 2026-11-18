# JobFlow

A fault-tolerant, horizontally scalable distributed job execution platform built in Go. JobFlow will let developers submit, schedule, execute, retry, and monitor asynchronous background tasks across multiple workers through a REST API and lightweight Python SDK.

**Status: Milestones 1–5 implemented.** JobFlow supports durable submission, fenced leases and recovery, scheduling, cooperative cancellation, and operator workflows through the REST API, Python SDK, and dashboard. Prometheus metrics and structured logs support fault diagnosis. Benchmarking and CI remain on the roadmap.

## Run locally

Requirements: Docker with Compose. Go 1.24 or newer is needed for local Go commands. The container build pins Go 1.24.9. No host PostgreSQL client or migration tool is required.

```sh
cp .env.example .env
docker compose up --build -d
```

Compose waits for PostgreSQL, runs the versioned migrations as a separate one-shot service, then starts the API, dashboard, and three independent worker processes. Each worker defaults to four execution slots (12 total). PostgreSQL data lives in a persistent named volume. Edit `.env` before startup to change `POSTGRES_PORT`, `API_PORT`, or `DASHBOARD_PORT` if their defaults (5432, 8080, and 5173) are in use. Open the [operator dashboard](http://localhost:5173). Credentials are for local development; use URL-safe credentials with this Compose configuration.

```sh
curl -sS http://localhost:8080/health
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22}}'
# Copy job_id from the response:
curl -sS http://localhost:8080/api/v1/jobs/JOB_ID
```

Submission returns `202 Accepted` with `job_id` and `status: QUEUED` after persistence. Retrieval returns `id`, `type`, `payload`, `status`, timestamps, and the result or `last_error`. The example reaches `COMPLETED` with `result: {"sum":42}`. `add_numbers` accepts exactly two signed 64-bit integers, `a` and `b`. Addition overflow is an execution failure and, with the default zero retries, produces a persisted `DEAD_LETTER` job.

The lifecycle is `QUEUED -> RUNNING -> COMPLETED`, or `RUNNING -> QUEUED` after a retryable failure/lease expiry, `RUNNING -> DEAD_LETTER` on exhaustion, or `QUEUED/RUNNING -> CANCELLED` after cancellation. Invalid JSON, unknown tasks/fields, case aliases, duplicate keys, invalid payloads/options, and invalid idempotency keys return `400`; bodies over 64 KiB return `413`; unknown job IDs return `404`; database errors return `503`. `/health` checks database connectivity. Idempotency conflicts and state conflicts during retry or cancellation return `409`.

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

Real-PostgreSQL tests cover durable submission, competing claims, locked-row skipping, bounded distributed concurrency, shutdown draining, atomic history rollback, migration backfill, lease renewal/recovery races, fencing against stale owners/tokens, retry limits and eligibility, timeouts (including noncooperative handlers), concurrent idempotency, and an actual killed worker process. Additional checks cover scheduled eligibility, cancellation races, the Python SDK, and the dashboard. The suite also checks queue eligibility metrics and committed outcomes during transient failures, cancellation, and fenced recovery. Benchmarks and CI remain deferred.

### Metrics and logs

The API exposes `GET /metrics` on its usual HTTP address. Each worker exposes `GET /metrics` on its own listener. Host processes default to `127.0.0.1:9091`. Give each local worker a distinct `WORKER_METRICS_ADDR` or set it to an empty value to disable its listener. Compose listens on port 9091 inside each worker container and leaves that port on the private Compose network. A worker continues executing if its metrics listener fails, and logs the listener error.

| Metric | Process | Meaning |
| --- | --- | --- |
| `jobflow_queue_depth{eligibility}` | API | Gauge of persisted `QUEUED` jobs. `eligible` means `next_run_at <=` PostgreSQL time at query start. `delayed` includes future schedules and automatic retry backoff. A concurrently locked queued row can still appear in the gauge. |
| `jobflow_queue_wait_seconds` | Worker | Histogram with one observation per committed claim, from the attempt's persisted `next_run_at` to its PostgreSQL `started_at`. Deliberate schedule delay and retry backoff are excluded. |
| `jobflow_attempt_outcomes_total{outcome}` | Worker | One increment per committed attempt resolution. Values are `completed`, `failed`, and `cancelled`. `failed` includes both retries and dead letters. Queued cancellations have no attempt to count. |
| `jobflow_automatic_retries_total{cause}` | Worker | Counts committed automatic requeues with cause `failure` (including timeouts) or `lease_expired`. Manual retry is recorded in logs. |
| `jobflow_lease_recoveries_total` | Worker | Counts committed expired-lease resolutions, including requeues, dead letters, and cancellations. The recovering process records the recovery and its associated outcome. |

The queue gauge is a database snapshot and survives process restarts. Scrape one API per database for queue depth, or deduplicate equivalent API replicas before aggregation. Each API permits one scrape at a time, using a separate connection pool and a two-second query deadline. A failed depth query fails the scrape. Worker counters and histogram buckets reset on process restart and can lose observations between scrapes or between commit and process death. Attempt history is the durable record. Sum per-worker rates after applying `rate()`, for example `sum by (outcome) (rate(jobflow_attempt_outcomes_total[5m]))`. Metric labels contain only the fixed values listed above.

The supplied Prometheus configuration uses one API target and DNS discovery to scrape each worker IP independently. Run Prometheus on the Compose network. [Configuration file](deploy/prometheus/prometheus.yml) · [Prometheus DNS discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#dns_sd_config)

Use these commands for the default Compose project name `jobflow`.

```sh
curl -fsS http://localhost:8080/metrics
docker compose exec --index 1 worker wget -qO- http://127.0.0.1:9091/metrics
docker run --rm --network jobflow_default -p 127.0.0.1:9090:9090 \
  -v "$PWD/deploy/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v3.5.0
```

API and worker lifecycle logs are JSON. Filter by `job_id` and follow `attempt_number` and `fencing_token` across claims and retries. `worker_id` identifies the attempt owner where available, and `observer_worker_id` identifies the worker reporting the store event, including lease recovery by another worker. Committed outcomes include the resulting `job_status`. `QUEUED` with `retry_delay_ms` identifies an automatic retry. Cancellation logs distinguish a recorded request on a running job from a committed cancellation. Lease renewal failures, fenced outcomes, and outcome-write failures use separate events. Consult attempt history for persisted results and errors.

### Reliability contracts

- Submission accepts `max_retries` (integer 0–100, default 0) and `timeout_seconds` (integer 1–86400, default 300). Omitted or JSON `null` options use defaults. Every execution failure, panic, invalid result, timeout, or expired lease consumes an attempt. There is one initial attempt plus at most `max_retries` retries per cycle; crashes do not bypass that limit.
- Failure with remaining budget returns to `QUEUED`. Retry number r (first retry r=1) waits a random delay in `[cap/2, cap]`, where `cap = min(60s, 1s * 2^(r-1))`. The selected `next_run_at` is persisted using database time. Workers cannot claim before it. Retry eligibility is calculated independently of the original schedule.
- Exhaustion enters terminal `DEAD_LETTER` with an inspectable `last_error`. `POST /api/v1/jobs/{id}/retry` returns 202 and starts a fresh retry cycle using the original configuration. Only dead letters and legacy `FAILED` jobs qualify; missing IDs return 404 and other states return 409. Concurrent retry requests can only perform one transition. Total `attempt_count` and fencing tokens continue increasing; no history is deleted. Old `FAILED` jobs remain inspectable until explicitly retried.
- Each claim increments a per-job fencing token, persists the worker owner and expiry, and inserts a `RUNNING` attempt in one transaction. Outcome/recovery commits the job and attempt together. Attempt statuses are `RUNNING`, `COMPLETED`, `FAILED`, and `CANCELLED`. A dead letter's final attempt is `FAILED`. Recovery records `lease expired` for failures without a pending cancellation. Previously failed attempts retain their original errors/results/timestamps even after retry or manual retry. Job-level `started_at` describes its current/latest attempt while running or terminal, is cleared when requeued, and `completed_at` is set only for terminal outcomes.
- `WORKER_LEASE_DURATION` defaults to 10s (executable minimum 30ms). Each active attempt renews every lease/3; renewal operations have a lease/3 deadline. Owner, token, running state, and unexpired lease must all match. Renewal and outcome paths lock the job before evaluating expiry, so waiting for a database lock cannot revive an expired lease. Recovery locks expired jobs with `SKIP LOCKED` and processes up to 100 per maintenance cycle. Database time is authoritative; validity is checked under the row lock when changing state. A transaction holding the row can delay recovery until it commits/rolls back.
- Every worker maintains a `workers` row and heartbeat about every lease/3, including while draining. Heartbeats are diagnostic and never authorize ownership or recovery. A new process has a new worker ID. Rows are retained; a stopped worker is inferred from its old heartbeat. The worker endpoint and dashboard derive freshness from the database heartbeat timestamp. Heartbeat/renewal/recovery database contention can cause lease loss, so choose lease and connection-pool capacity for the deployment rather than treating very short test leases as production settings.
- Handler execution receives a context deadline and stable `tasks.ExecutionFromContext(ctx).JobID` plus its fencing token. Deadlines/renewal failures signal cancellation; the worker checks cancellation even if a handler returns apparent success. Go cannot forcibly terminate arbitrary goroutines. A noncooperative handler retains its local slot, stops renewing at its deadline, and may overlap a recovered attempt elsewhere. The process shutdown deadline still bounds process lifetime. All slots can be occupied by such handlers until they return or the process is replaced.
- **At-least-once execution:** failures may cause duplicate execution, subject to retry limits. Fencing protects JobFlow database state, not arbitrary external effects. Handlers must use stable operation keys/unique constraints or external fencing to make repeated effects safe. The process crash test demonstrates handler-side effect deduplication using the stable job ID; JobFlow does not provide exactly-once external effects.

### Submission idempotency

`Idempotency-Key` is optional, global to this deployment, case-sensitive, and retained indefinitely with its job. It must be a single header value of 1–200 visible ASCII characters without whitespace. Without a key every submission creates a new job. There is no authentication/tenant namespace or key cleanup yet.

Equivalent submissions have the same validated task type, PostgreSQL JSONB-equal payload (object key order/whitespace do not matter), effective `max_retries`, effective `timeout_seconds`, and the same original `scheduled_at` instant or absence of a schedule. A concurrent or later equivalent request returns 202, the **original ID and current status**, and the same Location, including after the job becomes terminal or is manually retried. Conflicting valid submissions return 409 without creating a job. Validation still runs before deduplication. Job creation and key association commit atomically; a database outage/ambiguous HTTP response can be retried safely with the same key. Equivalent means effective values, so omitted options and explicit defaults are equivalent.

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: addition-42' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22},"max_retries":3,"timeout_seconds":30}'
curl -sS -X POST http://localhost:8080/api/v1/jobs/JOB_ID/retry
```

### Distributed workers, history, and upgrades

`WORKER_CONCURRENCY` accepts integers 1–256 (default 4). Each fixed goroutine slot claims only when it can execute and remains occupied through handler execution and outcome persistence. There is no prefetch queue. Handlers and registries must be safe for concurrent use. Claims use `FOR UPDATE SKIP LOCKED` in short transactions; execution holds no claim transaction open. Lease maintenance uses additional database connections independently of slots.

**Upgrade with old API/worker binaries stopped**, run migrations explicitly, then restart new binaries. Old workers can mishandle lifecycle changes. Migration 4 adds nullable schedule/cancellation timestamps and the `CANCELLED` status, retaining existing jobs and histories. Take a pre-upgrade backup for rollback. To upgrade an existing Compose project, run `docker compose stop api worker`, `docker compose run --rm migrate`, then `docker compose up --build -d`. Migration 3 preserves all existing attempts and terminal jobs, and gives pre-upgrade `RUNNING` jobs an already-expired legacy lease. The ordinary recovery path closes those attempts; with their legacy default zero retry budget they become dead letters and may be manually retried. Migration 3 intentionally refuses downgrade because old schemas cannot represent reliability state/history; restore a pre-upgrade backup for rollback.

```sh
# Five independent processes with two local execution slots each:
WORKER_CONCURRENCY=2 docker compose up -d --scale worker=5
# Optional direct SQL inspection of history and heartbeats:
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

The Compose script builds a dedicated project and refuses existing project containers or a nonempty jobs table. It submits 2,001 jobs, restarts the API before starting workers, and checks the exact accepted IDs and the restored eligible queue depth. It starts five workers, kills two during deliberately delayed outcome transactions, removes the test-only database trigger, and checks every result, retry budget, attempt history, and the metrics from the surviving workers. It then restores five replicas and checks for fresh heartbeats. The script retains containers and the volume for inspection and prints the cleanup command. On failure, inspect that project's logs and remove its trigger or stop its containers before rerunning with a fresh project. The fault injection lives in the test harness. `RELIABILITY_DB_PORT` and `RELIABILITY_API_PORT` override script ports. The scenario checks correctness for a controlled workload.

The PostgreSQL suite also kills a worker process while its handler is still executing, forces lease expiration, rejects stale writes after reclamation, and verifies that a transient handler failure waits for retry eligibility and succeeds on its next attempt.

### Milestone 5 verification record

Verified November 18, 2026.

- SQL generation, Go formatting, build, vet, unit tests, and the race detector passed. The full PostgreSQL 17.6 integration suite passed normally and under the race detector. New tests verified eligible and delayed queue metrics, retries after transient handler failure, cancellation overriding a handler result, and the absence of committed outcome signals on rollback or stale writes.
- The five-worker scenario preserved all 2,001 accepted IDs across API restart. It verified 2,000 correct results, one dead letter after exhausting its retry budget, exactly two recovered leases, 2,004 consistent attempts, the surviving workers' metrics, and five fresh heartbeats after worker restart.
- Prometheus 3.5.0 validated the scrape configuration and discovered six healthy targets on the Compose network, one API and five independent workers.
- Two independent review passes caught logging that could delay renewal or cancellation, a deadline race during renewal cleanup, and stale totals in the fault report. The fixes passed regression checks.

The final verification project remains available for inspection. Its API listens on port 18088 and its PostgreSQL instance on port 55438. Stop it with `docker compose -p jobflow-m3-m5-acceptance down` to retain the volume. The controlled fault run measured job accounting and recovery correctness. Throughput and recovery time targets remain for Milestone 6.

### Milestone 4 verification record

Verified November 17, 2026:

- Go generation, formatting, build, vet, unit, and race checks passed. The full PostgreSQL 17.6 integration suite passed normally and under the race detector. PostgreSQL coverage includes scheduled eligibility and idempotency, cancellation racing with claims, outcomes and recovery, cooperative cancellation, and lease recovery when a handler keeps running.
- All five Python SDK tests passed, and the operator example ran against the Compose API. Dashboard lint, type checking, production build, and three isolated Playwright tests passed. The Playwright live test passed against the Compose dashboard, real Go API, and three workers. It verified result precision, filtering, attempt history, scheduled cancellation, retry, and worker heartbeat display.
- The five-worker Compose regression completed all 1,000 accepted jobs after two worker crashes. It recorded exactly two recovered jobs and 1,002 attempts, and all five restarted workers reported fresh heartbeats.
- An independent reviewer found three edge cases in queued-cancellation timestamps and dashboard schedule/key inputs. The fixes passed a focused second review and regression tests. No confirmed P0–P2 findings remained.

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

The API and workers run as separate processes. Docker Compose runs the local stack, and the React dashboard exposes job lifecycles and worker health through the API.

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

## REST API

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

### Scheduling and submission

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: scheduled-addition' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22},"scheduled_at":"2026-12-01T09:00:00Z","max_retries":3}'
```

- `scheduled_at` may be omitted, `null`, or an RFC3339 timestamp such as `2026-12-01T09:00:00Z` with uppercase `T`, a required `Z` or numeric offset, at most six fractional digits, and a UTC year from 0001 through 9999. Invalid calendar times and leap seconds return 400. Omitted and `null` schedule immediately and are equivalent for idempotency. Explicit schedules are equivalent only when they represent the same instant, including when the timestamp is in the past. The original timestamp is returned in UTC as `scheduled_at`; jobs without a schedule omit it.
- The initial `next_run_at` is the later of the requested time and the database clock at insertion. Past and current timestamps make the job immediately eligible while preserving the requested timestamp. Workers use database time to enforce eligibility. A future timestamp sets the earliest possible claim; polling, available slots, or database outages can delay the start.
- Automatic retry applies its persisted backoff to `next_run_at`. Manual retry makes the job eligible immediately. Both preserve the original schedule. Scheduled and backing-off jobs have status `QUEUED`; inspect `scheduled_at`, `next_run_at`, and `attempt_count` to distinguish them.

### Cancellation and race outcomes

`POST /api/v1/jobs/{id}/cancel` returns the full job representation and requires no request body. A queued job, including one waiting for a schedule or retry, moves directly to `CANCELLED` with HTTP 200. It records `cancel_requested_at` and `completed_at` and leaves prior attempts intact. A running job records `cancel_requested_at`, retains `RUNNING`, and returns HTTP 202. Repeating an accepted cancellation returns the current snapshot with HTTP 202 while pending or 200 once cancelled, preserving the original request timestamp. Unknown jobs return 404. Completed, dead-letter, and legacy failed jobs return 409.

| Concurrent operation | Outcome |
| --- | --- |
| Claim | If cancellation locks the queued job first, it becomes terminal without a new attempt. If claim commits first, cancellation records a running request. |
| Completion, failure, or timeout outcome | The first committed change under the job lock determines the transition. If cancellation was recorded first, a still-owning attempt closes as `CANCELLED` with no result/error and no retry. If the outcome commits first, cancellation sees the new state and can cancel a requeued job or return 409 for a terminal one. |
| Lease renewal and expiry | The owning worker observes the request at its next successful renewal check, signals its handler context, and stops renewing. An already-expired or stale attempt cannot persist an outcome. |
| Recovery | Recovery atomically closes a requested job and its active attempt as `CANCELLED`, regardless of remaining retry budget. If recovery commits first, it closes the original attempt as failed and either requeues or dead-letters the job. Cancellation then operates on that current state. |
| Manual retry | Only dead-letter and legacy failed jobs can be retried. Cancelled jobs reject manual retry. If a manual retry commits first, the newly queued job can be cancelled. |

Cancellation is cooperative. A handler may return before it observes the signal, continue to occupy its local slot after lease recovery, or already have performed external effects. Cancellation changes JobFlow’s persisted outcome and does not roll back effects. Pending cancellation needs a live worker and database to resolve through the owning attempt or lease recovery.

### Operator reads

- `GET /api/v1/jobs` returns `{"jobs":[...],"next_cursor":"..."}` with the same job shape as retrieval. Results are ordered by descending `created_at`, then descending ID for ties. Use `limit` (default 25, range 1–100) and pass the returned opaque `next_cursor` as `cursor` with the same filters to fetch older jobs. The final page omits `next_cursor`. Pages reflect current data, so concurrent status changes can change filter membership.
- Optional filters are `status` (one of `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`, `CANCELLED`), `type` (exact task type), and `q` (literal prefix of job ID or task type). Filters are case-sensitive and combine with AND. Unknown, duplicated, empty, or malformed parameters, invalid cursors, and values over 128 bytes return 400. A filter with no matches returns an empty `jobs` array.
- `GET /api/v1/jobs/{id}/attempts` returns `{"attempts":[...]}` in ascending attempt number across all retry cycles. Each entry includes the job ID, attempt number, status, worker ID, fencing token, timing, and result or last error when present. A known job can have an empty history. Unknown jobs return 404.
- `GET /api/v1/workers` returns `{"workers":[...]}` ordered by most recent heartbeat, then ascending worker ID. Each entry includes `id`, `started_at`, `heartbeat_at`, and `status`. `ONLINE` means a heartbeat within the last 30 seconds, inclusive, as measured by the database at query start. Older records are `STALE` and remain visible. This is a freshness indicator, independent of ownership and recovery. Worker configurations whose heartbeat interval approaches or exceeds 30 seconds can appear stale while running.
- Database or query timeouts return 503. The operator API has the same local-development access model as submission. Compose binds the exposed ports to loopback.

## Python SDK

Python 3.10+ and `httpx` are required. Run from the repository root:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e sdk/python
python -m unittest discover -s sdk/python/tests -v
JOBFLOW_URL=http://localhost:8080 python sdk/python/examples/operator.py
```

The synchronous `jobflow.Client` supports `submit`, `get_job`, `list_jobs`, `attempts`, `retry`, `cancel`, and `workers`. It returns decoded API dictionaries. Submit options include retry budget, execution timeout, idempotency key, and an RFC3339 string or timezone-aware Python `datetime` for scheduling. Use the client as a context manager or call `close()`.

The HTTP timeout defaults to 10 seconds per httpx timeout phase and can be overridden with `timeout`. Non-2xx responses raise `JobFlowHTTPError` with `status_code` and the server message when present. Invalid success bodies raise `JobFlowProtocolError`. Transport failures, including HTTP timeouts, propagate as httpx exceptions. The client does not automatically retry requests.

`wait(job_id, timeout=60, interval=0.5)` polls immediately and returns the first observed terminal job, including failures and cancellations. The polling budget uses monotonic time, and exhaustion raises Python `TimeoutError`. An in-progress request can outlast the polling budget according to the client's HTTP timeout. Polling errors propagate and timing out never cancels the job.

## Dashboard development and verification

Compose serves the production build at [http://localhost:5173](http://localhost:5173). The dashboard reads the actual API through a same-origin proxy, refreshes the current data about every five seconds, and supports filterable job pages, job details, attempt history, submission, cancellation, retry, and worker freshness. The submit form targets the registered `add_numbers` task. The form accepts RFC3339 schedule entry with an explicit offset, so the requested instant is independent of browser timezone and daylight-saving changes.

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

The normal Playwright suite isolates loading, empty, and error responses, pagination, and display of pending cancellation. The live test submits signed 64-bit payloads, inspects results and attempts, filters jobs, cancels a scheduled job, retries a dead letter, and checks worker status.

## Data model

| Table | Responsibility |
| --- | --- |
| `jobs` | Configuration, payload, current state, result, scheduling, and lease ownership |
| `job_attempts` | Per-attempt execution history, failures, and timing |
| `workers` | Worker registration, heartbeats, and health |
| `idempotency_keys` | Deduplicated submission keys linked to jobs |

Planned `jobs` fields: `id`, `task_type`, `payload`, `status`, `created_at`, `scheduled_at`, `started_at`, `completed_at`, `attempt_count`, `max_retries`, `timeout_seconds`, `next_run_at`, `cancel_requested_at`, `lease_owner`, `lease_expires_at`, `fencing_token`, `result`, and `last_error`.

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
4. **Developer experience (implemented):** Scheduling, cancellation, REST operator endpoints, Python SDK, and React dashboard for operating jobs through the API.
5. **Observability and fault testing (implemented):** Prometheus metrics, correlated transition logs, PostgreSQL fault tests, and a reproducible five-worker Compose check.
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
