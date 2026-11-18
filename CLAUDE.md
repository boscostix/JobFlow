# JobFlow development guidance

## Project context

JobFlow is a Go distributed job execution platform. Its core objective is durable submission, reliable distributed execution, and automatic recovery from worker crashes, transient failures, and service restarts.

Read [README.md](README.md) for the MVP scope, planned API, data model, directory layout, roadmap, and definition of done. These documents summarize the supplied *JobFlow - Complete MVP Specification*.

Milestones 1–4 are implemented: API/worker executables, PostgreSQL migrations/sqlc, distributed bounded concurrency, atomic attempt histories, renewable fenced leases, worker heartbeats, automatic recovery, bounded jittered retries, cooperative execution timeouts, idempotent submission, dead letters, and manual retry. Compose defaults to three workers with four slots each. Scheduling, cancellation, operator REST endpoints, the Python SDK, and the React dashboard are available. Milestone 5 adds Prometheus metrics, structured transition logs, and expanded process/Compose reliability tests. See the README reliability contracts for defaults, transitions, idempotency retention/conflicts, timeout limitations, and upgrade requirements. Do not claim benchmark targets as measured performance. Follow [AGENTS.md](AGENTS.md) for bounded implementation and review.

## Architecture and stack

- Use a modular Go codebase with separate executables in `cmd/api`, `cmd/worker`, and `cmd/scheduler`.
- Use PostgreSQL as both durable storage and the job queue. Coordination belongs in database transactions; recovery is not a separate queue.
- Use Go `net/http` + Chi for REST, pgx + sqlc for database access, and `slog` for structured logging.
- Use React + TypeScript with Vite and Tailwind CSS for a simple operator dashboard.
- Use Python + httpx for a lightweight REST client SDK. Execution remains in Go workers.
- Use Docker Compose for local deployment, Prometheus for metrics, Go testing for backend tests, k6 for load tests, and GitHub Actions for CI.
- Keep business logic under the planned `internal/` packages and database schema changes under `migrations/`.

## Correctness requirements

### Durable submission and state transitions

- Persist accepted jobs before returning successful submission responses.
- Validate task type, payload, and execution configuration at the API boundary.
- Make job lifecycle transitions explicit and transactionally enforced.
- Preserve execution history in `job_attempts`; do not erase prior failures when retrying.
- Maintain the four core tables: `jobs`, `job_attempts`, `workers`, and `idempotency_keys`.
- Index job eligibility and run times, lease expiration, and status queries.

### Distributed claiming and concurrency

- Atomically claim eligible jobs with `SELECT ... FOR UPDATE SKIP LOCKED` and transactional ownership updates.
- Prevent two workers from successfully claiming the same available job concurrently.
- Respect scheduled times and retry eligibility (`scheduled_at` / `next_run_at`).
- Bound each worker's local goroutine concurrency through configuration.
- Keep task execution outside the database claim transaction; long-running handlers must not hold queue row locks.

### Leases, fencing, and recovery

- Record worker ownership, lease expiration, and the execution attempt when claiming a job.
- Renew leases during execution; recover abandoned jobs after lease expiration.
- Use per-attempt fencing tokens to reject stale state updates after ownership changes. Check ownership and fencing atomically when committing attempt-related changes.
- A stale or disconnected worker must not overwrite a newer attempt's committed result.
- Workers and recovery controllers must coordinate through PostgreSQL transactions.

### Delivery and idempotency

- The guarantee is **at-least-once execution**, never exactly-once execution.
- Deduplicate submissions with the same idempotency key and equivalent payload into the same logical job.
- Give handlers a stable job identifier or operation key for deduplicating external side effects across attempts.
- Fencing protects stored JobFlow state; it does not make external side effects exactly once. Document and test this distinction.
- The spec does not fully define conflicting idempotency payloads, key retention, or all lifecycle transitions. Define and document these contracts when implementing them rather than treating an assumption as a settled requirement.

### Retries, timeouts, and cancellation

- Support configurable retry limits with exponential backoff and jitter.
- Keep `max_retries` and total attempt count distinct: the spec illustrates three retries following an initial attempt, allowing four attempts total.
- Move exhausted jobs into a terminal dead-letter state with inspectable errors and a manual retry operation.
- Use context cancellation to signal deadlines to cooperative handlers.
- Do not promise forcible termination of arbitrary Go goroutines; hard isolation requires a separate execution boundary.
- Expose cancellation as a request. Define queued and running cancellation behavior explicitly, including how it interacts with ownership and completion.

## Implementation order

Build in the six milestones documented in the README:

1. Basic submission, storage, single-worker execution, and result retrieval.
2. Distributed transactional claiming, bounded local concurrency, and attempt tracking.
3. Leases, fencing, recovery, retries, timeouts, idempotency, and dead-letter handling.
4. Scheduling, cancellation, complete API, Python SDK, and dashboard.
5. Observability and automated fault injection.
6. Benchmarks, optimization based on measurements, CI, and release documentation.

Keep each milestone demonstrable. Prioritize execution correctness and recovery before expanding the frontend or adding infrastructure.

## Verification expectations

Use real PostgreSQL integration tests for transactional coordination. Reliability testing is part of the MVP, not optional polish. Cover:

- Multiple workers trying to claim the same available jobs.
- Worker death during execution and recovery after lease expiration.
- Forced lease expiration and stale completion after another worker takes ownership.
- Transient task failures, retry limits, backoff, and terminal failure.
- Submission deduplication and handler-side deduplication of repeated effects.
- API restarts with thousands of queued jobs and service restarts without job loss.

Validate the complete Docker Compose scenario with five workers, terminate two during processing, recover and drain the queue, and account for every accepted job. Verify attempt history and worker health through the dashboard.

Measure queue latency, throughput, retry rates, recovery time, and scaling with increasing worker count. The initial targets are 10,000+ jobs/hour, 5-10 workers, zero lost accepted jobs, and recovery under 30 seconds. Report workload, hardware, and lease/detection configuration alongside measured results.

Available checks: `make fmt`, `make build`, `make vet`, `make test`, `make race`, and `TEST_DATABASE_URL=... make integration`. Integration tests use isolated PostgreSQL schemas and require schema-creation permission; they do not truncate existing tables. `make generate` regenerates sqlc code. See the README for startup and migration commands. Run checks appropriate to each implemented change. Lease/crash reliability checks exist in the PostgreSQL integration suite and `tests/reliability/compose_recovery.py`; README documents the SDK and dashboard checks. Load tests and CI remain pending.

## Scope boundaries

Do not add DAG orchestration, priority or fair scheduling, multi-tenant authentication or quotas, Kubernetes/autoscaling, Redis/Kafka queues, durable workflow checkpoints, or multi-region coordination to the MVP unless the user changes scope.

Defer Grafana, OpenTelemetry, and Terraform until the core engine is validated. Keep the dashboard focused on search, status filters, execution histories, errors, worker health, and basic operational metrics.

Keep documentation aligned with implementation. Distinguish requirements, design decisions still to be made, and verified behavior.
