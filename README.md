# JobFlow

A fault-tolerant, horizontally scalable distributed job execution platform built in Go. JobFlow will let developers submit, schedule, execute, retry, and monitor asynchronous background tasks across multiple workers through a REST API and lightweight Python SDK.

**Status: specification stage.** This repository currently contains planning documentation. The capabilities, endpoints, layout, and performance targets below describe the intended MVP; they are not implemented or measured yet. Setup and run instructions will be added as the application is built.

The primary objective is to keep accepted jobs durable, execute them reliably, and recover automatically from worker crashes, transient failures, and service restarts. Distributed coordination will be implemented directly using PostgreSQL transactions.

## MVP capabilities

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

JobFlow provides **at-least-once execution**. A task may run more than once after a crash or lost lease. Fencing protects JobFlow's stored state; task handlers must separately make external side effects safe to repeat.

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

Example submission contract:

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
```

## Development roadmap

1. **Basic execution engine:** Initialize Go, PostgreSQL schema, job submission, and one worker with registered handlers. Deliver submission, execution, and result retrieval.
2. **Distributed concurrency:** Add transactional claiming, multiple worker processes, bounded goroutine concurrency, and attempt tracking. Demonstrate that workers cannot simultaneously claim the same available job.
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
