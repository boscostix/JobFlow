# Running JobFlow

## Configuration

Compose reads the settings in [.env.example](../.env.example). Use URL-safe database credentials with the supplied Compose file. Start extra replicas with:

```sh
docker compose up -d --scale worker=5
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `WORKER_CONCURRENCY` | `4` | Execution slots per worker process. Valid range is 1–256. |
| `WORKER_POLL_INTERVAL` | `250ms` | How long an idle slot waits before polling PostgreSQL again. |
| `WORKER_LEASE_DURATION` | `10s` | Ownership lease duration. The executable accepts a minimum of 30 ms. |
| `WORKER_SHUTDOWN_TIMEOUT` | `10s` | Maximum graceful drain time after a shutdown signal. |

Keep the container stop grace period longer than the worker shutdown timeout. Workers stop claiming during shutdown and continue renewing leases for draining jobs. Recovery needs a live worker and an available database.

## Upgrade an existing Compose installation

Back up PostgreSQL before upgrading. Stop the old API and workers, apply migrations, and rebuild the stack.

```sh
docker compose stop api worker
docker compose run --rm migrate
docker compose up --build -d
```

Migration 3 converts legacy running jobs into expired leases and preserves their history. It requires restoring a backup for rollback. Migration 4 adds scheduling and cancellation fields. See [the migrations](../migrations/) for schema details.

## Metrics and logs

The API exposes `GET /metrics` on its usual HTTP address. Each worker exposes `GET /metrics` on its own listener. Host processes default to `127.0.0.1:9091`. Give each local worker a distinct `WORKER_METRICS_ADDR` or set it to an empty value to disable its listener. Compose listens on port 9091 inside each worker container and leaves that port on the private Compose network. A worker continues executing if its metrics listener fails, and logs the listener error.

| Metric | Process | Meaning |
| --- | --- | --- |
| `jobflow_queue_depth{eligibility}` | API | Gauge of persisted `QUEUED` jobs. `eligible` means `next_run_at <=` PostgreSQL time at query start. `delayed` includes future schedules and automatic retry backoff. A concurrently locked queued row can still appear in the gauge. |
| `jobflow_queue_wait_seconds` | Worker | Histogram with one observation per committed claim, from the attempt's persisted `next_run_at` to its PostgreSQL `started_at`. Deliberate schedule delay and retry backoff are excluded. |
| `jobflow_attempt_outcomes_total{outcome}` | Worker | One increment per committed attempt resolution. Values are `completed`, `failed`, and `cancelled`. `failed` includes both retries and dead letters. Queued cancellations have no attempt to count. |
| `jobflow_automatic_retries_total{cause}` | Worker | Counts committed automatic requeues with cause `failure` (including timeouts) or `lease_expired`. Manual retry is recorded in logs. |
| `jobflow_lease_recoveries_total` | Worker | Counts committed expired-lease resolutions, including requeues, dead letters, and cancellations. The recovering process records the recovery and its associated outcome. |

The queue gauge is a database snapshot and survives process restarts. Scrape one API per database for queue depth, or deduplicate equivalent API replicas before aggregation. Each API permits one scrape at a time, using a separate connection pool and a two-second query deadline. A failed depth query fails the scrape. Worker counters and histogram buckets reset on process restart and can lose observations between scrapes or between commit and process death. Attempt history is the durable record. Sum per-worker rates after applying `rate()`, for example `sum by (outcome) (rate(jobflow_attempt_outcomes_total[5m]))`. Metric labels contain only the fixed values listed above.

The supplied Prometheus configuration uses one API target and DNS discovery to scrape each worker IP independently. Run Prometheus on the Compose network. [Configuration file](../deploy/prometheus/prometheus.yml) · [Prometheus DNS discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#dns_sd_config)

Use these commands for the default Compose project name `jobflow`.

```sh
curl -fsS http://localhost:8080/metrics
docker compose exec --index 1 worker wget -qO- http://127.0.0.1:9091/metrics
docker run --rm --network jobflow_default -p 127.0.0.1:9090:9090 \
  -v "$PWD/deploy/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v3.5.0
```

API and worker lifecycle logs are JSON. Filter by `job_id` and follow `attempt_number` and `fencing_token` across claims and retries. `worker_id` identifies the attempt owner where available, and `observer_worker_id` identifies the worker reporting the store event, including lease recovery by another worker. Committed outcomes include the resulting `job_status`. `QUEUED` with `retry_delay_ms` identifies an automatic retry. Cancellation logs distinguish a recorded request on a running job from a committed cancellation. Lease renewal failures, fenced outcomes, and outcome-write failures use separate events. Consult attempt history for persisted results and errors.

## Reliability contracts

- Submission accepts `max_retries` (integer 0–100, default 0) and `timeout_seconds` (integer 1–86400, default 300). Omitted or JSON `null` options use defaults. Every execution failure, panic, invalid result, timeout, or expired lease consumes an attempt. There is one initial attempt plus at most `max_retries` retries per cycle; crashes do not bypass that limit.
- Failure with remaining budget returns to `QUEUED`. Retry number r (first retry r=1) waits a random delay in `[cap/2, cap]`, where `cap = min(60s, 1s * 2^(r-1))`. The selected `next_run_at` is persisted using database time. Workers cannot claim before it. Retry eligibility is calculated independently of the original schedule.
- Exhaustion enters terminal `DEAD_LETTER` with an inspectable `last_error`. `POST /api/v1/jobs/{id}/retry` returns 202 and starts a fresh retry cycle using the original configuration. Only dead letters and legacy `FAILED` jobs qualify; missing IDs return 404 and other states return 409. Concurrent retry requests can only perform one transition. Total `attempt_count` and fencing tokens continue increasing; no history is deleted. Old `FAILED` jobs remain inspectable until explicitly retried.
- Each claim increments a per-job fencing token, persists the worker owner and expiry, and inserts a `RUNNING` attempt in one transaction. Outcome/recovery commits the job and attempt together. Attempt statuses are `RUNNING`, `COMPLETED`, `FAILED`, and `CANCELLED`. A dead letter's final attempt is `FAILED`. Recovery records `lease expired` for failures without a pending cancellation. Previously failed attempts retain their original errors/results/timestamps even after retry or manual retry. Job-level `started_at` describes its current/latest attempt while running or terminal, is cleared when requeued, and `completed_at` is set only for terminal outcomes.
- `WORKER_LEASE_DURATION` defaults to 10s (executable minimum 30ms). Each active attempt renews every lease/3; renewal operations have a lease/3 deadline. Owner, token, running state, and unexpired lease must all match. Renewal and outcome paths lock the job before evaluating expiry, so waiting for a database lock cannot revive an expired lease. Recovery locks expired jobs with `SKIP LOCKED` and processes up to 100 per maintenance cycle. Database time is authoritative; validity is checked under the row lock when changing state. A transaction holding the row can delay recovery until it commits/rolls back.
- Every worker maintains a `workers` row and heartbeat about every lease/3, including while draining. Heartbeats are diagnostic and never authorize ownership or recovery. A new process has a new worker ID. Rows are retained; a stopped worker is inferred from its old heartbeat. The worker endpoint and dashboard derive freshness from the database heartbeat timestamp. Heartbeat/renewal/recovery database contention can cause lease loss, so choose lease and connection-pool capacity for the deployment rather than treating very short test leases as production settings.
- Handler execution receives a context deadline and stable `tasks.ExecutionFromContext(ctx).JobID` plus its fencing token. Deadlines/renewal failures signal cancellation; the worker checks cancellation even if a handler returns apparent success. Go cannot forcibly terminate arbitrary goroutines. A noncooperative handler retains its local slot, stops renewing at its deadline, and may overlap a recovered attempt elsewhere. The process shutdown deadline still bounds process lifetime. All slots can be occupied by such handlers until they return or the process is replaced.
- **At-least-once execution:** failures may cause duplicate execution, subject to retry limits. Fencing protects JobFlow database state, not arbitrary external effects. Handlers must use stable operation keys/unique constraints or external fencing to make repeated effects safe. The process crash test demonstrates handler-side effect deduplication using the stable job ID; JobFlow does not provide exactly-once external effects.
