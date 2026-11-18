# REST API

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

## Scheduling and submission

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: scheduled-addition' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22},"scheduled_at":"2026-12-01T09:00:00Z","max_retries":3}'
```

- `scheduled_at` may be omitted, `null`, or an RFC3339 timestamp such as `2026-12-01T09:00:00Z` with uppercase `T`, a required `Z` or numeric offset, at most six fractional digits, and a UTC year from 0001 through 9999. Invalid calendar times and leap seconds return 400. Omitted and `null` schedule immediately and are equivalent for idempotency. Explicit schedules are equivalent only when they represent the same instant, including when the timestamp is in the past. The original timestamp is returned in UTC as `scheduled_at`; jobs without a schedule omit it.
- The initial `next_run_at` is the later of the requested time and the database clock at insertion. Past and current timestamps make the job immediately eligible while preserving the requested timestamp. Workers use database time to enforce eligibility. A future timestamp sets the earliest possible claim; polling, available slots, or database outages can delay the start.
- Automatic retry applies its persisted backoff to `next_run_at`. Manual retry makes the job eligible immediately. Both preserve the original schedule. Scheduled and backing-off jobs have status `QUEUED`; inspect `scheduled_at`, `next_run_at`, and `attempt_count` to distinguish them.

| Field | Meaning |
| --- | --- |
| `type` | Registered task name. The bundled registry contains `add_numbers`. |
| `payload` | Task-specific JSON object. For `add_numbers`, provide exactly two signed 64-bit integers, `a` and `b`. |
| `max_retries` | Integer from 0 to 100, default 0. Each execution cycle has one initial attempt plus this retry budget. |
| `timeout_seconds` | Integer from 1 to 86,400, default 300. The deadline applies to each attempt. |
| `scheduled_at` | Optional earliest execution time. |

Omitted or JSON `null` options use their defaults. A successful submission returns HTTP 202 with `job_id`, `status`, and a `Location` header for retrieving the job. Job retrieval includes the payload, status, timestamps, and the result or error when available. Addition overflow fails the attempt and produces a `DEAD_LETTER` with the default zero retries. Invalid input returns 400. Bodies above 64 KiB return 413. Missing job IDs return 404. State or idempotency conflicts return 409. Database errors return 503.

## Submission idempotency

`Idempotency-Key` is optional, global to this deployment, case-sensitive, and retained indefinitely with its job. It must be a single header value of 1–200 visible ASCII characters without whitespace. Without a key every submission creates a new job. There is no authentication/tenant namespace or automatic key cleanup.

Equivalent submissions have the same validated task type, PostgreSQL JSONB-equal payload (object key order/whitespace do not matter), effective `max_retries`, effective `timeout_seconds`, and the same original `scheduled_at` instant or absence of a schedule. A concurrent or later equivalent request returns 202, the **original ID and current status**, and the same Location, including after the job becomes terminal or is manually retried. Conflicting valid submissions return 409 without creating a job. Validation still runs before deduplication. Job creation and key association commit atomically; a database outage/ambiguous HTTP response can be retried safely with the same key. Equivalent means effective values, so omitted options and explicit defaults are equivalent.

```sh
curl -sS -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: addition-42' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22},"max_retries":3,"timeout_seconds":30}'
```

## Manual retry

`POST /api/v1/jobs/{id}/retry` returns HTTP 202 and makes a `DEAD_LETTER` or legacy `FAILED` job immediately eligible for a new cycle with its original retry budget. The operation preserves the job ID and previous attempt history. Other states return 409.

```sh
curl -fsS -X POST http://localhost:8080/api/v1/jobs/JOB_ID/retry
```

## Cancellation and race outcomes

`POST /api/v1/jobs/{id}/cancel` returns the full job representation and requires no request body. A queued job, including one waiting for a schedule or retry, moves directly to `CANCELLED` with HTTP 200. It records `cancel_requested_at` and `completed_at` and leaves prior attempts intact. A running job records `cancel_requested_at`, retains `RUNNING`, and returns HTTP 202. Repeating an accepted cancellation returns the current snapshot with HTTP 202 while pending or 200 once cancelled, preserving the original request timestamp. Unknown jobs return 404. Completed, dead-letter, and legacy failed jobs return 409.

| Concurrent operation | Outcome |
| --- | --- |
| Claim | If cancellation locks the queued job first, it becomes terminal without a new attempt. If claim commits first, cancellation records a running request. |
| Completion, failure, or timeout outcome | The first committed change under the job lock determines the transition. If cancellation was recorded first, a still-owning attempt closes as `CANCELLED` with no result/error and no retry. If the outcome commits first, cancellation sees the new state and can cancel a requeued job or return 409 for a terminal one. |
| Lease renewal and expiry | The owning worker observes the request at its next successful renewal check, signals its handler context, and stops renewing. An already-expired or stale attempt cannot persist an outcome. |
| Recovery | Recovery atomically closes a requested job and its active attempt as `CANCELLED`, regardless of remaining retry budget. If recovery commits first, it closes the original attempt as failed and either requeues or dead-letters the job. Cancellation then operates on that current state. |
| Manual retry | Only dead-letter and legacy failed jobs can be retried. Cancelled jobs reject manual retry. If a manual retry commits first, the newly queued job can be cancelled. |

Cancellation is cooperative. A handler may return before it observes the signal, continue to occupy its local slot after lease recovery, or already have performed external effects. Cancellation changes JobFlow’s persisted outcome and does not roll back effects. Pending cancellation needs a live worker and database to resolve through the owning attempt or lease recovery.

## Operator reads

- `GET /api/v1/jobs` returns `{"jobs":[...],"next_cursor":"..."}` with the same job shape as retrieval. Results are ordered by descending `created_at`, then descending ID for ties. Use `limit` (default 25, range 1–100) and pass the returned opaque `next_cursor` as `cursor` with the same filters to fetch older jobs. The final page omits `next_cursor`. Pages reflect current data, so concurrent status changes can change filter membership.
- Optional filters are `status` (one of `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`, `CANCELLED`), `type` (exact task type), and `q` (literal prefix of job ID or task type). Filters are case-sensitive and combine with AND. Unknown, duplicated, empty, or malformed parameters, invalid cursors, and values over 128 bytes return 400. A filter with no matches returns an empty `jobs` array.
- `GET /api/v1/jobs/{id}/attempts` returns `{"attempts":[...]}` in ascending attempt number across all retry cycles. Each entry includes the job ID, attempt number, status, worker ID, fencing token, timing, and result or last error when present. A known job can have an empty history. Unknown jobs return 404.
- `GET /api/v1/workers` returns `{"workers":[...]}` ordered by most recent heartbeat, then ascending worker ID. Each entry includes `id`, `started_at`, `heartbeat_at`, and `status`. `ONLINE` means a heartbeat within the last 30 seconds, inclusive, as measured by the database at query start. Older records are `STALE` and remain visible. This is a freshness indicator, independent of ownership and recovery. Worker configurations whose heartbeat interval approaches or exceeds 30 seconds can appear stale while running.
- Database or query timeouts return 503. The operator API has the same local-development access model as submission. Compose binds the exposed ports to loopback.

## Python SDK behavior

The synchronous `jobflow.Client` supports `submit`, `get_job`, `list_jobs`, `attempts`, `retry`, `cancel`, and `workers`. It returns decoded API dictionaries. Submit options include retry budget, execution timeout, idempotency key, and an RFC3339 string or timezone-aware Python `datetime` for scheduling. Use the client as a context manager or call `close()`.

The HTTP timeout defaults to 10 seconds per httpx timeout phase and can be overridden with `timeout`. Non-2xx responses raise `JobFlowHTTPError` with `status_code` and the server message when present. Invalid success bodies raise `JobFlowProtocolError`. Transport failures, including HTTP timeouts, propagate as httpx exceptions. The client does not automatically retry requests.

`wait(job_id, timeout=60, interval=0.5)` polls immediately and returns the first observed terminal job, including failures and cancellations. The polling budget uses monotonic time, and exhaustion raises Python `TimeoutError`. An in-progress request can outlast the polling budget according to the client's HTTP timeout. Polling errors propagate and timing out never cancels the job.
