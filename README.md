# JobFlow

JobFlow is a distributed background job runner written in Go. Submit jobs through the REST API or Python SDK, and inspect results, retries, and worker health in the web dashboard. PostgreSQL stores the queue and job history. Independent workers can recover unfinished jobs after a crash.

- Schedule jobs, set execution timeouts, and cancel queued or running work.
- Configure automatic retries with backoff. Inspect or manually retry jobs that reach the dead-letter state.
- Track attempts through the dashboard, structured logs, and Prometheus metrics.

## Quickstart

Install Docker with Compose, then run these commands from the repository root.

```sh
[ -f .env ] || cp .env.example .env
docker compose up --build -d
```

Compose starts PostgreSQL, applies migrations, and starts the API, dashboard, and three workers. Each worker has four execution slots by default.

| Service | Default address |
| --- | --- |
| Dashboard | [localhost:5173](http://localhost:5173) |
| REST API | [localhost:8080](http://localhost:8080/health) |
| PostgreSQL | `localhost:5432` |

Use the dashboard to submit an `add_numbers` job, or submit one through the API.

```sh
curl -fsS http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"add_numbers","payload":{"a":20,"b":22}}'
```

The API returns HTTP 202 and a `job_id` once the job is stored. Replace `JOB_ID` in the command below with that ID.

```sh
curl -fsS http://localhost:8080/api/v1/jobs/JOB_ID
```

Repeat the request until the status is `COMPLETED` and the result is `{"sum":42}`. The included `add_numbers` task accepts two signed 64-bit integers. To run custom work, implement the Go `Handler` interface, register it in `DefaultRegistry`, and rebuild the API and workers. [Task registry](internal/tasks/tasks.go)

## REST API

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/api/v1/jobs` | Submit a job |
| GET | `/api/v1/jobs` | List, search, and filter jobs |
| GET | `/api/v1/jobs/{id}` | Get status, result, or error |
| GET | `/api/v1/jobs/{id}/attempts` | Get attempt history |
| POST | `/api/v1/jobs/{id}/cancel` | Cancel or request cancellation |
| POST | `/api/v1/jobs/{id}/retry` | Start a new execution cycle for a dead-letter or legacy failed job |
| GET | `/api/v1/workers` | List workers and heartbeat status |
| GET | `/health` | Check database connectivity |
| GET | `/metrics` | Export Prometheus metrics |

Jobs are eligible immediately by default. Set `scheduled_at` to an RFC 3339 timestamp for future execution. Use `max_retries` and `timeout_seconds` to control retries and per-attempt deadlines. Use the `Idempotency-Key` header to safely repeat an equivalent submission after a lost response. [API reference](docs/api.md)

## Python SDK

With Python 3.10 or later, install the client from the repository root.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./sdk/python
```

```python
from jobflow import Client

with Client("http://localhost:8080") as client:
    job = client.submit("add_numbers", {"a": 20, "b": 22})
    finished = client.wait(job["job_id"])
    print(finished.get("result") or finished.get("last_error") or finished["status"])
```

The SDK also lists jobs and workers, reads attempt history, and requests cancellation or retry. [SDK example](sdk/python/examples/operator.py) · [Client behavior](docs/api.md#python-sdk-behavior)

## Run and monitor workers

Set `API_PORT`, `DASHBOARD_PORT`, `POSTGRES_PORT`, or `WORKER_CONCURRENCY` in `.env` to adjust the local stack. The supplied Compose configuration binds published ports to loopback and uses development credentials. The API and dashboard have no built-in authentication.

```sh
docker compose up -d --scale worker=5
docker compose logs -f api worker
docker compose down
```

`docker compose down` keeps the PostgreSQL volume. The dashboard shows job results, attempts, and worker heartbeat status. Use `/metrics` on the API and worker processes for queue depth, wait time, outcomes, retries, and lease recovery. [Configuration, upgrades, and monitoring](docs/operations.md)

## Execution guarantees

JobFlow provides **at-least-once execution**, subject to the retry budget. Workers can reclaim jobs after a lease expires, and fencing prevents an older attempt from overwriting a newer outcome. A handler can run more than once, so make external side effects safe to repeat.

Queued jobs cancel immediately. For running jobs, the worker signals cancellation and timeouts through the handler’s Go context. The handler must cooperate to stop executing promptly. Cancellation preserves attempt history, and side effects already performed remain in the external system. [Reliability contracts](docs/operations.md#reliability-contracts)

## Project resources

- [How JobFlow works](docs/how-jobflow-works.md) · Job lifecycle, crash recovery, and worked examples
- [Architecture and design decisions](docs/architecture.md)
- [Development and testing](docs/development.md)
- [Benchmarks and crash recovery](docs/benchmarks.md)
- [GitHub Actions workflow](.github/workflows/ci.yml)
- [MIT license](LICENSE)
