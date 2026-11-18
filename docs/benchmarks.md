# Benchmark and recovery results

These benchmarks measure throughput and latency at five and ten workers, including the effect of worker concurrency. A separate fault scenario measures recovery after two worker crashes.

## Repeat the measurements

From the repository root, use Python 3 and Docker with Compose 2.24.4 or newer. The harness runs the pinned k6 container and creates a private Compose project and fresh PostgreSQL volume per sample. It removes the project's containers, local app image tags, and volume after each sample. Build output goes to stderr and each sample emits one JSON line on stdout.

```sh
python3 tests/load/benchmark.py --repeats 2 > /tmp/jobflow-benchmark.jsonl
python3 tests/load/benchmark.py --jobs 100 --workers 2 --vus 4
```

The default workload submits 10,000 immediate `add_numbers` jobs through one API, with 16 k6 virtual users and four slots per worker. It runs with five and ten worker processes, a 250 ms idle poll interval, and a 10 s lease. Submissions omit idempotency keys and use the default zero retry budget. k6 validates each HTTP 202 and records the returned ID. The harness matches all input/ID pairs to PostgreSQL and verifies the result and attempt history for every job. Unexpected failures, missing acknowledgments, missing jobs, or extra attempts fail the run.

| Measurement | Definition |
| --- | --- |
| HTTP submit latency | k6 HTTP request duration percentiles, in milliseconds. |
| Queue latency | PostgreSQL job creation timestamp to the first claim. This includes the end of the submission transaction and applies to the immediate workload. |
| End-to-end latency | PostgreSQL job creation timestamp to the completed attempt timestamp. |
| Completion throughput | Verified completions divided by the interval from the first creation timestamp to the last completion. This is a short-batch rate expressed in jobs/hour. |
| Distribution and retries | Claims per worker and additional durable attempts beyond the initial attempts. |

Run the separate fault scenario to measure recovery and retries. Use a fresh project name and unused ports.

```sh
RELIABILITY_PROJECT=jobflow-m3-bench-repeat RELIABILITY_DB_PORT=55441 \
  RELIABILITY_API_PORT=18091 python3 tests/reliability/compose_recovery.py
```

The fault output includes the monotonic time from invoking the first worker kill until polling observes both recovered jobs completed. That window includes both kill calls, database lock release, lease detection, retry backoff, and re-execution. The project retains its volume for inspection.

## Measured results

Measured November 18, 2026 on an Apple M1 Pro with 16 GiB RAM and macOS 14.6.1. The ARM64 Docker VM reported 8 CPUs and about 7.75 GiB RAM. The run used Docker 29.6.2, Compose 5.3.1, Go 1.24.9 in the application images, PostgreSQL 17.6, and k6 1.4.2. The load generator, API, database, and workers shared the Docker VM.

Each row is one independent 10,000-job batch at the default four slots per worker. Latencies are p50 / p95 / p99 in milliseconds.

| Workers | Run | Batch duration (s) | Completed jobs/s | HTTP submit (ms) | Queue (ms) | End-to-end (ms) |
| ---: | ---: | ---: | ---: | --- | --- | --- |
| 5 | 1 | 8.727 | 1,146 | 11.7 / 25.4 / 40.5 | 108.4 / 204.9 / 241.8 | 116.9 / 216.6 / 253.6 |
| 10 | 1 | 8.914 | 1,122 | 11.6 / 27.2 / 47.2 | 28.4 / 136.8 / 235.8 | 38.0 / 147.8 / 251.4 |
| 5 | 2 | 8.807 | 1,135 | 11.6 / 25.3 / 48.8 | 115.0 / 365.2 / 506.0 | 125.0 / 379.3 / 519.0 |
| 10 | 2 | 8.986 | 1,113 | 12.1 / 27.2 / 39.5 | 28.9 / 213.6 / 279.9 | 38.9 / 227.1 / 292.0 |

All 40,000 acknowledged jobs completed, with zero lost jobs, incorrect results, or retry attempts. Every configured worker claimed work. Each five-worker run saw 1,942–2,037 claims per process. The ten-worker runs saw 843–1,143 per process. The observed short-batch completion rates correspond to about 4.01–4.13 million jobs/hour. A sustained production workload and handlers with external I/O require separate measurements.

### Bottleneck investigation

Ten workers kept the queue shorter, but completion throughput stayed close to the five-worker configuration. The ten-worker batches finished near the end of submission, so increasing worker count had little effect on overall throughput for this workload.

Two earlier five-worker trials with eight slots per process measured 84–202 ms p95 queue latency and 1,215–1,325 jobs/s. The corresponding four-slot baseline measured 509–640 ms p95 and 1,488–1,607 jobs/s. Additional slots reduced queue wait at a throughput cost. Keep four slots as the default and use the worker count and slot count to tune the latency/throughput trade-off for the deployment.

```sh
python3 tests/load/benchmark.py --workers 5 --slots 8 --repeats 2
```

An experimental single-statement submission path passed a deferred-commit failure test, but its five-worker queue p95 rose to 1,154–1,240 ms, and its throughput varied relative to the repeated baselines. The release keeps the existing submission transaction.

### Crash recovery

The measured fault run used five workers with one slot each, a 10 s lease, maintenance every lease/3, and 50 ms idle polling. It completed both recovered jobs **10.017 s** after the first kill was invoked.

Of 2,001 accepted jobs, 2,000 completed with the expected results and one overflow job reached dead letter after exhausting its retry budget. The two recovered jobs and the overflow job each used one retry, for three retries (0.15% of accepted jobs) and 2,004 attempts in total. Zero accepted jobs were missing, and all five workers reported fresh heartbeats after the restart.

The measured project's data remains available with `docker compose -p jobflow-m3-m6-measured logs worker`. Its API and PostgreSQL ports are 18089 and 55439. Use `docker compose -p jobflow-m3-m6-measured down` to stop the stack and retain its volume.
