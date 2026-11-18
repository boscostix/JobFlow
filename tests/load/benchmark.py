#!/usr/bin/env python3
"""Run a fixed k6 workload against fresh, private Compose projects."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
ACK = re.compile(r"^JF_ACK (\d+) (job_[a-f0-9]{32})$")
DATABASE_USER = "jobflow_benchmark"

# All jobs in this database belong to the measured run. The workload has one
# immediate attempt per job, so job-level timestamps retain that attempt's times.
SUMMARY_SQL = """
WITH timings AS (
    SELECT extract(epoch FROM (started_at - created_at)) * 1000 AS queue_ms,
           extract(epoch FROM (completed_at - created_at)) * 1000 AS end_to_end_ms
    FROM jobs
), worker_claims AS (
    SELECT count(*) AS claims FROM job_attempts GROUP BY worker_id
)
SELECT json_build_object(
    'completed', (SELECT count(*) FROM jobs WHERE status = 'COMPLETED'),
    'attempts', (SELECT count(*) FROM job_attempts),
    'incorrect', (SELECT count(*) FROM jobs j
        LEFT JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = 1
        WHERE j.status <> 'COMPLETED' OR j.attempt_count <> 1
           OR j.started_at IS DISTINCT FROM a.started_at
           OR j.completed_at IS DISTINCT FROM a.completed_at
           OR a.status IS DISTINCT FROM 'COMPLETED'
           OR a.fencing_token IS DISTINCT FROM j.fencing_token
           OR a.result IS DISTINCT FROM j.result
           OR j.result IS DISTINCT FROM jsonb_build_object('sum', (j.payload->>'a')::bigint + 1)),
    'batch_seconds', (SELECT extract(epoch FROM max(completed_at) - min(created_at)) FROM jobs),
    'queue_ms_p50_p95_p99', (SELECT percentile_cont(ARRAY[0.5,0.95,0.99])
        WITHIN GROUP (ORDER BY queue_ms) FROM timings),
    'end_to_end_ms_p50_p95_p99', (SELECT percentile_cont(ARRAY[0.5,0.95,0.99])
        WITHIN GROUP (ORDER BY end_to_end_ms) FROM timings),
    'claims_per_worker', (SELECT json_agg(claims ORDER BY claims DESC) FROM worker_claims)
);
"""


def wait_for(predicate, description, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {description}")


def run_benchmark(args, workers, repetition):
    project = f"jobflow-bench-{uuid4().hex[:12]}"
    env = dict(
        os.environ,
        POSTGRES_USER=DATABASE_USER,
        POSTGRES_PASSWORD="jobflow_benchmark",
        POSTGRES_DB=DATABASE_USER,
        WORKER_CONCURRENCY=str(args.slots),
        WORKER_LEASE_DURATION="10s",
        WORKER_POLL_INTERVAL=f"{args.poll_ms}ms",
        COMPOSE_PROGRESS="plain",
    )
    command = ["docker", "compose", "--env-file", "/dev/null", "-p", project,
               "-f", "docker-compose.yml", "-f", "tests/load/compose.yml"]

    def compose(*cli, check=True, **kwargs):
        kwargs.setdefault("stdout", sys.stderr)
        return subprocess.run([*command, *cli], cwd=ROOT, env=env, check=check, text=True, **kwargs)

    def sql(query):
        return compose("exec", "-T", "postgres", "psql", "-X", "-U", DATABASE_USER,
                       "-d", DATABASE_USER, "-v", "ON_ERROR_STOP=1", "-At", "-c", query,
                       stdout=subprocess.PIPE, timeout=15).stdout.strip()

    print(f"{project}: {workers} workers, repetition {repetition}", file=sys.stderr)
    try:
        compose("up", "--build", "-d", "--scale", f"worker={workers}", "api", "worker", timeout=600)
        wait_for(lambda: sql("SELECT count(*) FROM workers WHERE heartbeat_at >= "
                             "statement_timestamp() - interval '10 seconds'") == str(workers),
                 f"{workers} worker heartbeats", 60)
        wait_for(lambda: compose("exec", "-T", "api", "wget", "-qO-", "-T", "2",
                                 "http://127.0.0.1:8080/health", check=False, timeout=5,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0,
                 "the API health endpoint", 30)
        if sql("SELECT count(*) FROM jobs") != "0":
            raise RuntimeError("expected a fresh jobs table")
        load = compose("run", "--rm", "-T", "--no-deps", "-e", f"BENCH_JOBS={args.jobs}",
                       "-e", f"BENCH_VUS={args.vus}", "k6", "run", "--quiet", "--no-color",
                       "--log-format", "raw", "/bench/submit.js",
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=360)
        acknowledged = {}
        for line in load.stderr.splitlines():
            match = ACK.fullmatch(line)
            if match:
                sequence, job_id = int(match[1]), match[2]
                if sequence in acknowledged:
                    raise RuntimeError(f"duplicate acknowledgment for input {sequence}")
                acknowledged[sequence] = job_id
        summary = json.loads(load.stdout)
        if (acknowledged.keys() != set(range(args.jobs)) or len(set(acknowledged.values())) != args.jobs
                or summary["accepted"] != args.jobs):
            raise RuntimeError(f"expected {args.jobs} unique acknowledgments, captured {len(acknowledged)}")

        wait_for(lambda: sql("SELECT count(*) FROM jobs WHERE status = 'COMPLETED'") == str(args.jobs),
                 "the accepted batch to complete", args.drain_timeout)
        stored = json.loads(sql("SELECT json_agg(json_build_array((payload->>'a')::int,id)) FROM jobs"))
        if len(stored) != args.jobs or dict(stored) != acknowledged:
            raise RuntimeError("stored jobs differ from the acknowledged input and ID pairs")
        durable = json.loads(sql(SUMMARY_SQL))
        if durable["incorrect"] or durable["attempts"] != args.jobs or durable["completed"] != args.jobs:
            raise RuntimeError(f"invalid durable outcomes: {durable}")

        return dict(summary, **durable, workers=workers, slots_per_worker=args.slots, vus=args.vus,
                    repetition=repetition, lost=0, retry_attempts=durable["attempts"] - args.jobs,
                    poll_ms=args.poll_ms, lease_seconds=10,
                    completed_jobs_per_hour=args.jobs * 3600 / durable["batch_seconds"])
    except subprocess.CalledProcessError as error:
        if error.stderr:
            print("\n".join(line for line in error.stderr.splitlines() if not ACK.fullmatch(line)), file=sys.stderr)
        compose("logs", "--tail=25", "api", "worker", "postgres", check=False, timeout=30)
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans", "--rmi", "local", timeout=90)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--workers", type=int, nargs="+", default=[5, 10])
    parser.add_argument("--slots", type=int, default=4)
    parser.add_argument("--vus", type=int, default=16)
    parser.add_argument("--poll-ms", type=int, default=250)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--drain-timeout", type=int, default=120)
    args = parser.parse_args()
    if not (1 <= args.vus <= args.jobs <= 2_147_483_647 and 1 <= args.slots <= 256
            and min(*args.workers, args.repeats, args.drain_timeout, args.poll_ms) > 0):
        parser.error("require 1 <= vus <= jobs, slots between 1 and 256, and positive workers, repeats, poll interval and timeout")
    for repetition in range(1, args.repeats + 1):
        for workers in args.workers:
            print(json.dumps(run_benchmark(args, workers, repetition), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
