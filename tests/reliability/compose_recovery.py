#!/usr/bin/env python3
"""Isolated five-worker crash scenario. Requires Docker Compose and Python 3.

Creates a dedicated Compose project and retains its volume for inspection.
Never targets the default development project. No production fault hooks.
"""
import json
import os
import subprocess
import time
import urllib.request

PROJECT = os.environ.get("RELIABILITY_PROJECT", "jobflow-m3-check")
if PROJECT == "jobflow" or not PROJECT.startswith("jobflow-m3-"):
    raise SystemExit("RELIABILITY_PROJECT must begin with jobflow-m3-")
ENV = dict(os.environ, POSTGRES_PORT=os.environ.get("RELIABILITY_DB_PORT", "55433"),
           API_PORT=os.environ.get("RELIABILITY_API_PORT", "18081"),
           WORKER_CONCURRENCY="1", WORKER_LEASE_DURATION="10s", WORKER_POLL_INTERVAL="50ms")
BASE = "http://127.0.0.1:" + ENV["API_PORT"]


def compose(*args):
    return subprocess.check_output(["docker", "compose", "-p", PROJECT, *args], env=ENV, text=True).strip()


def sql(query):
    return compose("exec", "-T", "postgres", "psql", "-U", ENV.get("POSTGRES_USER", "jobflow"),
                   "-d", ENV.get("POSTGRES_DB", "jobflow"), "-v", "ON_ERROR_STOP=1", "-Atc", query)


def wait_for(predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise RuntimeError("timed out: " + description)


if compose("ps", "-aq"):
    raise SystemExit("Verification project already has containers; use a fresh project name.")
compose("up", "--build", "-d", "api")
if sql("SELECT count(*) FROM jobs") != "0":
    raise SystemExit("Verification volume contains jobs; use a fresh project name.")
accepted = []
for n in range(1000):
    payload = {"a": -999, "b": 1000} if n < 2 else {"a": n, "b": 1}
    body = json.dumps({"type": "add_numbers", "payload": payload, "max_retries": 2}).encode()
    req = urllib.request.Request(BASE + "/api/v1/jobs", body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status == 202
        accepted.append(json.load(resp)["job_id"])
assert len(set(accepted)) == 1000
compose("restart", "api")
wait_for(lambda: sql("SELECT count(*) FROM jobs WHERE status='QUEUED'") == "1000", "durable queue")
# Delay outcome history persistence for exactly the two marker jobs. Killing
# their workers rolls back the entire outcome transaction, then lease expiry
# recovers them. Other jobs run normally. Drop the trigger before recovery.
sql("""CREATE FUNCTION crash_window() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.status='COMPLETED' AND EXISTS(SELECT 1 FROM jobs WHERE id=NEW.job_id AND payload->>'a'='-999') THEN
 PERFORM pg_sleep(4); END IF; RETURN NEW; END $$;
 CREATE TRIGGER crash_window BEFORE UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION crash_window();""")
compose("up", "-d", "--scale", "worker=5", "worker")
wait_for(lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event='PgSleep'") == "2", "two blocked outcome writes", 30)
owners = sql("SELECT DISTINCT a.worker_id FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.payload->>'a'='-999'").splitlines()
assert len(owners) == 2, owners
killed = []
for container in compose("ps", "-q", "worker").splitlines():
    logs = subprocess.check_output(["docker", "logs", container], stderr=subprocess.STDOUT, text=True)
    starts = [json.loads(line) for line in logs.splitlines() if '"msg":"worker starting"' in line]
    assert len(starts) == 1, starts
    if starts[0]["worker_id"] in owners:
        subprocess.check_call(["docker", "kill", container], stdout=subprocess.DEVNULL)
        killed.append(container)
assert len(killed) == 2
sql("DROP TRIGGER crash_window ON job_attempts; DROP FUNCTION crash_window();")
wait_for(lambda: sql("SELECT count(*) FROM jobs WHERE status='COMPLETED'") == "1000", "recovery and drain")
compose("up", "-d", "--scale", "worker=5", "worker")
wait_for(lambda: len(compose("ps", "-q", "worker").splitlines()) == 5, "five restarted workers")
assert sql("SELECT count(*) FROM jobs") == "1000"
assert sql("SELECT count(*) FROM jobs WHERE result <> jsonb_build_object('sum',(payload->>'a')::bigint+(payload->>'b')::bigint)") == "0"
assert sql("SELECT count(*) FROM job_attempts WHERE status='RUNNING'") == "0"
assert sql("SELECT count(*) FROM job_attempts WHERE status='FAILED' AND last_error='lease expired'") == "2"
assert sql("SELECT count(*) FROM jobs WHERE attempt_count=2") == "2"
assert sql("SELECT count(*) FROM job_attempts") == "1002"
assert sql("SELECT count(*) FROM jobs j WHERE attempt_count<>(SELECT count(*) FROM job_attempts a WHERE a.job_id=j.id)") == "0"
assert sql("SELECT count(*) FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.attempt_number=j.attempt_count WHERE a.status<>j.status OR a.result IS DISTINCT FROM j.result OR a.fencing_token<>j.fencing_token") == "0"
wait_for(lambda: int(sql("SELECT count(*) FROM workers WHERE heartbeat_at>clock_timestamp()-interval '2 seconds'")) == 5, "five fresh heartbeats")
print(json.dumps({"project": PROJECT, "accepted": 1000, "completed": 1000, "attempts": 1002,
                  "crashed_workers": 2, "recovered_jobs": 2, "fresh_workers": 5}))
print("Inspect with: docker compose -p " + PROJECT + " logs worker")
print("Cleanup (retains volume): docker compose -p " + PROJECT + " down")
