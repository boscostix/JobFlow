#!/usr/bin/env python3
"""Isolated five-worker crash scenario. Requires Docker Compose and Python 3.

Creates a dedicated Compose project and retains its volume for inspection.
Never targets the default development project. No production fault hooks.
"""
from collections import Counter
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
SUCCESSFUL = 2000
TOTAL = SUCCESSFUL + 1


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


def queue_is(expected):
    try:
        with urllib.request.urlopen(BASE + "/metrics", timeout=4) as response:
            lines = response.read().decode().splitlines()
    except OSError:
        return False
    return (f'jobflow_queue_depth{{eligibility="eligible"}} {expected}' in lines and
            'jobflow_queue_depth{eligibility="delayed"} 0' in lines)


if compose("ps", "-aq"):
    raise SystemExit("Verification project already has containers; use a fresh project name.")
compose("up", "--build", "-d", "api")
if sql("SELECT count(*) FROM jobs") != "0":
    raise SystemExit("Verification volume contains jobs; use a fresh project name.")
wait_for(lambda: queue_is(0), "API metrics ready")
accepted = []
for n in range(TOTAL):
    payload = {"a": -999, "b": 1000} if n < 2 else {"a": n, "b": 1}
    if n == SUCCESSFUL:
        payload = {"a": 9223372036854775807, "b": 1}
    body = json.dumps({"type": "add_numbers", "payload": payload, "max_retries": 1}).encode()
    req = urllib.request.Request(BASE + "/api/v1/jobs", body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status == 202
        accepted.append(json.load(resp)["job_id"])
assert len(set(accepted)) == TOTAL
compose("restart", "api")
wait_for(lambda: queue_is(TOTAL), "API metrics report the persisted eligible queue after restart")
assert set(sql("SELECT id FROM jobs WHERE status='QUEUED'").splitlines()) == set(accepted)
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
kill_started = None
for container in compose("ps", "-q", "worker").splitlines():
    logs = subprocess.check_output(["docker", "logs", container], stderr=subprocess.STDOUT, text=True)
    starts = [json.loads(line) for line in logs.splitlines() if '"msg":"worker starting"' in line]
    assert len(starts) == 1, starts
    if starts[0]["worker_id"] in owners:
        if kill_started is None:
            kill_started = time.monotonic()
        subprocess.check_call(["docker", "kill", container], stdout=subprocess.DEVNULL)
        killed.append(container)
assert len(killed) == 2
sql("DROP TRIGGER crash_window ON job_attempts; DROP FUNCTION crash_window();")
wait_for(lambda: sql("SELECT count(*) FROM jobs WHERE payload->>'a'='-999' AND "
                     "status='COMPLETED' AND attempt_count=2") == "2", "both recovered jobs to complete")
recovery_seconds = time.monotonic() - kill_started
wait_for(lambda: sql("SELECT count(*) FROM jobs WHERE status IN ('COMPLETED','DEAD_LETTER')") == str(TOTAL),
         "recovery and drain")


def worker_metrics_match(replicas):
    counts = Counter()
    containers = compose("ps", "-q", "worker").splitlines()
    if len(containers) != replicas:
        return False
    for container in containers:
        scrape = subprocess.run(["docker", "exec", container, "wget", "-T", "4", "-qO-",
                                 "http://127.0.0.1:9091/metrics"], capture_output=True, text=True)
        if scrape.returncode:
            return False
        raw = scrape.stdout
        for line in raw.splitlines():
            if line.startswith("jobflow_"):
                name, value = line.split()
                counts[name] += float(value)
    return {
        "jobflow_queue_wait_seconds_count": TOTAL + 1,
        'jobflow_attempt_outcomes_total{outcome="completed"}': SUCCESSFUL,
        'jobflow_attempt_outcomes_total{outcome="failed"}': 4,
        'jobflow_attempt_outcomes_total{outcome="cancelled"}': 0,
        'jobflow_automatic_retries_total{cause="failure"}': 1,
        'jobflow_automatic_retries_total{cause="lease_expired"}': 2,
        "jobflow_lease_recoveries_total": 2,
    }.items() <= counts.items()


# The killed workers lost two observations of their initial committed claims.
wait_for(lambda: worker_metrics_match(3), "committed metrics from the three surviving replicas", 10)
assert queue_is(0)
compose("up", "-d", "--scale", "worker=5", "worker")
wait_for(lambda: worker_metrics_match(5), "five worker endpoints after restart with reset counters", 10)
assert set(sql("SELECT id FROM jobs").splitlines()) == set(accepted)
assert sql("SELECT count(*) FROM jobs WHERE status='COMPLETED'") == str(SUCCESSFUL)
assert sql("SELECT count(*) FROM jobs WHERE status='COMPLETED' AND result IS DISTINCT FROM "
           "jsonb_build_object('sum',(payload->>'a')::numeric+(payload->>'b')::numeric)") == "0"
assert sql("SELECT id FROM jobs WHERE status='DEAD_LETTER' AND attempt_count=2") == accepted[-1]
assert sql("SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id=a.job_id "
           "WHERE j.status='DEAD_LETTER' AND a.status='FAILED' AND a.last_error LIKE '%overflow%'") == "2"
assert sql("SELECT count(*) FROM job_attempts WHERE status='RUNNING'") == "0"
assert set(sql("SELECT job_id FROM job_attempts WHERE status='FAILED' AND last_error='lease expired'").splitlines()) == set(accepted[:2])
assert sql("SELECT count(*) FROM jobs WHERE attempt_count=2") == "3"
assert sql("SELECT count(*) FROM job_attempts") == str(TOTAL + 3)
assert sql("SELECT count(*) FROM jobs j WHERE attempt_count<>(SELECT count(*) FROM job_attempts a WHERE a.job_id=j.id)") == "0"
assert sql("SELECT count(*) FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.attempt_number=j.attempt_count WHERE a.status<>CASE WHEN j.status='DEAD_LETTER' THEN 'FAILED' ELSE j.status END OR a.result IS DISTINCT FROM j.result OR a.last_error IS DISTINCT FROM j.last_error OR a.fencing_token<>j.fencing_token") == "0"
wait_for(lambda: int(sql("SELECT count(*) FROM workers WHERE heartbeat_at>clock_timestamp()-interval '2 seconds'")) == 5, "five fresh heartbeats")
print(json.dumps({"project": PROJECT, "accepted": TOTAL, "completed": SUCCESSFUL, "dead_lettered": 1,
                  "attempts": TOTAL + 3,
                  "crashed_workers": 2, "recovered_jobs": 2, "recovery_seconds": round(recovery_seconds, 3),
                  "fresh_workers": 5}))
print("Inspect with: docker compose -p " + PROJECT + " logs worker")
print("Cleanup (retains volume): docker compose -p " + PROJECT + " down")
