"""Run with python sdk/python/examples/operator.py against a live JobFlow stack."""

import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from jobflow import Client

with Client(os.getenv("JOBFLOW_URL", "http://localhost:8080")) as client:
    submitted = client.submit("add_numbers", {"a": 20, "b": 22}, idempotency_key=uuid4().hex)
    job_id = submitted["job_id"]
    print(client.wait(job_id))
    print(client.attempts(job_id))
    print(client.list_jobs(status="COMPLETED", task_type="add_numbers", limit=5))
    print(client.workers())

    scheduled = client.submit(
        "add_numbers", {"a": 1, "b": 2}, scheduled_at=datetime.now(timezone.utc) + timedelta(minutes=5)
    )
    print(client.cancel(scheduled["job_id"]))

    overflow = client.submit("add_numbers", {"a": 2**63 - 1, "b": 1})
    print(client.wait(overflow["job_id"]))
    print(client.retry(overflow["job_id"]))
    print(client.wait(overflow["job_id"]))
