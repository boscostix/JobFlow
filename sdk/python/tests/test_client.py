import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx

from jobflow import Client, JobFlowHTTPError, JobFlowProtocolError


class ClientTests(unittest.TestCase):
    def test_endpoints_and_submission_options(self):
        requests = []

        def reply(request):
            requests.append(request)
            return httpx.Response(202 if request.method == "POST" else 200, json={"status": "QUEUED"})

        with Client(transport=httpx.MockTransport(reply)) as client:
            client.submit(
                "add_numbers",
                {"a": 2**63 - 1, "b": 1},
                max_retries=2,
                timeout_seconds=45,
                scheduled_at=datetime(2027, 1, 1, 12, tzinfo=timezone(timedelta(hours=-6))),
                idempotency_key="test-key",
            )
            client.get_job("job_123")
            client.list_jobs(status="QUEUED", task_type="add_numbers", search="job_", limit=11, cursor="next")
            client.attempts("job_123")
            client.retry("job_123")
            client.cancel("job_123")
            client.workers()
        self.assertEqual(
            [(r.method, r.url.path) for r in requests],
            [
                ("POST", "/api/v1/jobs"),
                ("GET", "/api/v1/jobs/job_123"),
                ("GET", "/api/v1/jobs"),
                ("GET", "/api/v1/jobs/job_123/attempts"),
                ("POST", "/api/v1/jobs/job_123/retry"),
                ("POST", "/api/v1/jobs/job_123/cancel"),
                ("GET", "/api/v1/workers"),
            ],
        )
        self.assertEqual(requests[0].headers["Idempotency-Key"], "test-key")
        self.assertEqual(
            json.loads(requests[0].content),
            {
                "type": "add_numbers",
                "payload": {"a": 2**63 - 1, "b": 1},
                "timeout_seconds": 45,
                "max_retries": 2,
                "scheduled_at": "2027-01-01T18:00:00+00:00",
            },
        )
        self.assertEqual(
            dict(requests[2].url.params),
            {"status": "QUEUED", "type": "add_numbers", "q": "job_", "limit": "11", "cursor": "next"},
        )

    def test_http_and_protocol_errors(self):
        replies = iter([
            httpx.Response(409, json={"error": "key conflict"}),
            httpx.Response(502, text="bad gateway"),
            httpx.Response(200, json=[]),
            httpx.Response(200, text="{"),
            httpx.Response(200, json={"status": None}),
        ])
        with Client(transport=httpx.MockTransport(lambda _: next(replies))) as client:
            with self.assertRaisesRegex(JobFlowHTTPError, "key conflict") as conflict:
                client.cancel("job_123")
            self.assertEqual(conflict.exception.status_code, 409)
            with self.assertRaisesRegex(JobFlowHTTPError, "Bad Gateway"):
                client.workers()
            with self.assertRaises(JobFlowProtocolError):
                client.workers()
            with self.assertRaises(JobFlowProtocolError):
                client.workers()
            with self.assertRaises(JobFlowProtocolError):
                client.wait("job_123")

    def test_transport_error_propagates(self):
        def timeout(request):
            self.assertEqual(request.extensions["timeout"]["read"], 2.0)
            raise httpx.ReadTimeout("timed out", request=request)

        with Client(timeout=2.0, transport=httpx.MockTransport(timeout)) as client:
            with self.assertRaises(httpx.ReadTimeout):
                client.list_jobs()

    def test_wait_observes_terminal_state(self):
        states = iter(["QUEUED", "RUNNING", "CANCELLED"])
        transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"status": next(states)}))
        with Client(transport=transport) as client, patch("jobflow.time.sleep") as sleep:
            self.assertEqual(client.wait("job_123")["status"], "CANCELLED")
            self.assertEqual(sleep.call_count, 2)

    def test_poll_budget_and_validation(self):
        transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"status": "RUNNING"}))
        with Client(transport=transport) as client:
            with patch("jobflow.time.monotonic", side_effect=[0, 0, 0, 1]), patch("jobflow.time.sleep") as sleep:
                with self.assertRaises(TimeoutError):
                    client.wait("job_123", timeout=1.0, interval=2.0)
                sleep.assert_called_once_with(1.0)
            for limit in (0, float("nan"), float("inf")):
                with self.assertRaises(ValueError):
                    client.wait("job_123", timeout=limit)
            with self.assertRaisesRegex(ValueError, "timezone-aware"):
                client.submit("add_numbers", {"a": 1, "b": 2}, scheduled_at=datetime(2027, 1, 1))


if __name__ == "__main__":
    unittest.main()
