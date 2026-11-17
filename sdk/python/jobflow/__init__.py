"""Synchronous client for JobFlow."""

import math
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx

__all__ = ["Client", "JobFlowError", "JobFlowHTTPError", "JobFlowProtocolError"]

TERMINAL = frozenset({"COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED"})


class JobFlowError(Exception):
    """Base for errors in the JobFlow HTTP response."""


class JobFlowHTTPError(JobFlowError):
    """An HTTP error response from JobFlow or its proxy."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class JobFlowProtocolError(JobFlowError):
    """A successful response was not a JSON object."""


class Client:
    """Owns an httpx client. Use as a context manager or call close()."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        timeout: float | httpx.Timeout = 10.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self._http = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._http.request(method, f"/api/v1/{path}", **kwargs)
        try:
            body = response.json()
        except ValueError:
            body = None
        if not response.is_success:
            message = body.get("error") if isinstance(body, dict) else None
            raise JobFlowHTTPError(
                response.status_code, message if isinstance(message, str) else response.reason_phrase
            )
        if not isinstance(body, dict):
            raise JobFlowProtocolError("expected a JSON object from JobFlow")
        return body

    @staticmethod
    def _job_path(job_id: str) -> str:
        # A JobFlow ID must be one path segment.
        if not job_id or job_id in {".", ".."} or "/" in job_id:
            raise ValueError("invalid job ID")
        return f"jobs/{quote(job_id, safe='')}"

    def submit(
        self,
        task_type: str,
        payload: dict[str, Any],
        *,
        max_retries: int | None = None,
        timeout_seconds: int | None = None,
        scheduled_at: str | datetime | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(scheduled_at, datetime):
            if scheduled_at.utcoffset() is None:
                raise ValueError("scheduled_at must be timezone-aware")
            scheduled_at = scheduled_at.astimezone(timezone.utc).isoformat()
        body = {
            "type": task_type,
            "payload": payload,
            "max_retries": max_retries,
            "timeout_seconds": timeout_seconds,
            "scheduled_at": scheduled_at,
        }
        headers = {} if idempotency_key is None else {"Idempotency-Key": idempotency_key}
        return self._request("POST", "jobs", json=body, headers=headers)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", self._job_path(job_id))

    def list_jobs(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
        search: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        filters = {"status": status, "type": task_type, "q": search, "limit": limit, "cursor": cursor}
        return self._request("GET", "jobs", params={k: v for k, v in filters.items() if v is not None})

    def attempts(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"{self._job_path(job_id)}/attempts")

    def retry(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"{self._job_path(job_id)}/retry")

    def cancel(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"{self._job_path(job_id)}/cancel")

    def workers(self) -> dict[str, Any]:
        return self._request("GET", "workers")

    def wait(self, job_id: str, *, timeout: float = 60.0, interval: float = 0.5) -> dict[str, Any]:
        """Poll until terminal. An in-flight HTTP request uses the client timeout."""
        if not all(math.isfinite(value) and value > 0 for value in (timeout, interval)):
            raise ValueError("timeout and interval must be finite and positive")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.get_job(job_id)
            status = job.get("status")
            if not isinstance(status, str):
                raise JobFlowProtocolError("expected a job status string")
            if status in TERMINAL:
                return job
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(interval, remaining))
        raise TimeoutError(f"job {job_id} did not reach a terminal state within {timeout:g}s")
