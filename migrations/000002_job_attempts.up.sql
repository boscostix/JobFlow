CREATE TABLE job_attempts (
    job_id text NOT NULL REFERENCES jobs(id),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    worker_id text NOT NULL CHECK (worker_id <> ''),
    status text NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    result jsonb,
    last_error text,
    PRIMARY KEY (job_id, attempt_number),
    CHECK (
        (status = 'RUNNING' AND completed_at IS NULL AND result IS NULL AND last_error IS NULL)
        OR (status = 'COMPLETED' AND completed_at IS NOT NULL AND result IS NOT NULL AND last_error IS NULL)
        OR (status = 'FAILED' AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NOT NULL)
    )
);
-- Preserve pre-Milestone-2 executions without inventing a worker identity.
INSERT INTO job_attempts (job_id, attempt_number, worker_id, status, started_at, completed_at, result, last_error)
SELECT id, 1, 'legacy-unknown', status, started_at, completed_at, result, last_error
FROM jobs WHERE status <> 'QUEUED';
