-- name: CreateJob :one
INSERT INTO jobs (id, task_type, payload) VALUES ($1, $2, $3) RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs WHERE id = $1;

-- name: ClaimJob :one
WITH candidate AS (
    SELECT id FROM jobs WHERE status = 'QUEUED'
    ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE jobs SET status = 'RUNNING', started_at = now()
FROM candidate WHERE jobs.id = candidate.id RETURNING jobs.*;

-- name: CompleteJob :execrows
UPDATE jobs SET status = 'COMPLETED', completed_at = now(), result = $2
WHERE id = $1 AND status = 'RUNNING';

-- name: FailJob :execrows
UPDATE jobs SET status = 'FAILED', completed_at = now(), last_error = $2
WHERE id = $1 AND status = 'RUNNING';

-- name: CreateAttempt :execrows
INSERT INTO job_attempts (job_id, attempt_number, worker_id, status, started_at)
SELECT id, 1, $2, 'RUNNING', started_at FROM jobs WHERE id = $1 AND status = 'RUNNING';

-- name: FinishAttempt :execrows
UPDATE job_attempts SET status = jobs.status, completed_at = jobs.completed_at,
    result = jobs.result, last_error = jobs.last_error
FROM jobs WHERE job_attempts.job_id = jobs.id AND jobs.id = $1
    AND job_attempts.attempt_number = 1 AND job_attempts.status = 'RUNNING'
    AND jobs.status IN ('COMPLETED', 'FAILED');
