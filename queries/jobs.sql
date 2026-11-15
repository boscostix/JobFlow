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
