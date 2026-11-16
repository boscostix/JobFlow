-- name: CreateJob :one
INSERT INTO jobs (id, task_type, payload, max_retries, timeout_seconds) VALUES ($1,$2,$3,$4,$5) RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs WHERE id=$1;

-- name: ClaimJob :one
WITH candidate AS (
 SELECT id FROM jobs WHERE status='QUEUED' AND next_run_at <= clock_timestamp()
 ORDER BY next_run_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE jobs SET status='RUNNING',started_at=clock_timestamp(),last_error=NULL,
 attempt_count=attempt_count+1,fencing_token=fencing_token+1,lease_owner=$1,
 lease_expires_at=clock_timestamp()+sqlc.arg(lease_ms)::bigint * interval '1 millisecond'
FROM candidate WHERE jobs.id=candidate.id RETURNING jobs.*;

-- name: CreateAttempt :execrows
INSERT INTO job_attempts(job_id,attempt_number,worker_id,status,started_at,fencing_token)
SELECT id,attempt_count,lease_owner,'RUNNING',started_at,fencing_token FROM jobs WHERE id=$1 AND status='RUNNING';

-- name: LockOwned :one
SELECT * FROM jobs WHERE id=$1 FOR UPDATE;

-- name: LockExpired :one
SELECT * FROM jobs WHERE status='RUNNING' AND lease_expires_at<=clock_timestamp()
ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT 1;

-- name: FinishJob :execrows
UPDATE jobs SET status=sqlc.arg(new_status),completed_at=CASE WHEN sqlc.arg(new_status)::text='QUEUED' THEN NULL ELSE now() END,
 started_at=CASE WHEN sqlc.arg(new_status)::text='QUEUED' THEN NULL ELSE started_at END,
 result=sqlc.narg(result),last_error=sqlc.narg(last_error),lease_owner=NULL,lease_expires_at=NULL,
 next_run_at=clock_timestamp()+sqlc.arg(delay_ms)::bigint * interval '1 millisecond'
WHERE id=sqlc.arg(id) AND status='RUNNING' AND lease_owner=sqlc.arg(owner) AND fencing_token=sqlc.arg(token)
 AND ((sqlc.arg(expired)::boolean AND lease_expires_at<=clock_timestamp()) OR
 (NOT sqlc.arg(expired)::boolean AND lease_expires_at>clock_timestamp()));

-- name: FinishAttempt :execrows
UPDATE job_attempts SET status=$3,completed_at=now(),result=$4,last_error=$5
WHERE job_id=$1 AND fencing_token=$2 AND status='RUNNING';

-- name: RenewLease :execrows
UPDATE jobs SET lease_expires_at=clock_timestamp()+sqlc.arg(lease_ms)::bigint * interval '1 millisecond'
WHERE id=$1 AND status='RUNNING' AND lease_owner=sqlc.arg(owner) AND fencing_token=sqlc.arg(token) AND lease_expires_at>clock_timestamp();

-- name: Heartbeat :exec
INSERT INTO workers(id) VALUES($1) ON CONFLICT(id) DO UPDATE SET heartbeat_at=clock_timestamp();

-- name: ManualRetry :one
UPDATE jobs SET status='QUEUED',retry_start=attempt_count,started_at=NULL,completed_at=NULL,result=NULL,
 next_run_at=clock_timestamp() WHERE id=$1 AND status IN ('FAILED','DEAD_LETTER') RETURNING *;

-- name: InsertIdempotencyKey :execrows
INSERT INTO idempotency_keys(key,job_id) VALUES($1,$2) ON CONFLICT(key) DO NOTHING;

-- name: GetIdempotentJob :one
SELECT jobs.* FROM jobs JOIN idempotency_keys ON jobs.id=idempotency_keys.job_id WHERE key=$1;

-- name: EquivalentSubmission :one
SELECT task_type=$2 AND payload=$3::jsonb AND max_retries=$4 AND timeout_seconds=$5 AS equivalent FROM jobs WHERE id=$1;
