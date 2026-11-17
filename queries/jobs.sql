-- name: CreateJob :one
INSERT INTO jobs (id, task_type, payload, max_retries, timeout_seconds, scheduled_at, next_run_at)
VALUES ($1,$2,$3,$4,$5,sqlc.narg(scheduled_at),GREATEST(clock_timestamp(),sqlc.narg(scheduled_at)))
RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs WHERE id=$1;

-- name: ClaimJob :one
WITH candidate AS (
 SELECT id FROM jobs WHERE status='QUEUED' AND next_run_at <= statement_timestamp()
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
UPDATE jobs SET status=sqlc.arg(new_status),completed_at=CASE WHEN sqlc.arg(new_status)::text='QUEUED' THEN NULL ELSE clock_timestamp() END,
 started_at=CASE WHEN sqlc.arg(new_status)::text='QUEUED' THEN NULL ELSE started_at END,
 result=sqlc.narg(result),last_error=sqlc.narg(last_error),lease_owner=NULL,lease_expires_at=NULL,
 next_run_at=clock_timestamp()+sqlc.arg(delay_ms)::bigint * interval '1 millisecond'
WHERE id=sqlc.arg(id) AND status='RUNNING' AND lease_owner=sqlc.arg(owner) AND fencing_token=sqlc.arg(token)
 AND ((sqlc.arg(expired)::boolean AND lease_expires_at<=clock_timestamp()) OR
 (NOT sqlc.arg(expired)::boolean AND lease_expires_at>clock_timestamp()));

-- name: FinishAttempt :execrows
UPDATE job_attempts SET status=sqlc.arg(status),
 completed_at=COALESCE((SELECT completed_at FROM jobs WHERE id=sqlc.arg(job_id)),clock_timestamp()),
 result=sqlc.narg(result),last_error=sqlc.narg(last_error)
WHERE job_attempts.job_id=sqlc.arg(job_id) AND job_attempts.fencing_token=sqlc.arg(fencing_token) AND job_attempts.status='RUNNING';

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
SELECT task_type=$2 AND payload=$3::jsonb AND max_retries=$4 AND timeout_seconds=$5
 AND scheduled_at IS NOT DISTINCT FROM sqlc.narg(scheduled_at)::timestamptz AS equivalent FROM jobs WHERE id=$1;

-- name: RequestCancellation :one
UPDATE jobs SET cancel_requested_at=statement_timestamp(),
 status=CASE WHEN status='QUEUED' THEN 'CANCELLED' ELSE status END,
 completed_at=CASE WHEN status='QUEUED' THEN statement_timestamp() ELSE completed_at END,
 last_error=NULL
WHERE id=$1 AND status IN ('QUEUED','RUNNING') AND cancel_requested_at IS NULL
RETURNING *;

-- name: ListJobs :many
SELECT * FROM jobs
WHERE (sqlc.arg(status)::text='' OR status=sqlc.arg(status))
 AND (sqlc.arg(task_type)::text='' OR task_type=sqlc.arg(task_type))
 AND (sqlc.arg(search)::text='' OR starts_with(id,sqlc.arg(search)) OR starts_with(task_type,sqlc.arg(search)))
 AND (sqlc.narg(before_time)::timestamptz IS NULL OR (created_at,id)<(sqlc.narg(before_time),sqlc.arg(before_id)::text))
ORDER BY created_at DESC,id DESC LIMIT sqlc.arg(page_size);

-- name: ListAttempts :many
SELECT * FROM job_attempts WHERE job_id=$1 ORDER BY attempt_number;

-- name: ListWorkers :many
SELECT id,started_at,heartbeat_at,
 CASE WHEN heartbeat_at >= statement_timestamp()-interval '30 seconds' THEN 'ONLINE' ELSE 'STALE' END AS status
FROM workers ORDER BY heartbeat_at DESC,id;
