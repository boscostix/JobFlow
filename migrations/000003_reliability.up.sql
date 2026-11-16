-- Upgrade with old workers stopped: old binaries cannot honor leases/fencing.
DROP TRIGGER jobs_transition ON jobs;
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check, DROP CONSTRAINT jobs_check;
ALTER TABLE jobs
 ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
 ADD COLUMN retry_start integer NOT NULL DEFAULT 0 CHECK (retry_start >= 0),
 ADD COLUMN max_retries integer NOT NULL DEFAULT 0 CHECK (max_retries BETWEEN 0 AND 100),
 ADD COLUMN timeout_seconds integer NOT NULL DEFAULT 300 CHECK (timeout_seconds BETWEEN 1 AND 86400),
 ADD COLUMN next_run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN lease_owner text,
 ADD COLUMN lease_expires_at timestamptz,
 ADD COLUMN fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0);
UPDATE jobs SET attempt_count=1, fencing_token=1 WHERE status <> 'QUEUED';
-- Recover pre-upgrade in-flight work through the ordinary expiry path.
UPDATE jobs SET lease_owner='legacy-unknown',lease_expires_at=clock_timestamp() WHERE status='RUNNING';
ALTER TABLE jobs ADD CHECK (status IN ('QUEUED','RUNNING','COMPLETED','FAILED','DEAD_LETTER')),
 ADD CHECK (
 (status='QUEUED' AND started_at IS NULL AND completed_at IS NULL AND result IS NULL)
 OR (status='RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL AND result IS NULL AND last_error IS NULL)
 OR (status='COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NOT NULL AND last_error IS NULL)
 OR (status IN ('FAILED','DEAD_LETTER') AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NOT NULL)),
 ADD CHECK ((status='RUNNING' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
 OR (status<>'RUNNING' AND lease_owner IS NULL AND lease_expires_at IS NULL));
ALTER TABLE job_attempts ADD COLUMN fencing_token bigint NOT NULL DEFAULT 1;
CREATE TABLE workers (id text PRIMARY KEY CHECK (id <> ''), started_at timestamptz NOT NULL DEFAULT clock_timestamp(), heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE idempotency_keys (key text PRIMARY KEY, job_id text NOT NULL UNIQUE REFERENCES jobs(id));
DROP INDEX jobs_queued_idx;
CREATE INDEX jobs_queued_idx ON jobs(next_run_at,created_at,id) WHERE status='QUEUED';
CREATE INDEX jobs_expired_idx ON jobs(lease_expires_at) WHERE status='RUNNING';
CREATE OR REPLACE FUNCTION enforce_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT ((OLD.status='QUEUED' AND NEW.status='RUNNING')
 OR (OLD.status='RUNNING' AND NEW.status IN ('RUNNING','QUEUED','COMPLETED','DEAD_LETTER'))
 OR (OLD.status IN ('FAILED','DEAD_LETTER') AND NEW.status='QUEUED')) THEN
 RAISE EXCEPTION 'invalid job transition: % -> %', OLD.status, NEW.status;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER jobs_transition BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION enforce_job_transition();
