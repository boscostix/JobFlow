-- Stop old API and worker binaries before enabling cancellation.
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check, DROP CONSTRAINT jobs_check;
ALTER TABLE jobs
 ADD COLUMN scheduled_at timestamptz,
 ADD COLUMN cancel_requested_at timestamptz,
 ADD CHECK (status IN ('QUEUED','RUNNING','COMPLETED','FAILED','DEAD_LETTER','CANCELLED')),
 ADD CHECK (
 (status='QUEUED' AND started_at IS NULL AND completed_at IS NULL AND result IS NULL)
 OR (status='RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL AND result IS NULL AND last_error IS NULL)
 OR (status='COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NOT NULL AND last_error IS NULL)
 OR (status IN ('FAILED','DEAD_LETTER') AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NOT NULL)
 OR (status='CANCELLED' AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NULL)),
 ADD CHECK (
 (status='CANCELLED' AND cancel_requested_at IS NOT NULL)
 OR status='RUNNING'
 OR (status<>'CANCELLED' AND cancel_requested_at IS NULL));

ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_status_check, DROP CONSTRAINT job_attempts_check;
ALTER TABLE job_attempts
 ADD CHECK (status IN ('RUNNING','COMPLETED','FAILED','CANCELLED')),
 ADD CHECK (
 (status='RUNNING' AND completed_at IS NULL AND result IS NULL AND last_error IS NULL)
 OR (status='COMPLETED' AND completed_at IS NOT NULL AND result IS NOT NULL AND last_error IS NULL)
 OR (status='FAILED' AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NOT NULL)
 OR (status='CANCELLED' AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NULL));

CREATE INDEX jobs_created_idx ON jobs(created_at DESC, id DESC);

CREATE OR REPLACE FUNCTION enforce_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT ((OLD.status='QUEUED' AND NEW.status IN ('RUNNING','CANCELLED'))
 OR (OLD.status='RUNNING' AND OLD.cancel_requested_at IS NULL AND NEW.status IN ('RUNNING','QUEUED','COMPLETED','DEAD_LETTER'))
 OR (OLD.status='RUNNING' AND OLD.cancel_requested_at IS NOT NULL AND NEW.status='CANCELLED')
 OR (OLD.status IN ('FAILED','DEAD_LETTER') AND NEW.status='QUEUED')) THEN
 RAISE EXCEPTION 'invalid job transition: % -> %', OLD.status, NEW.status;
 END IF;
 RETURN NEW;
END; $$;
