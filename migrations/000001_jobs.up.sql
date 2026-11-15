CREATE TABLE jobs (
    id text PRIMARY KEY,
    task_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','RUNNING','COMPLETED','FAILED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    result jsonb,
    last_error text,
    CHECK (
        (status = 'QUEUED' AND started_at IS NULL AND completed_at IS NULL AND result IS NULL AND last_error IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL AND result IS NULL AND last_error IS NULL)
        OR (status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NOT NULL AND last_error IS NULL)
        OR (status = 'FAILED' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND result IS NULL AND last_error IS NOT NULL)
    )
);
CREATE INDEX jobs_queued_idx ON jobs (created_at, id) WHERE status = 'QUEUED';

CREATE FUNCTION enforce_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT ((OLD.status = 'QUEUED' AND NEW.status = 'RUNNING')
         OR (OLD.status = 'RUNNING' AND NEW.status IN ('COMPLETED', 'FAILED'))) THEN
        RAISE EXCEPTION 'invalid job transition: % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER jobs_transition BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION enforce_job_transition();
