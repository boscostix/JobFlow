-- Reliability histories and retry state cannot be represented by the old schema.
DO $$ BEGIN RAISE EXCEPTION 'migration 3 is irreversible; restore a pre-upgrade backup to downgrade'; END $$;
