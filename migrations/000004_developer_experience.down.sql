-- Old binaries cannot represent cancellation outcomes or honor pending requests.
DO $$ BEGIN RAISE EXCEPTION 'migration 4 is irreversible; restore a pre-upgrade backup to downgrade'; END $$;
