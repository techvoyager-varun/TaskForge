BEGIN;
ALTER TABLE jobs ADD COLUMN state_version bigint NOT NULL DEFAULT 1 CHECK(state_version>0);
-- A version covers every field returned by job detail, including renewed lease metadata.
CREATE FUNCTION bump_job_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.state_version=OLD.state_version+1; RETURN NEW; END $$;
CREATE TRIGGER job_version BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION bump_job_version();
CREATE FUNCTION bump_attempt_version() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 UPDATE jobs SET state_version=state_version+1 WHERE id=NEW.job_id;
 RETURN NEW;
END $$;
CREATE TRIGGER attempt_version AFTER INSERT OR UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION bump_attempt_version();
REVOKE ALL ON FUNCTION bump_job_version(),bump_attempt_version() FROM PUBLIC;
COMMIT;