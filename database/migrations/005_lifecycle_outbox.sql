BEGIN;
ALTER TABLE workflows ADD COLUMN state_version bigint NOT NULL DEFAULT 1;
ALTER TABLE workers ADD COLUMN offline_reported boolean NOT NULL DEFAULT false;
ALTER TABLE workers ADD COLUMN offline_epoch bigint NOT NULL DEFAULT 0;
CREATE TABLE outbox_events (
 event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), sequence_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 schema_version integer NOT NULL DEFAULT 1 CHECK(schema_version=1), event_type text NOT NULL,
 aggregate_type text NOT NULL CHECK(aggregate_type IN ('job','workflow','worker')), aggregate_id uuid NOT NULL,
 project_id uuid REFERENCES projects(id), state_version bigint NOT NULL, occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(), body jsonb NOT NULL,
 delivered_at timestamptz, attempts integer NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL DEFAULT now(),
 claim_token uuid, lease_until timestamptz, last_error text
);
CREATE INDEX outbox_pending ON outbox_events(next_attempt_at,sequence_id) WHERE delivered_at IS NULL;
CREATE INDEX outbox_project ON outbox_events(project_id,sequence_id);
CREATE FUNCTION emit_job_event(j jobs,kind text) RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path=public,pg_temp AS $$
 INSERT INTO outbox_events(event_type,aggregate_type,aggregate_id,project_id,state_version,body)
 VALUES(kind,'job',j.id,j.project_id,j.state_version,jsonb_build_object('id',j.id,'project_id',j.project_id,'queue_id',j.queue_id,'workflow_id',j.workflow_id,'status',j.status,'type',j.type,'attempt_no',j.attempt_no,'created_at',j.created_at,'finished_at',j.finished_at))
$$;
CREATE FUNCTION job_lifecycle_event() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  PERFORM emit_job_event(NEW,'JOB_CREATED');
  IF NEW.status='READY' THEN PERFORM emit_job_event(NEW,'JOB_READY'); END IF;
 ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
  IF NEW.status='READY' THEN
   IF OLD.status IN ('RETRY_WAIT','CANCELLED','DEAD_LETTERED') THEN PERFORM emit_job_event(NEW,'JOB_RETRIED'); END IF;
   PERFORM emit_job_event(NEW,'JOB_READY');
  ELSIF NEW.status IN ('RETRY_WAIT','DEAD_LETTERED') THEN PERFORM emit_job_event(NEW,'JOB_FAILED');
  ELSIF NEW.status='RUNNING' THEN PERFORM emit_job_event(NEW,'JOB_ASSIGNED');
  ELSE PERFORM emit_job_event(NEW,'JOB_'||NEW.status); END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER job_events AFTER INSERT OR UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION job_lifecycle_event();
CREATE FUNCTION attempt_lifecycle_event() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE j jobs;
BEGIN
 IF NEW.status='RUNNING' AND (TG_OP='INSERT' OR OLD.status IS DISTINCT FROM NEW.status) THEN
  SELECT * INTO j FROM jobs WHERE id=NEW.job_id; PERFORM emit_job_event(j,'JOB_STARTED');
 END IF; RETURN NEW;
END $$;
-- Alphabetical trigger order ensures the attempt cache-version bump has already happened.
CREATE TRIGGER z_attempt_events AFTER INSERT OR UPDATE ON job_attempts FOR EACH ROW EXECUTE FUNCTION attempt_lifecycle_event();
CREATE FUNCTION workflow_event_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.state_version=OLD.state_version+1; RETURN NEW; END $$;
CREATE TRIGGER workflow_version BEFORE UPDATE ON workflows FOR EACH ROW EXECUTE FUNCTION workflow_event_version();
CREATE FUNCTION workflow_lifecycle_event() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF TG_OP='INSERT' OR NEW.status IS DISTINCT FROM OLD.status THEN
  INSERT INTO outbox_events(event_type,aggregate_type,aggregate_id,project_id,state_version,body)
  VALUES(CASE WHEN TG_OP='INSERT' THEN 'WORKFLOW_CREATED' ELSE 'WORKFLOW_'||NEW.status END,'workflow',NEW.id,NEW.project_id,NEW.state_version,jsonb_build_object('id',NEW.id,'project_id',NEW.project_id,'status',NEW.status,'created_at',NEW.created_at,'finished_at',NEW.finished_at));
 END IF; RETURN NEW;
END $$;
CREATE TRIGGER workflow_events AFTER INSERT OR UPDATE ON workflows FOR EACH ROW EXECUTE FUNCTION workflow_lifecycle_event();
CREATE FUNCTION worker_online_reset() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.heartbeat_at>OLD.heartbeat_at THEN NEW.offline_reported=false; END IF; RETURN NEW; END $$;
CREATE TRIGGER worker_online BEFORE UPDATE ON workers FOR EACH ROW EXECUTE FUNCTION worker_online_reset();
CREATE FUNCTION report_offline_workers() RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE w workers; n integer=0;
BEGIN
 FOR w IN SELECT * FROM workers WHERE NOT offline_reported AND heartbeat_at<clock_timestamp()-interval '10 seconds' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 100 LOOP
  UPDATE workers SET offline_reported=true,offline_epoch=offline_epoch+1 WHERE id=w.id;
  INSERT INTO outbox_events(event_type,aggregate_type,aggregate_id,state_version,body)
  VALUES('WORKER_OFFLINE','worker',w.id,w.offline_epoch+1,jsonb_build_object('id',w.id,'heartbeat_at',w.heartbeat_at)); n=n+1;
 END LOOP; RETURN n;
END $$;
CREATE TABLE processed_events(namespace text NOT NULL,event_id uuid NOT NULL,processed_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(namespace,event_id));
CREATE TABLE analytics_projection (
 namespace text NOT NULL,project_id uuid NOT NULL REFERENCES projects(id),aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,
 state_version bigint NOT NULL,status text NOT NULL,body jsonb NOT NULL,event_id uuid NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(namespace,aggregate_type,aggregate_id)
);
CREATE INDEX analytics_project ON analytics_projection(namespace,project_id,aggregate_type,status);
CREATE TABLE analytics_event_counts(namespace text NOT NULL,project_id uuid NOT NULL REFERENCES projects(id),event_type text NOT NULL,total bigint NOT NULL,PRIMARY KEY(namespace,project_id,event_type));
CREATE TABLE consumer_positions(namespace text NOT NULL,topic text NOT NULL,partition integer NOT NULL,last_offset bigint NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(namespace,topic,partition));
CREATE TABLE consumer_failures(namespace text NOT NULL,topic text NOT NULL,partition integer NOT NULL,message_offset bigint NOT NULL,reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(namespace,topic,partition,message_offset));
CREATE TABLE pipeline_health(component text PRIMARY KEY,heartbeat_at timestamptz NOT NULL DEFAULT now(),available boolean NOT NULL,lag bigint,error_count bigint NOT NULL DEFAULT 0);
DO $$ BEGIN
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='taskforge_publisher') THEN CREATE ROLE taskforge_publisher NOLOGIN; END IF;
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='taskforge_consumer') THEN CREATE ROLE taskforge_consumer NOLOGIN; END IF;
END $$;
GRANT USAGE ON SCHEMA public TO taskforge_publisher,taskforge_consumer;
GRANT SELECT,UPDATE ON outbox_events TO taskforge_publisher;
GRANT INSERT,UPDATE,SELECT ON pipeline_health TO taskforge_publisher,taskforge_consumer;
GRANT INSERT,UPDATE,SELECT ON processed_events,analytics_projection,analytics_event_counts,consumer_positions,consumer_failures TO taskforge_consumer;
GRANT SELECT ON outbox_events,analytics_projection,analytics_event_counts,consumer_positions,pipeline_health,consumer_failures TO taskforge_api;
REVOKE ALL ON FUNCTION emit_job_event(jobs,text),job_lifecycle_event(),attempt_lifecycle_event(),workflow_event_version(),workflow_lifecycle_event(),worker_online_reset(),report_offline_workers() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION report_offline_workers() TO taskforge_publisher;
-- Seed compact current-state snapshots for records which predate event capture.
SELECT emit_job_event(j,'JOB_SNAPSHOT') FROM jobs j;
INSERT INTO outbox_events(event_type,aggregate_type,aggregate_id,project_id,state_version,body)
 SELECT 'WORKFLOW_SNAPSHOT','workflow',id,project_id,state_version,jsonb_build_object('id',id,'project_id',project_id,'status',status,'created_at',created_at,'finished_at',finished_at) FROM workflows;
COMMIT;