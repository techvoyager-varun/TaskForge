BEGIN;
CREATE TABLE projects (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL CHECK(length(name) BETWEEN 1 AND 100),
 token_hash text UNIQUE NOT NULL CHECK(length(token_hash)=64), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE queues (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id),
 name text NOT NULL CHECK(length(name) BETWEEN 1 AND 100), concurrency_limit integer NOT NULL DEFAULT 4 CHECK(concurrency_limit BETWEEN 1 AND 256),
 last_dispatched_at timestamptz NOT NULL DEFAULT '-infinity', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(project_id,name), UNIQUE(id,project_id)
);
CREATE TABLE workers (
 id uuid PRIMARY KEY, name text NOT NULL, capacity integer NOT NULL CHECK(capacity BETWEEN 1 AND 256),
 heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(), started_at timestamptz NOT NULL DEFAULT now(), draining boolean NOT NULL DEFAULT false
);
CREATE TABLE workflows (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), name text NOT NULL,
 status text NOT NULL DEFAULT 'DRAFT' CHECK(status IN ('DRAFT','RUNNING','COMPLETED','FAILED','CANCELLED')),
 created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, UNIQUE(id,project_id)
);
CREATE TABLE jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), queue_id uuid NOT NULL,
 workflow_id uuid, node_key text, type text NOT NULL CHECK(type IN ('noop','echo','sum','sleep','fail')), payload jsonb NOT NULL DEFAULT '{}',
 status text NOT NULL DEFAULT 'READY' CHECK(status IN ('PENDING','READY','RUNNING','COMPLETED','RETRY_WAIT','CANCEL_REQUESTED','CANCELLED','DEAD_LETTERED')),
 priority integer NOT NULL DEFAULT 0 CHECK(priority BETWEEN -100 AND 100), available_at timestamptz NOT NULL DEFAULT now(),
 max_attempts integer NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 100000), timeout_ms integer NOT NULL DEFAULT 30000 CHECK(timeout_ms BETWEEN 100 AND 3600000),
 attempt_no integer NOT NULL DEFAULT 0 CHECK(attempt_no>=0), worker_id uuid REFERENCES workers(id), lease_until timestamptz, deadline_at timestamptz,
 idempotency_key text, request_hash text, result jsonb, error text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 FOREIGN KEY(queue_id,project_id) REFERENCES queues(id,project_id), FOREIGN KEY(workflow_id,project_id) REFERENCES workflows(id,project_id),
 UNIQUE(project_id,idempotency_key), UNIQUE(workflow_id,node_key), UNIQUE(id,workflow_id),
 CHECK((workflow_id IS NULL)=(node_key IS NULL)), CHECK((idempotency_key IS NULL)=(request_hash IS NULL)),
 CHECK(status NOT IN ('RUNNING','CANCEL_REQUESTED') OR (worker_id IS NOT NULL AND lease_until IS NOT NULL AND deadline_at IS NOT NULL))
);
CREATE INDEX jobs_ready ON jobs(queue_id,priority DESC,available_at,created_at) WHERE status='READY';
CREATE INDEX jobs_project_page ON jobs(project_id,created_at DESC,id DESC);
CREATE INDEX jobs_recovery ON jobs(lease_until) WHERE status IN ('RUNNING','CANCEL_REQUESTED');
CREATE INDEX jobs_worker ON jobs(worker_id,status);
CREATE INDEX jobs_workflow ON jobs(workflow_id,status);
CREATE TABLE job_attempts (
 job_id uuid NOT NULL REFERENCES jobs(id), attempt_no integer NOT NULL, worker_id uuid NOT NULL REFERENCES workers(id),
 status text NOT NULL CHECK(status IN ('ASSIGNED','RUNNING','COMPLETED','FAILED','TIMED_OUT','CANCELLED')),
 assigned_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz, error text, PRIMARY KEY(job_id,attempt_no)
);
CREATE TABLE workflow_edges (
 workflow_id uuid NOT NULL REFERENCES workflows(id), parent_id uuid NOT NULL, child_id uuid NOT NULL,
 PRIMARY KEY(workflow_id,parent_id,child_id), CHECK(parent_id<>child_id),
 FOREIGN KEY(parent_id,workflow_id) REFERENCES jobs(id,workflow_id), FOREIGN KEY(child_id,workflow_id) REFERENCES jobs(id,workflow_id)
);
CREATE INDEX edges_child ON workflow_edges(child_id);
CREATE TABLE execution_logs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, job_id uuid NOT NULL, attempt_no integer NOT NULL,
 level text NOT NULL CHECK(level IN ('info','error')), message text NOT NULL CHECK(length(message)<=4000), created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(job_id,attempt_no) REFERENCES job_attempts(job_id,attempt_no)
);
CREATE INDEX logs_job ON execution_logs(job_id,id);
CREATE TABLE dead_letter_jobs (job_id uuid PRIMARY KEY REFERENCES jobs(id),reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE audit_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,project_id uuid NOT NULL REFERENCES projects(id),action text NOT NULL,resource_id uuid NOT NULL,created_at timestamptz NOT NULL DEFAULT now()
);
CREATE FUNCTION validate_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.attempt_no<OLD.attempt_no THEN RAISE EXCEPTION 'attempt fencing counter cannot decrease' USING ERRCODE='23514'; END IF;
 IF NEW.status<>OLD.status AND NOT (
  (OLD.status='PENDING' AND NEW.status IN ('READY','CANCELLED')) OR
  (OLD.status='READY' AND NEW.status IN ('RUNNING','CANCELLED')) OR
  (OLD.status='RUNNING' AND NEW.status IN ('COMPLETED','RETRY_WAIT','DEAD_LETTERED','CANCEL_REQUESTED','CANCELLED')) OR
  (OLD.status='CANCEL_REQUESTED' AND NEW.status='CANCELLED') OR
  (OLD.status='RETRY_WAIT' AND NEW.status IN ('READY','CANCELLED')) OR
  (OLD.status IN ('CANCELLED','DEAD_LETTERED') AND NEW.status='READY')
 ) THEN RAISE EXCEPTION 'illegal job transition: % -> %',OLD.status,NEW.status USING ERRCODE='23514'; END IF;
 NEW.updated_at=clock_timestamp(); RETURN NEW;
END $$;
CREATE TRIGGER job_transition BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION validate_job_transition();
CREATE FUNCTION register_worker(w uuid,label text,slots integer,is_draining boolean DEFAULT false) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path=public,pg_temp AS $$
 INSERT INTO workers(id,name,capacity,draining) VALUES(w,left(label,100),slots,is_draining)
 ON CONFLICT(id) DO UPDATE SET heartbeat_at=clock_timestamp(),draining=EXCLUDED.draining;
$$;
CREATE FUNCTION dispatch_job(w uuid) RETURNS TABLE(job_id uuid,attempt_no integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE worker workers%ROWTYPE; q uuid; j jobs%ROWTYPE; stamp timestamptz:=clock_timestamp();
BEGIN
 SELECT * INTO worker FROM workers WHERE id=w AND NOT draining AND heartbeat_at>stamp-interval '10 seconds' FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN; END IF;
 IF (SELECT count(*) FROM jobs WHERE worker_id=w AND status IN ('RUNNING','CANCEL_REQUESTED'))>=worker.capacity THEN RETURN; END IF;
 SELECT x.id INTO q FROM queues x WHERE
 (SELECT count(*) FROM jobs active WHERE active.queue_id=x.id AND active.status IN ('RUNNING','CANCEL_REQUESTED'))<x.concurrency_limit
 AND EXISTS(SELECT 1 FROM jobs ready WHERE ready.queue_id=x.id AND ready.status='READY' AND ready.available_at<=stamp)
 ORDER BY x.last_dispatched_at,x.id FOR UPDATE OF x SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 IF (SELECT count(*) FROM jobs WHERE queue_id=q AND status IN ('RUNNING','CANCEL_REQUESTED'))>=(SELECT concurrency_limit FROM queues WHERE id=q) THEN RETURN; END IF;
 SELECT * INTO j FROM jobs WHERE queue_id=q AND status='READY' AND available_at<=stamp
 ORDER BY priority DESC,available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 UPDATE jobs SET status='RUNNING',worker_id=w,attempt_no=jobs.attempt_no+1,
 deadline_at=stamp+timeout_ms*interval '1 millisecond',lease_until=LEAST(stamp+interval '6 seconds',stamp+timeout_ms*interval '1 millisecond')
 WHERE id=j.id RETURNING * INTO j;
 INSERT INTO job_attempts(job_id,attempt_no,worker_id,status) VALUES(j.id,j.attempt_no,w,'ASSIGNED');
 INSERT INTO execution_logs(job_id,attempt_no,level,message) VALUES(j.id,j.attempt_no,'info','Lease assigned');
 UPDATE queues SET last_dispatched_at=stamp WHERE id=q;
 RETURN QUERY SELECT j.id,j.attempt_no;
END $$;
CREATE FUNCTION next_assignment(w uuid) RETURNS TABLE(job_id uuid,attempt_no integer,type text,payload jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE j jobs%ROWTYPE;
BEGIN
 SELECT x.* INTO j FROM jobs x JOIN job_attempts a ON a.job_id=x.id AND a.attempt_no=x.attempt_no
 WHERE x.worker_id=w AND x.status='RUNNING' AND x.lease_until>clock_timestamp() AND a.status='ASSIGNED'
 ORDER BY x.created_at FOR UPDATE OF x SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 UPDATE job_attempts SET status='RUNNING',started_at=clock_timestamp() WHERE job_attempts.job_id=j.id AND job_attempts.attempt_no=j.attempt_no;
 INSERT INTO execution_logs(job_id,attempt_no,level,message) VALUES(j.id,j.attempt_no,'info','Handler started');
 RETURN QUERY SELECT j.id,j.attempt_no,j.type,j.payload;
END $$;
CREATE FUNCTION renew_lease(j uuid,w uuid,a integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE state text;
BEGIN
 UPDATE jobs SET lease_until=LEAST(clock_timestamp()+interval '6 seconds',deadline_at)
 WHERE id=j AND worker_id=w AND attempt_no=a AND status IN ('RUNNING','CANCEL_REQUESTED') AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()
 RETURNING status INTO state; RETURN state;
END $$;
CREATE FUNCTION settle_attempt(j uuid,outcome text,output jsonb,failure text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE job jobs%ROWTYPE; next_state text; message text;
BEGIN
 SELECT * INTO STRICT job FROM jobs WHERE id=j FOR UPDATE;
 IF outcome NOT IN ('COMPLETED','FAILED','TIMED_OUT','CANCELLED') THEN RAISE EXCEPTION 'invalid outcome'; END IF;
 IF job.status='CANCEL_REQUESTED' THEN outcome='CANCELLED'; END IF;
 message=left(COALESCE(failure,outcome),4000);
 next_state=CASE WHEN outcome='COMPLETED' THEN 'COMPLETED' WHEN outcome='CANCELLED' THEN 'CANCELLED' WHEN job.attempt_no<job.max_attempts THEN 'RETRY_WAIT' ELSE 'DEAD_LETTERED' END;
 UPDATE job_attempts SET status=outcome,finished_at=clock_timestamp(),error=CASE WHEN outcome='COMPLETED' THEN NULL ELSE message END WHERE job_id=j AND attempt_no=job.attempt_no;
 UPDATE jobs SET status=next_state,result=CASE WHEN outcome='COMPLETED' THEN output ELSE NULL END,
 error=CASE WHEN outcome='COMPLETED' THEN NULL ELSE message END,lease_until=NULL,
 available_at=CASE WHEN next_state='RETRY_WAIT' THEN clock_timestamp()+(LEAST(300000,500*power(2,LEAST(job.attempt_no-1,10)))*(0.5+random()*0.5))*interval '1 millisecond' ELSE available_at END,
 finished_at=CASE WHEN next_state IN ('COMPLETED','CANCELLED','DEAD_LETTERED') THEN clock_timestamp() ELSE NULL END WHERE id=j;
 INSERT INTO execution_logs(job_id,attempt_no,level,message) VALUES(j,job.attempt_no,CASE WHEN outcome='COMPLETED' THEN 'info' ELSE 'error' END,message);
 IF next_state='DEAD_LETTERED' THEN INSERT INTO dead_letter_jobs(job_id,reason) VALUES(j,message) ON CONFLICT(job_id) DO UPDATE SET reason=EXCLUDED.reason,created_at=clock_timestamp(); END IF;
END $$;
CREATE FUNCTION finish_job(j uuid,w uuid,a integer,outcome text,output jsonb,failure text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE job jobs%ROWTYPE;
BEGIN
 SELECT * INTO job FROM jobs WHERE id=j FOR UPDATE;
 IF NOT FOUND OR job.worker_id IS DISTINCT FROM w OR job.attempt_no<>a OR job.status NOT IN ('RUNNING','CANCEL_REQUESTED') OR job.lease_until<=clock_timestamp() THEN RETURN false; END IF;
 PERFORM settle_attempt(j,outcome,output,failure); RETURN true;
END $$;
CREATE FUNCTION maintain_jobs() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE job jobs%ROWTYPE; changed integer;
BEGIN
 IF NOT pg_try_advisory_xact_lock(8761234001) THEN RETURN; END IF;
 FOR job IN SELECT * FROM jobs WHERE status IN ('RUNNING','CANCEL_REQUESTED') AND lease_until<=clock_timestamp() FOR UPDATE SKIP LOCKED LOOP
  PERFORM settle_attempt(job.id,CASE WHEN job.status='CANCEL_REQUESTED' THEN 'CANCELLED' WHEN job.deadline_at<=clock_timestamp() THEN 'TIMED_OUT' ELSE 'FAILED' END,NULL,'Lease expired or execution deadline exceeded');
 END LOOP;
 UPDATE jobs SET status='READY' WHERE status='RETRY_WAIT' AND available_at<=clock_timestamp();
 LOOP
  UPDATE jobs child SET status='CANCELLED',error='A dependency did not complete',finished_at=clock_timestamp()
  WHERE child.status='PENDING' AND EXISTS(SELECT 1 FROM workflow_edges e JOIN jobs parent ON parent.id=e.parent_id WHERE e.child_id=child.id AND parent.status IN ('CANCELLED','DEAD_LETTERED'));
  GET DIAGNOSTICS changed=ROW_COUNT; EXIT WHEN changed=0;
 END LOOP;
 UPDATE jobs child SET status='READY' WHERE child.status='PENDING' AND child.available_at<=clock_timestamp()
 AND (child.workflow_id IS NULL OR EXISTS(SELECT 1 FROM workflows f WHERE f.id=child.workflow_id AND f.status='RUNNING'))
 AND NOT EXISTS(SELECT 1 FROM workflow_edges e JOIN jobs parent ON parent.id=e.parent_id WHERE e.child_id=child.id AND parent.status<>'COMPLETED');
 UPDATE workflows f SET status=CASE WHEN EXISTS(SELECT 1 FROM jobs WHERE workflow_id=f.id AND status='DEAD_LETTERED') THEN 'FAILED'
 WHEN EXISTS(SELECT 1 FROM jobs WHERE workflow_id=f.id AND status='CANCELLED') THEN 'CANCELLED' ELSE 'COMPLETED' END,finished_at=clock_timestamp()
 WHERE f.status='RUNNING' AND NOT EXISTS(SELECT 1 FROM jobs WHERE workflow_id=f.id AND status NOT IN ('COMPLETED','CANCELLED','DEAD_LETTERED'));
END $$;
CREATE FUNCTION cancel_job(p uuid,j uuid) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE job jobs%ROWTYPE;
BEGIN
 SELECT * INTO job FROM jobs WHERE id=j AND project_id=p FOR UPDATE;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF job.status IN ('PENDING','READY','RETRY_WAIT') THEN UPDATE jobs SET status='CANCELLED',finished_at=clock_timestamp() WHERE id=j;
 ELSIF job.status='RUNNING' THEN UPDATE jobs SET status='CANCEL_REQUESTED' WHERE id=j; END IF;
 INSERT INTO audit_events(project_id,action,resource_id) VALUES(p,'job.cancel',j);
 RETURN (SELECT status FROM jobs WHERE id=j);
END $$;
CREATE FUNCTION retry_job(p uuid,j uuid) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE job jobs%ROWTYPE;
BEGIN
 SELECT * INTO job FROM jobs WHERE id=j AND project_id=p FOR UPDATE;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF job.workflow_id IS NOT NULL OR job.status NOT IN ('CANCELLED','DEAD_LETTERED') OR job.attempt_no>=99997 THEN RETURN 'CONFLICT'; END IF;
 UPDATE jobs SET status='READY',max_attempts=attempt_no+3,worker_id=NULL,lease_until=NULL,deadline_at=NULL,result=NULL,error=NULL,finished_at=NULL,available_at=clock_timestamp() WHERE id=j;
 DELETE FROM dead_letter_jobs WHERE job_id=j;
 INSERT INTO audit_events(project_id,action,resource_id) VALUES(p,'job.retry',j); RETURN 'READY';
END $$;
DO $$ BEGIN
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='taskforge_api') THEN CREATE ROLE taskforge_api NOLOGIN; END IF;
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='taskforge_scheduler') THEN CREATE ROLE taskforge_scheduler NOLOGIN; END IF;
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='taskforge_worker') THEN CREATE ROLE taskforge_worker NOLOGIN; END IF;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO taskforge_api,taskforge_scheduler,taskforge_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO taskforge_api;
GRANT INSERT ON queues,jobs,workflows,workflow_edges,audit_events TO taskforge_api;
GRANT UPDATE(status) ON workflows TO taskforge_api;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO taskforge_api;
GRANT EXECUTE ON FUNCTION cancel_job(uuid,uuid),retry_job(uuid,uuid) TO taskforge_api;
GRANT SELECT ON workers TO taskforge_scheduler;
GRANT EXECUTE ON FUNCTION dispatch_job(uuid),maintain_jobs() TO taskforge_scheduler;
GRANT EXECUTE ON FUNCTION register_worker(uuid,text,integer,boolean),next_assignment(uuid),renew_lease(uuid,uuid,integer),finish_job(uuid,uuid,integer,text,jsonb,text) TO taskforge_worker;
COMMIT;
