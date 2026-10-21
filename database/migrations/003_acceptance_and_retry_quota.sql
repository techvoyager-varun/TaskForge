BEGIN;
-- Locking the job row alone does not refresh the joined attempt's SELECT snapshot.
-- The attempt update is the acceptance compare-and-swap and must affect one row.
CREATE OR REPLACE FUNCTION next_assignment(w uuid) RETURNS TABLE(job_id uuid,attempt_no integer,type text,payload jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE j jobs%ROWTYPE;
BEGIN
 SELECT x.* INTO j FROM jobs x JOIN job_attempts a ON a.job_id=x.id AND a.attempt_no=x.attempt_no
 WHERE x.worker_id=w AND x.status='RUNNING' AND x.lease_until>clock_timestamp() AND a.status='ASSIGNED'
 ORDER BY x.created_at FOR UPDATE OF x SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 UPDATE job_attempts SET status='RUNNING',started_at=clock_timestamp()
 WHERE job_attempts.job_id=j.id AND job_attempts.attempt_no=j.attempt_no AND status='ASSIGNED';
 IF NOT FOUND THEN RETURN; END IF;
 INSERT INTO execution_logs(job_id,attempt_no,level,message) VALUES(j.id,j.attempt_no,'info','Handler started');
 RETURN QUERY SELECT j.id,j.attempt_no,j.type,j.payload;
END $$;
CREATE OR REPLACE FUNCTION retry_job(p uuid,j uuid) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE job jobs%ROWTYPE;
BEGIN
 -- Same project lock and count as HTTP job/workflow submission.
 PERFORM pg_advisory_xact_lock(hashtextextended(p::text,0));
 SELECT * INTO job FROM jobs WHERE id=j AND project_id=p FOR UPDATE;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF job.workflow_id IS NOT NULL OR job.status NOT IN ('CANCELLED','DEAD_LETTERED') OR job.attempt_no>=99997 THEN RETURN 'CONFLICT'; END IF;
 IF (SELECT count(*) FROM jobs WHERE project_id=p AND status IN ('PENDING','READY','RUNNING','RETRY_WAIT','CANCEL_REQUESTED'))>=10000 THEN RETURN 'QUOTA'; END IF;
 UPDATE jobs SET status='READY',max_attempts=attempt_no+3,worker_id=NULL,lease_until=NULL,deadline_at=NULL,result=NULL,error=NULL,finished_at=NULL,available_at=clock_timestamp() WHERE id=j;
 DELETE FROM dead_letter_jobs WHERE job_id=j;
 INSERT INTO audit_events(project_id,action,resource_id) VALUES(p,'job.retry',j); RETURN 'READY';
END $$;
COMMIT;
