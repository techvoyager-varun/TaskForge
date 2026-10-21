\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE p uuid;q uuid;j uuid;w uuid:=gen_random_uuid();a record;s text;
BEGIN
 INSERT INTO projects(name,token_hash) VALUES('fault-check',md5(random()::text)||md5(random()::text)) RETURNING id INTO p;
 INSERT INTO queues(project_id,name,concurrency_limit) VALUES(p,'default',1) RETURNING id INTO q;
 INSERT INTO workers(id,name,capacity) VALUES(w,'fault-check',1);
 INSERT INTO jobs(project_id,queue_id,type,payload) VALUES(p,q,'noop','{}') RETURNING id INTO j;
 SELECT * INTO a FROM dispatch_job(w);
 UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=j;
 IF finish_job(j,w,a.attempt_no,'COMPLETED','{}',NULL) THEN RAISE EXCEPTION 'expired lease accepted before reassignment'; END IF;
 PERFORM maintain_jobs();
 IF (SELECT status FROM jobs WHERE id=j)<>'RETRY_WAIT' THEN RAISE EXCEPTION 'expired lease was not recovered';END IF;
 UPDATE jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=j;
 PERFORM maintain_jobs(); SELECT * INTO a FROM dispatch_job(w);
 IF a.attempt_no<>2 THEN RAISE EXCEPTION 'fencing did not increase';END IF;
 IF finish_job(j,w,1,'COMPLETED','{}',NULL) THEN RAISE EXCEPTION 'stale completion overwrote retry';END IF;
 s=cancel_job(p,j); IF s<>'CANCEL_REQUESTED' THEN RAISE EXCEPTION 'running cancellation not requested';END IF;
 s=renew_lease(j,w,2);IF s<>'CANCEL_REQUESTED' THEN RAISE EXCEPTION 'worker did not receive cancellation';END IF;
 IF NOT finish_job(j,w,2,'COMPLETED','{}',NULL) THEN RAISE EXCEPTION 'cancel race was not resolved'; END IF;
 IF (SELECT status FROM jobs WHERE id=j)<>'CANCELLED' THEN RAISE EXCEPTION 'cancelled job completed';END IF;
 s=retry_job(p,j);IF s<>'READY' THEN RAISE EXCEPTION 'manual retry failed';END IF;
 SELECT * INTO a FROM dispatch_job(w);IF a.attempt_no<>3 THEN RAISE EXCEPTION 'manual retry reset fencing';END IF;
 PERFORM finish_job(j,w,3,'COMPLETED','{}',NULL);
 INSERT INTO jobs(project_id,queue_id,type,payload,max_attempts,timeout_ms) VALUES(p,q,'noop','{}',1,100) RETURNING id INTO j;
 SELECT * INTO a FROM dispatch_job(w);
 UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second',deadline_at=clock_timestamp()-interval '1 second' WHERE id=j;
 PERFORM maintain_jobs();
 IF (SELECT status FROM jobs WHERE id=j)<>'DEAD_LETTERED' THEN RAISE EXCEPTION 'timeout was not exhausted';END IF;
 IF (SELECT status FROM job_attempts WHERE job_id=j AND attempt_no=1)<>'TIMED_OUT' THEN RAISE EXCEPTION 'timeout history lost';END IF;
 IF NOT EXISTS(SELECT FROM dead_letter_jobs WHERE job_id=j) THEN RAISE EXCEPTION 'dead-letter record missing';END IF;
 RAISE NOTICE 'PASS: expired/stale fencing, recovery, cancellation race, manual retry, timeout exhaustion';
END $$;
ROLLBACK;
