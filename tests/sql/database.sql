\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE p uuid; q uuid; j uuid; a record; w uuid := gen_random_uuid();
BEGIN
 INSERT INTO projects(name, token_hash) VALUES ('local-check',repeat('a',64)) RETURNING id INTO p;
 INSERT INTO queues(project_id,name,concurrency_limit) VALUES (p,'default',1) RETURNING id INTO q;
 INSERT INTO workers(id,name,capacity) VALUES(w,'local-check',2);
 INSERT INTO jobs(project_id,queue_id,type,payload,status) VALUES(p,q,'echo','{"value":"ok"}','READY') RETURNING id INTO j;
 SELECT * INTO a FROM dispatch_job(w);
 IF a.job_id IS DISTINCT FROM j OR a.attempt_no <> 1 THEN RAISE EXCEPTION 'job was not leased'; END IF;
 IF EXISTS(SELECT 1 FROM dispatch_job(w)) THEN RAISE EXCEPTION 'queue concurrency exceeded'; END IF;
 IF finish_job(j, w, 99, 'COMPLETED', '{}', NULL) THEN RAISE EXCEPTION 'stale token accepted'; END IF;
 IF NOT finish_job(j,w,1,'COMPLETED','{"value":"ok"}',NULL) THEN RAISE EXCEPTION 'current lease completion rejected'; END IF;
 IF (SELECT status FROM jobs WHERE id=j) <> 'COMPLETED' THEN RAISE EXCEPTION 'result was not durable'; END IF;
 BEGIN
   UPDATE jobs SET status='RUNNING' WHERE id=j;
   RAISE EXCEPTION 'illegal transition accepted';
 EXCEPTION WHEN check_violation THEN NULL;
 END;
 RAISE NOTICE 'PASS: lease, concurrency, fencing, completion, transitions';
END $$;
ROLLBACK;
