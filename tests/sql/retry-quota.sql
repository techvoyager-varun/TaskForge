\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE p uuid;q uuid;j uuid;s text;
BEGIN
 INSERT INTO projects(name,token_hash) VALUES('retry-quota-check',md5(random()::text)||md5(random()::text)) RETURNING id INTO p;
 INSERT INTO queues(project_id,name) VALUES(p,'default') RETURNING id INTO q;
 INSERT INTO jobs(project_id,queue_id,type,status,available_at) SELECT p,q,'noop','PENDING',now()+interval '1 year' FROM generate_series(1,10000);
 INSERT INTO jobs(project_id,queue_id,type,status) VALUES(p,q,'noop','CANCELLED') RETURNING id INTO j;
 s=retry_job(p,j);IF s<>'QUOTA' THEN RAISE EXCEPTION 'retry bypassed project active-job quota: %',s;END IF;
 RAISE NOTICE 'PASS: manual retry respects active-job quota';
END $$;
ROLLBACK;
