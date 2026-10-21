BEGIN;
ALTER TABLE workflows ADD COLUMN root_workflow_id uuid;
ALTER TABLE workflows ADD CONSTRAINT workflow_root_project FOREIGN KEY(root_workflow_id,project_id) REFERENCES workflows(id,project_id);
ALTER TABLE workflows ADD CONSTRAINT workflow_root_not_self CHECK(root_workflow_id IS NULL OR root_workflow_id<>id);
CREATE INDEX workflows_history ON workflows(project_id,root_workflow_id,created_at DESC);
CREATE FUNCTION validate_workflow_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.status<>OLD.status AND NOT ((OLD.status='DRAFT' AND NEW.status IN ('RUNNING','CANCELLED')) OR
 (OLD.status='RUNNING' AND NEW.status IN ('COMPLETED','FAILED','CANCELLED'))) THEN
 RAISE EXCEPTION 'illegal workflow transition' USING ERRCODE='23514'; END IF;
 IF NEW.status IN ('COMPLETED','FAILED','CANCELLED') THEN NEW.finished_at=clock_timestamp(); END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_transition BEFORE UPDATE ON workflows FOR EACH ROW EXECUTE FUNCTION validate_workflow_transition();
REVOKE ALL ON FUNCTION validate_workflow_transition() FROM PUBLIC;
COMMIT;
