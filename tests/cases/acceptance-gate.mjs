import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
const options = { connectionString: testDatabase };
const a = new pg.Client(options),
  b = new pg.Client(options);
await a.connect();
await b.connect();
const gate = '8761234998';
try {
  await a.query('BEGIN');
  const p = (
    await a.query(
      "INSERT INTO projects(name,token_hash) VALUES('acceptance-gate-check',md5(random()::text)||md5(random()::text)) RETURNING id",
    )
  ).rows[0];
  const q = (
    await a.query("INSERT INTO queues(project_id,name) VALUES($1,'default') RETURNING id", [p.id])
  ).rows[0];
  const w = (
    await a.query(
      "INSERT INTO workers(id,name,capacity) VALUES(gen_random_uuid(),'acceptance-gate-check',1) RETURNING id",
    )
  ).rows[0];
  const j = (
    await a.query(
      "INSERT INTO jobs(project_id,queue_id,type,max_attempts) VALUES($1,$2,'noop',1) RETURNING id",
      [p.id, q.id],
    )
  ).rows[0];
  await a.query('SELECT * FROM dispatch_job($1)', [w.id]);
  await a.query('UPDATE workers SET draining=true WHERE id=$1', [w.id]);
  await a.query('COMMIT');
  await a.query(
    `CREATE OR REPLACE FUNCTION qa_acceptance_gate(t timestamptz) RETURNS timestamptz LANGUAGE plpgsql VOLATILE AS $$ BEGIN PERFORM pg_advisory_xact_lock(${gate}); RETURN t; END $$`,
  );
  let definition = (
    await a.query("SELECT pg_get_functiondef('next_assignment(uuid)'::regprocedure) AS sql")
  ).rows[0].sql;
  definition = definition
    .replace('FUNCTION public.next_assignment(', 'FUNCTION public.qa_next_assignment(')
    .replace('ORDER BY x.created_at', 'ORDER BY qa_acceptance_gate(x.created_at)');
  assert.match(definition, /qa_next_assignment/);
  assert.match(definition, /ORDER BY qa_acceptance_gate/);
  // Exact current function body with only a local timing gate before LockRows.
  await a.query(definition);
  await a.query('SELECT pg_advisory_lock($1::bigint)', [gate]);
  const pid = (await b.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  const waiting = b.query('SELECT * FROM qa_next_assignment($1)', [w.id]);
  let blocked = false;
  const end = Date.now() + 3000;
  while (Date.now() < end) {
    const lock = await a.query(
      "SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted",
      [pid],
    );
    if (lock.rowCount) {
      blocked = true;
      break;
    }
    await new Promise((r) => setTimeout(r, 20));
  }
  assert.ok(blocked, 'second lane must pause after taking its stale SELECT snapshot');
  assert.equal((await a.query('SELECT * FROM next_assignment($1)', [w.id])).rows.length, 1);
  await a.query('SELECT pg_advisory_unlock($1::bigint)', [gate]);
  const duplicate = await waiting;
  assert.equal(
    duplicate.rows.length,
    0,
    'stale join snapshot must not return an already accepted current attempt',
  );
  console.log('PASS: deterministic READ COMMITTED acceptance race is rejected');
} finally {
  await a.query('SELECT pg_advisory_unlock($1::bigint)', [gate]).catch(() => {});
  await a.query('DROP FUNCTION IF EXISTS qa_next_assignment(uuid)').catch(() => {});
  await a.query('DROP FUNCTION IF EXISTS qa_acceptance_gate(timestamptz)').catch(() => {});
  await a.end();
  await b.end();
}
