import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { randomBytes, createHash } from 'node:crypto';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
import { createApp } from '../../apps/api/src/app.js';
const options = { connectionString: testDatabase };
const db = new pg.Client(options);
await db.connect();
const pool = new pg.Pool({ ...options, options: '-c role=taskforge_api' });
const token = 'tf_' + randomBytes(32).toString('hex');
const p = (
  await db.query(
    "INSERT INTO projects(name,token_hash) VALUES('cancel-lock-check',$1) RETURNING id",
    [createHash('sha256').update(token).digest('hex')],
  )
).rows[0];
const q = (
  await db.query("INSERT INTO queues(project_id,name) VALUES($1,'default') RETURNING id", [p.id])
).rows[0];
const f = (
  await db.query("INSERT INTO workflows(project_id,name) VALUES($1,'draft') RETURNING id", [p.id])
).rows[0];
await db.query(
  "INSERT INTO jobs(project_id,queue_id,workflow_id,node_key,type,status) VALUES($1,$2,$3,'A','noop','PENDING')",
  [p.id, q.id, f.id],
);
const server = createApp(pool).listen(0, '127.0.0.1');
await new Promise<void>((r) => server.once('listening', r));
let request: Promise<Response> | undefined;
try {
  await db.query('SELECT pg_advisory_lock(8761234001)');
  let completed = false;
  request = fetch(
    `http://127.0.0.1:${(server.address() as any).port}/api/v1/workflows/${f.id}/cancel`,
    {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: '{}',
    },
  ).then((r) => {
    completed = true;
    return r;
  });
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(
    completed,
    false,
    'workflow cancellation must wait for maintenance before taking any row locks',
  );
  await db.query('BEGIN');
  await db.query('SELECT id FROM workflows WHERE id=$1 FOR UPDATE NOWAIT', [f.id]);
  await db.query('SELECT id FROM jobs WHERE workflow_id=$1 FOR UPDATE NOWAIT', [f.id]);
  await db.query('COMMIT');
  await db.query('SELECT pg_advisory_unlock(8761234001)');
  assert.equal((await request).status, 202);
  console.log('PASS: cancellation serializes with maintenance before acquiring workflow/job rows');
} finally {
  await db.query('ROLLBACK').catch(() => {});
  await db.query('SELECT pg_advisory_unlock(8761234001)').catch(() => {});
  await request?.catch(() => {});
  await new Promise<void>((r) => server.close(() => r()));
  await pool.end();
  await db.end();
}
