import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
import { createApp } from '../../apps/api/src/app.js';
const db = new pg.Pool({ connectionString: testDatabase });
const token = 'tf_' + randomBytes(32).toString('hex');
const p = (
  await db.query(
    "INSERT INTO projects(name,token_hash) VALUES('pagination-check',$1) RETURNING id",
    [createHash('sha256').update(token).digest('hex')],
  )
).rows[0];
const q = (
  await db.query("INSERT INTO queues(project_id,name) VALUES($1,'default') RETURNING id", [p.id])
).rows[0];
await db.query(
  "INSERT INTO jobs(project_id,queue_id,type,created_at) SELECT $1,$2,'noop','2026-10-20T00:00:00.123456Z'::timestamptz+g*interval '1 microsecond' FROM generate_series(1,20) g",
  [p.id, q.id],
);
const server = createApp(db).listen(0, '127.0.0.1');
await new Promise<void>((r) => server.once('listening', r));
try {
  let cursor: string | null = null;
  const ids: string[] = [];
  do {
    const r = await fetch(
      `http://127.0.0.1:${(server.address() as any).port}/api/v1/jobs?limit=5` +
        (cursor ? '&cursor=' + cursor : ''),
      { headers: { Authorization: 'Bearer ' + token } },
    );
    assert.equal(r.status, 200);
    const body = (await r.json()) as any;
    ids.push(...body.data.map((j: any) => j.id));
    cursor = body.next_cursor;
  } while (cursor);
  assert.equal(ids.length, 20, 'pagination must preserve microsecond timestamps');
  assert.equal(new Set(ids).size, 20);
  console.log('PASS: microsecond keyset pagination without skips or duplicates');
} finally {
  await new Promise<void>((r) => server.close(() => r()));
  await db.end();
}
