import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
let createApp: any;
try {
  createApp = (await import('../../apps/api/src/app.ts')).createApp;
} catch {}
assert.equal(typeof createApp, 'function', 'Express application must be implemented');
const connection = { connectionString: testDatabase };
const admin = new pg.Pool(connection);
const pool = new pg.Pool({ ...connection, options: '-c role=taskforge_api' });
const token = 'tf_' + randomBytes(32).toString('hex');
const otherToken = 'tf_' + randomBytes(32).toString('hex');
const project = (
  await admin.query('INSERT INTO projects(name,token_hash) VALUES($1,$2) RETURNING id', [
    'api-check',
    createHash('sha256').update(token).digest('hex'),
  ])
).rows[0];
await admin.query('INSERT INTO projects(name,token_hash) VALUES($1,$2)', [
  'other-project',
  createHash('sha256').update(otherToken).digest('hex'),
]);
const server = createApp(pool).listen(0, '127.0.0.1');
await new Promise<void>((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}`;
async function call(path: string, body?: unknown, auth = token, extra = {}) {
  const response = await fetch(base + '/api/v1' + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Authorization: 'Bearer ' + auth, 'Content-Type': 'application/json', ...extra },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { status: response.status, body: (await response.json()) as any };
}
try {
  assert.equal((await fetch(base + '/api/v1/jobs')).status, 401);
  const queue = await call('/queues', { name: 'default', concurrency_limit: 2 });
  assert.equal(queue.status, 201);
  const input = { queue_id: queue.body.data.id, type: 'sum', payload: { numbers: [2, 3, 7] } };
  assert.equal(
    (await call('/jobs', input, token, { 'Idempotency-Key': '' })).status,
    400,
    'Empty idempotency header must be rejected before persistence',
  );
  const first = await call('/jobs', input, token, { 'Idempotency-Key': 'same-request' });
  assert.equal(first.status, 201);
  const duplicate = await call('/jobs', input, token, { 'Idempotency-Key': 'same-request' });
  assert.equal(duplicate.status, 200);
  assert.equal(duplicate.body.data.id, first.body.data.id);
  assert.equal(
    (
      await call('/jobs', { ...input, payload: { numbers: [9] } }, token, {
        'Idempotency-Key': 'same-request',
      })
    ).status,
    409,
  );
  assert.equal((await call('/jobs/' + first.body.data.id, undefined, otherToken)).status, 404);
  assert.equal((await call('/jobs/' + first.body.data.id + '/cancel', {}, otherToken)).status, 404);
  assert.equal(
    (await call('/jobs', { ...input, type: 'shell', payload: { command: 'whoami' } })).status,
    400,
  );
  assert.equal((await call('/jobs', { ...input, queue_id: 'bad' })).status, 400);
  assert.equal((await call('/jobs?limit=100000')).status, 400);
  let job: any;
  const deadline = Date.now() + 15000;
  do {
    job = await call('/jobs/' + first.body.data.id);
    if (job.body.data.status === 'COMPLETED') break;
    await new Promise((r) => setTimeout(r, 100));
  } while (Date.now() < deadline);
  assert.equal(job.body.data.status, 'COMPLETED');
  assert.deepEqual(job.body.data.result, { sum: 12 });
  assert.equal(job.body.data.attempts.length, 1);
  assert.ok((await call('/jobs/' + first.body.data.id + '/logs')).body.data.length >= 2);
  const workflow = await call('/workflows', {
    name: 'diamond',
    queue_id: queue.body.data.id,
    nodes: ['A', 'B', 'C', 'D'].map((key) => ({ key, type: 'noop' })),
    edges: [
      { from: 'A', to: 'B' },
      { from: 'A', to: 'C' },
      { from: 'B', to: 'D' },
      { from: 'C', to: 'D' },
    ],
  });
  assert.equal(workflow.status, 201);
  assert.equal((await call('/workflows/' + workflow.body.data.id + '/run', {})).status, 202);
  const cycle = await call('/workflows', {
    name: 'cycle',
    queue_id: queue.body.data.id,
    nodes: ['A', 'B'].map((key) => ({ key, type: 'noop' })),
    edges: [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'A' },
    ],
  });
  assert.equal(cycle.status, 400);
  console.log(
    'PASS: authenticated HTTP -> real C++ execution, tenant isolation, idempotency, validation, workflow creation',
  );
} finally {
  await new Promise<void>((r, e) => server.close((err: any) => (err ? e(err) : r())));
  await pool.end();
  await admin.end();
}
