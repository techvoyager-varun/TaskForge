import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
import { createApp } from '../../apps/api/src/app.js';
const connection = { connectionString: testDatabase };
const db = new pg.Pool(connection);
const pool = new pg.Pool({ ...connection, options: '-c role=taskforge_api' });
const token = 'tf_' + randomBytes(32).toString('hex');
const project = (
  await db.query('INSERT INTO projects(name,token_hash) VALUES($1,$2) RETURNING id', [
    'workflow-check',
    createHash('sha256').update(token).digest('hex'),
  ])
).rows[0];
const queue = (
  await db.query(
    "INSERT INTO queues(project_id,name,concurrency_limit) VALUES($1,'default',4) RETURNING id",
    [project.id],
  )
).rows[0];
const server = createApp(pool).listen(0, '127.0.0.1');
await new Promise<void>((r) => server.once('listening', r));
const base = `http://127.0.0.1:${(server.address() as any).port}/api/v1`;
async function api(path: string, body?: unknown) {
  const r = await fetch(base + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { status: r.status, body: (await r.json()) as any };
}
async function terminal(id: string, status: string) {
  const end = Date.now() + 20000;
  while (Date.now() < end) {
    const x = await api('/workflows/' + id);
    if (x.body.data.status === status) return x.body.data;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('workflow did not reach ' + status);
}
try {
  const input = {
    name: 'diamond',
    queue_id: queue.id,
    nodes: ['A', 'B', 'C', 'D'].map((key) => ({
      key,
      type: 'sleep',
      payload: { duration_ms: 150 },
    })),
    edges: [
      { from: 'A', to: 'B' },
      { from: 'A', to: 'C' },
      { from: 'B', to: 'D' },
      { from: 'C', to: 'D' },
    ],
  };
  const created = await api('/workflows', input);
  assert.equal(created.status, 201);
  const id = created.body.data.id;
  await new Promise((r) => setTimeout(r, 400));
  assert.equal(
    (
      await db.query(
        'SELECT count(*)::integer AS n FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.workflow_id=$1',
        [id],
      )
    ).rows[0].n,
    0,
    'draft must never execute',
  );
  await api('/workflows/' + id + '/run', {});
  await terminal(id, 'COMPLETED');
  const order = await db.query(
    `SELECT p.node_key AS parent,c.node_key AS child,p.finished_at,a.assigned_at FROM workflow_edges e JOIN jobs p ON p.id=e.parent_id JOIN jobs c ON c.id=e.child_id JOIN job_attempts a ON a.job_id=c.id WHERE e.workflow_id=$1`,
    [id],
  );
  assert.equal(order.rows.length, 4);
  for (const edge of order.rows)
    assert.ok(edge.finished_at <= edge.assigned_at, edge.parent + ' preceded ' + edge.child);
  const retry = await api('/workflows/' + id + '/retry', {});
  assert.equal(retry.status, 201, 'workflow retry must create a fresh execution');
  assert.notEqual(retry.body.data.id, id);
  await api('/workflows/' + retry.body.data.id + '/run', {});
  await terminal(retry.body.data.id, 'COMPLETED');
  const history = await api('/workflows/' + id + '/executions');
  assert.equal(history.body.data.length, 2);
  const failed = await api('/workflows', {
    ...input,
    name: 'failing',
    nodes: input.nodes.map((node) =>
      node.key === 'A'
        ? { key: 'A', type: 'fail', payload: { message: 'expected' }, max_attempts: 1 }
        : node,
    ),
  });
  await api('/workflows/' + failed.body.data.id + '/run', {});
  const failure = await terminal(failed.body.data.id, 'FAILED');
  assert.equal(failure.nodes.filter((n: any) => n.status === 'CANCELLED').length, 3);
  const draft = await api('/workflows', input);
  await api('/workflows/' + draft.body.data.id + '/cancel', {});
  await terminal(draft.body.data.id, 'CANCELLED');
  console.log(
    'PASS: diamond ordering, draft isolation, retry lineage/history, failure propagation, workflow cancellation',
  );
} finally {
  await new Promise<void>((r) => server.close(() => r()));
  await pool.end();
  await db.end();
}
