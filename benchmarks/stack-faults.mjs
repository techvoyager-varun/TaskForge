import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const exec = promisify(execFile),
  credentials = { token: process.env.TASKFORGE_TOKEN, queue_id: process.env.TASKFORGE_QUEUE };
if (!credentials.token || !credentials.queue_id)
  throw new Error('Set TASKFORGE_TOKEN and TASKFORGE_QUEUE for a local disposable demo project');
const base = new URL(process.env.TASKFORGE_URL ?? 'http://127.0.0.1:3000').origin + '/api/v1';
const request = async (path, body) => {
  const controller = new AbortController(),
    timer = setTimeout(() => controller.abort(), 8000);
  try {
    const response = await fetch(base + path, {
      signal: controller.signal,
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: 'Bearer ' + credentials.token, 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const text = await response.text();
    return { status: response.status, body: text ? JSON.parse(text) : {} };
  } finally {
    clearTimeout(timer);
  }
};
const compose = async (...args) => {
  await exec('docker', ['compose', ...args], { timeout: 90000, maxBuffer: 200000 });
};
const until = async (fn, label, timeout = 60000) => {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const value = await fn();
    if (value) return value;
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(label + ' timed out');
};
const results = { measured_at: new Date().toISOString(), checks: [] };
try {
  const before = await until(async () => {
    try {
      const r = await request('/infrastructure');
      return r.status === 200 ? r : false;
    } catch {
      return false;
    }
  }, 'initial infrastructure readiness');
  assert.equal(before.status, 200);
  results.initial = before.body.data;
  await compose('stop', 'publisher');
  const job = (
    await request('/jobs', {
      queue_id: credentials.queue_id,
      type: 'sum',
      payload: { numbers: [2, 3, 7] },
    })
  ).body.data;
  await until(async () => {
    const r = await request('/jobs/' + job.id);
    return r.body.data?.status === 'COMPLETED';
  }, 'job during publisher stop');
  const stopped = await request('/infrastructure');
  assert.ok(stopped.body.data.kafka.outbox.pending >= 2);
  results.checks.push({
    name: 'publisher stopped; real job completes and outbox remains pending',
    pending: stopped.body.data.kafka.outbox.pending,
  });
  await compose('up', '-d', '--no-deps', 'publisher');
  await until(
    async () => (await request('/infrastructure')).body.data?.kafka.outbox.pending === 0,
    'publisher restart drains outbox',
  );
  results.checks.push({ name: 'publisher restart drains committed outbox', passed: true });
  await compose('stop', 'kafka');
  const offline = (
    await request('/jobs', {
      queue_id: credentials.queue_id,
      type: 'echo',
      payload: { message: 'broker outage durability' },
    })
  ).body.data;
  assert.ok(offline?.id);
  await until(
    async () => (await request('/jobs/' + offline.id)).body.data?.status === 'COMPLETED',
    'job while broker down',
  );
  assert.ok((await request('/infrastructure')).body.data.kafka.outbox.pending >= 2);
  results.checks.push({
    name: 'broker stopped; durable C++ job completes with pending events',
    passed: true,
  });
  await compose('up', '-d', '--no-deps', 'kafka');
  await until(
    async () => {
      const r = await request('/infrastructure');
      const d = r.body.data;
      return (
        d?.kafka.outbox.pending === 0 &&
        d.kafka.health.some((h) => h.component === 'consumer:live' && h.available && h.lag === '0')
      );
    },
    'broker reconnect and consumer catchup',
    90000,
  );
  results.checks.push({
    name: 'broker restart delivers pending events and consumer reaches zero lag',
    passed: true,
  });
  const durable = (
    await request('/jobs', {
      queue_id: credentials.queue_id,
      type: 'sleep',
      payload: { duration_ms: 1200 },
      timeout_ms: 30000,
    })
  ).body.data;
  await compose('stop', 'redis');
  assert.equal((await request('/project')).status, 503);
  await new Promise((r) => setTimeout(r, 1600));
  await compose('up', '-d', '--no-deps', 'redis');
  await until(async () => (await request('/project')).status === 200, 'Redis reconnect');
  await until(
    async () => (await request('/jobs/' + durable.id)).body.data?.status === 'COMPLETED',
    'acknowledged job survives Redis outage',
  );
  results.checks.push({
    name: 'Redis stop rejects admission; acknowledged job survives and API reconnects',
    passed: true,
  });
  const cached = await request('/jobs/' + job.id);
  assert.equal(cached.body.data.result.sum, 12);
  const workflow = await request('/workflows', {
    name: 'Full stack audit diamond',
    queue_id: credentials.queue_id,
    nodes: [
      { key: 'root', type: 'noop', payload: {} },
      { key: 'left', type: 'sum', payload: { numbers: [1, 2] } },
      { key: 'right', type: 'echo', payload: { message: 'parallel' } },
      { key: 'join', type: 'noop', payload: {} },
    ],
    edges: [
      { from: 'root', to: 'left' },
      { from: 'root', to: 'right' },
      { from: 'left', to: 'join' },
      { from: 'right', to: 'join' },
    ],
  });
  assert.equal(workflow.status, 201, JSON.stringify(workflow.body));
  const id = workflow.body.data.id;
  assert.equal((await request('/workflows/' + id + '/run', {})).status, 202);
  await until(
    async () => (await request('/workflows/' + id)).body.data?.status === 'COMPLETED',
    'full stack diamond',
  );
  results.checks.push({
    name: 'real diamond DAG completed through API and C++ processes',
    passed: true,
  });
  results.final = (await request('/infrastructure')).body.data;
  await writeFile(
    'benchmarks/results/2026-10-21-stack-faults.json',
    JSON.stringify(results, null, 2) + '\n',
  );
  console.log(JSON.stringify(results.checks, null, 2));
} finally {
  await compose('up', '-d', '--no-deps', 'redis', 'kafka', 'publisher', 'consumer').catch(() => {});
}
