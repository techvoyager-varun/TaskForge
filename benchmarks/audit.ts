import { cpus, totalmem, platform, release, arch, availableParallelism } from 'node:os';
import { writeFile, mkdir } from 'node:fs/promises';
import { once } from 'node:events';
import { randomBytes, createHash, randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import pg from 'pg';
import { createApp } from '../apps/api/src/app.js';
import { RedisLayer } from '../apps/api/src/redis.js';
import {
  kafka,
  ensureTopic,
  publishBatch,
  startConsumer,
  observedLag,
} from '../apps/events/src/pipeline.js';
import { testDatabase } from '../tests/support/config.js';
import { startEngine, stopProcess, until } from '../tests/support/processes.js';
// This script intentionally requires the disposable test database, never production credentials.
process.env.ADMIN_DATABASE_URL = testDatabase;
await import('../scripts/migrate.js');
const db = new pg.Pool({ connectionString: testDatabase, max: 10 });
const redis = new RedisLayer(process.env.TEST_REDIS_URL!, 'bench:' + randomUUID(), 100000, 60000);
await redis.start();
const server = createApp(db, undefined, { redis }).listen(0, '127.0.0.1');
await once(server, 'listening');
const base =
  'http://127.0.0.1:' + (server.address() as import('node:net').AddressInfo).port + '/api/v1';
const token = 'tf_' + randomBytes(32).toString('hex');
const p = (
  await db.query(
    "INSERT INTO projects(name,token_hash) VALUES('audit-benchmark',$1) RETURNING id",
    [createHash('sha256').update(token).digest('hex')],
  )
).rows[0];
const q = (
  await db.query(
    "INSERT INTO queues(project_id,name,concurrency_limit) VALUES($1,'benchmark',8) RETURNING id",
    [p.id],
  )
).rows[0];
const request = async (path: string, body?: unknown) => {
  const r = await fetch(base + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return { response: r, data: (await r.json()).data };
};
const percentile = (values: number[], p: number) =>
  [...values].sort((a, b) => a - b)[Math.max(0, Math.ceil(values.length * p) - 1)] ?? 0;
const distribution = (values: number[]) => ({
  samples: values.length,
  mean: values.reduce((a, b) => a + b, 0) / values.length,
  p50: percentile(values, 0.5),
  p95: percentile(values, 0.95),
  p99: percentile(values, 0.99),
});
const scenarios: Record<string, unknown>[] = [];
let engines: ReturnType<typeof startEngine>[] = [];
const start = (workers: number, schedulers: number) => {
  engines = [
    ...Array.from({ length: workers }, () => startEngine('worker', 4)),
    ...Array.from({ length: schedulers }, () => startEngine('scheduler')),
  ];
};
const stop = async () => {
  await Promise.all(engines.map((p) => stopProcess(p.child)));
  engines = [];
};
let producer: ReturnType<ReturnType<typeof kafka>['producer']> | undefined,
  admin: ReturnType<ReturnType<typeof kafka>['admin']> | undefined,
  consumer: Awaited<ReturnType<typeof startConsumer>> | undefined;
try {
  for (const [workers, schedulers] of [
    [1, 1],
    [2, 1],
    [1, 2],
    [2, 2],
  ] as const) {
    start(workers, schedulers);
    await new Promise((r) => setTimeout(r, 1200));
    const count = 80,
      submission: number[] = [],
      ids: string[] = [];
    let next = 0,
      maxLockWaiters = 0,
      samples = 0;
    const began = performance.now();
    const initialDeadlocks = Number(
      (await db.query('SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()'))
        .rows[0].deadlocks,
    );
    await Promise.all(
      Array.from({ length: 8 }, async () => {
        while (next < count) {
          const index = next++,
            t = performance.now();
          const { data } = await request('/jobs', {
            queue_id: q.id,
            type: 'sleep',
            payload: { duration_ms: 10 },
            timeout_ms: 30000,
          });
          ids[index] = data.id;
          submission.push(performance.now() - t);
        }
      }),
    );
    const submitSeconds = (performance.now() - began) / 1000;
    await until(
      async () => {
        const [done, locks] = await Promise.all([
          db.query(
            "SELECT count(*)::integer AS n FROM jobs WHERE id=ANY($1::uuid[]) AND status='COMPLETED'",
            [ids],
          ),
          db.query(
            "SELECT count(*)::integer AS n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'",
          ),
        ]);
        samples++;
        maxLockWaiters = Math.max(maxLockWaiters, locks.rows[0].n);
        return done.rows[0].n === count;
      },
      'benchmark completions',
      60000,
    );
    const wall = (performance.now() - began) / 1000;
    const rows = (
      await db.query(
        `SELECT j.attempt_no,extract(epoch FROM j.finished_at-j.created_at)*1000 AS completion_ms,extract(epoch FROM a.assigned_at-j.created_at)*1000 AS queue_wait_ms,extract(epoch FROM a.finished_at-a.started_at)*1000 AS execution_ms FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.attempt_no=1 WHERE j.id=ANY($1::uuid[])`,
        [ids],
      )
    ).rows;
    scenarios.push({
      workers,
      schedulers,
      worker_threads: 4,
      queue_limit: 8,
      jobs: count,
      handler: 'sleep',
      duration_ms: 10,
      submission_concurrency: 8,
      submission_seconds: submitSeconds,
      submission_jobs_per_second: count / submitSeconds,
      observed_wall_seconds: wall,
      observed_completion_jobs_per_second: count / wall,
      submission_latency_ms: distribution(submission),
      completion_latency_ms: distribution(rows.map((r) => Number(r.completion_ms))),
      queue_wait_ms: distribution(rows.map((r) => Number(r.queue_wait_ms))),
      execution_ms: distribution(rows.map((r) => Number(r.execution_ms))),
      extra_attempts: rows.reduce((n, r) => n + r.attempt_no - 1, 0),
      database_contention: {
        lock_wait_samples: samples,
        max_lock_waiters: maxLockWaiters,
        deadlocks_delta:
          Number(
            (
              await db.query(
                'SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()',
              )
            ).rows[0].deadlocks,
          ) - initialDeadlocks,
      },
    });
    await stop();
  }
  start(1, 1);
  await new Promise((r) => setTimeout(r, 1000));
  const large = (
    await request('/jobs', {
      queue_id: q.id,
      type: 'sum',
      payload: { numbers: Array.from({ length: 10000 }, (_, i) => i + 1) },
    })
  ).data;
  await until(
    async () =>
      (await db.query("SELECT 1 FROM jobs WHERE id=$1 AND status='COMPLETED'", [large.id]))
        .rowCount,
    'sum completion',
  );
  const warm = (await request('/jobs/' + large.id)).data;
  if (warm.result.sum !== 50005000) throw new Error('Wrong sum');
  const hot: number[] = [],
    cold: number[] = [];
  for (const [values, forceMiss] of [
    [hot, false],
    [cold, true],
  ] as const) {
    for (let i = 0; i < 100; i++) {
      if (forceMiss)
        await redis.command().del(redis.key(p.id, large.id, String(warm.state_version)));
      const t = performance.now();
      const r = await request('/jobs/' + large.id);
      if (r.response.headers.get('X-TaskForge-Cache') !== (forceMiss ? 'miss' : 'hit'))
        throw new Error('Cache sample classification failed');
      values.push(performance.now() - t);
    }
  }
  const cache = {
    body_bytes: Buffer.byteLength(JSON.stringify(warm)),
    hit_http_ms: distribution(hot),
    forced_miss_http_ms: distribution(cold),
    counters: redis.metrics(),
    caveat:
      'Sequential warm hits then forced misses; same API, shared Redis limiter and authentication. Deletion is outside timing. A hit still validates the current PostgreSQL version. No sustained capacity or universal speedup claim.',
  };
  const crash = (
    await request('/jobs', {
      queue_id: q.id,
      type: 'sleep',
      payload: { duration_ms: 2000 },
      max_attempts: 4,
      timeout_ms: 30000,
    })
  ).data;
  await until(
    async () =>
      (
        await db.query("SELECT 1 FROM job_attempts WHERE job_id=$1 AND status='RUNNING'", [
          crash.id,
        ])
      ).rowCount,
    'crash job begins',
  );
  const killed = performance.now();
  engines[0]!.child.kill('SIGKILL');
  engines.push(startEngine('worker', 4));
  await until(
    async () =>
      (
        await db.query(
          "SELECT attempt_no FROM jobs WHERE id=$1 AND status='COMPLETED' AND attempt_no>1",
          [crash.id],
        )
      ).rows[0],
    'recovery completion',
    30000,
  );
  const recovery = {
    kill_to_durable_completion_ms: performance.now() - killed,
    handler_ms: 2000,
    attempts: (await db.query('SELECT attempt_no FROM jobs WHERE id=$1', [crash.id])).rows[0]
      .attempt_no,
    samples: 1,
  };
  await stop();
  const stream = 'taskforge-benchmark-' + randomUUID(),
    namespace = 'bench_' + randomUUID().replaceAll('-', ''),
    client = kafka();
  admin = client.admin();
  await admin.connect();
  await ensureTopic(admin, stream);
  producer = client.producer({ idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();
  // Publish only this isolated project's events; pause other pending fixture events to avoid including them in throughput.
  await db.query(
    "UPDATE outbox_events SET next_attempt_at=now()+interval '1 hour' WHERE project_id IS DISTINCT FROM $1",
    [p.id],
  );
  const pending = Number(
    (
      await db.query(
        'SELECT count(*) FROM outbox_events WHERE project_id=$1 AND delivered_at IS NULL',
        [p.id],
      )
    ).rows[0].count,
  );
  let delivered = 0,
    batches = 0;
  const publishBegan = performance.now();
  while (true) {
    const batch = await publishBatch(db, producer, stream);
    if (!batch.claimed) break;
    delivered += batch.delivered;
    batches++;
  }
  const publishSeconds = (performance.now() - publishBegan) / 1000;
  if (delivered !== pending) throw new Error('Outbox did not drain');
  const consumeBegan = performance.now();
  consumer = await startConsumer(db, namespace, 'group-' + namespace, stream);
  await until(
    async () => (await observedLag(db, admin!, namespace, stream)) === 0,
    'benchmark projection catches up',
    30000,
  );
  const consumeSeconds = (performance.now() - consumeBegan) / 1000;
  const events = {
    events: pending,
    batches,
    publish_seconds: publishSeconds,
    publish_events_per_second: delivered / publishSeconds,
    consume_including_group_start_seconds: consumeSeconds,
    observed_consume_events_per_second: pending / consumeSeconds,
    final_observed_lag: await observedLag(db, admin, namespace, stream),
    topic_partitions: 3,
    replication_factor: 1,
    acks: 'all',
    caveat:
      'Single local broker; batched send includes PostgreSQL claim and post-ack persistence. Consumer includes group startup and 50ms polling.',
  };
  const result = {
    measured_at: new Date().toISOString(),
    machine: {
      platform: platform(),
      release: release(),
      arch: arch(),
      cpu: cpus()[0]?.model,
      logical_cores: availableParallelism(),
      visible_ram_gib: totalmem() / 1024 ** 3,
      node: process.version,
      scope: 'Docker Desktop Linux test container on Windows host',
    },
    configuration: {
      postgres: 17,
      redis: '7.4',
      kafka: '3.9.1',
      cxx: 'GCC 12 C++20 Debug',
      samples_per_worker_case: 80,
    },
    scenarios,
    cache,
    recovery,
    events,
    caveat:
      'Short serial observations on a shared developer laptop. No statistical significance, production capacity, exactly-once effects, or speedup claim. Database contention is sampled after submission; lock spikes can be missed.',
  };
  await mkdir('benchmarks/results', { recursive: true });
  await writeFile(
    'benchmarks/results/2026-10-21-audit.json',
    JSON.stringify(result, null, 2) + '\n',
  );
  console.log(JSON.stringify(result, null, 2));
} finally {
  await stop();
  await consumer?.close();
  await producer?.disconnect();
  await admin?.disconnect();
  redis.close();
  server.closeAllConnections();
  await new Promise<void>((r) => server.close(() => r()));
  await db.end();
}
