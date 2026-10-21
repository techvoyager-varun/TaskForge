import test, { before, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
import { startEngine, stopProcess, until } from '../support/processes.js';
const exec = promisify(execFile);
before(async () => {
  process.env.ADMIN_DATABASE_URL = testDatabase;
  await import('../../scripts/migrate.js');
});
beforeEach(async () => {
  const db = new pg.Client({ connectionString: testDatabase });
  await db.connect();
  try {
    await db.query('TRUNCATE projects,workers CASCADE');
  } finally {
    await db.end();
  }
});
for (const [name, file, engine] of [
  ['HTTP validation, idempotency, tenant isolation and persisted C++ result', 'api.ts', true],
  ['DAG ordering, parallel predecessors, retry lineage and cancellation', 'workflows.ts', true],
  ['microsecond keyset pagination', 'pagination.ts', false],
  ['concurrent stream connection quota', 'streams.ts', false],
  ['workflow cancellation lock protocol', 'cancel-lock.ts', false],
  ['deterministic stale joined-snapshot assignment race', 'acceptance-gate.mjs', false],
] as const) {
  test(name, { timeout: 60000 }, async () => {
    const processes = engine
      ? [startEngine('worker'), startEngine('scheduler'), startEngine('scheduler')]
      : [];
    try {
      await exec(process.execPath, ['--import', 'tsx', 'tests/cases/' + file], {
        env: { ...process.env, LOG_LEVEL: 'silent' },
        timeout: 50000,
        maxBuffer: 100000,
      });
    } catch (error) {
      for (const p of processes) console.error(p.output());
      throw error;
    } finally {
      await Promise.all(processes.map((p) => stopProcess(p.child)));
    }
  });
}
for (const file of ['database.sql', 'reliability.sql', 'retry-quota.sql'])
  test('PostgreSQL invariants: ' + file, async () => {
    const db = new pg.Client({ connectionString: testDatabase });
    await db.connect();
    try {
      await db.query((await readFile('tests/sql/' + file, 'utf8')).replace(/^\\set.*\n/gm, ''));
    } finally {
      await db.query('ROLLBACK').catch(() => {});
      await db.end();
    }
  });
test(
  'two schedulers and two workers honor queue capacity and recover a killed worker',
  { timeout: 60000 },
  async () => {
    const db = new pg.Client({ connectionString: testDatabase });
    await db.connect();
    const processes: ReturnType<typeof startEngine>[] = [];
    try {
      const p = (
        await db.query(
          "INSERT INTO projects(name,token_hash) VALUES('process-test',md5(random()::text)||md5(random()::text)) RETURNING id",
        )
      ).rows[0];
      const q = (
        await db.query(
          "INSERT INTO queues(project_id,name,concurrency_limit) VALUES($1,'default',2) RETURNING id",
          [p.id],
        )
      ).rows[0];
      processes.push(startEngine('worker'), startEngine('scheduler'), startEngine('scheduler'));
      const crash = (
        await db.query(
          "INSERT INTO jobs(project_id,queue_id,type,payload,max_attempts) VALUES($1,$2,'sleep','{\"duration_ms\":1000}',5) RETURNING id",
          [p.id, q.id],
        )
      ).rows[0];
      await until(
        async () =>
          (
            await db.query("SELECT 1 FROM job_attempts WHERE job_id=$1 AND status='RUNNING'", [
              crash.id,
            ])
          ).rowCount,
        'handler start',
      );
      processes[0]!.child.kill('SIGKILL');
      await new Promise((r) => setTimeout(r, 10500));
      processes.push(startEngine('worker'), startEngine('worker'));
      await until(
        async () =>
          (
            await db.query(
              "SELECT 1 FROM jobs WHERE id=$1 AND status='COMPLETED' AND attempt_no>1",
              [crash.id],
            )
          ).rowCount,
        'crashed job recovery',
      );
      const jobs = (
        await db.query(
          "INSERT INTO jobs(project_id,queue_id,type,payload) SELECT $1,$2,'sleep','{\"duration_ms\":100}' FROM generate_series(1,24) RETURNING id",
          [p.id, q.id],
        )
      ).rows;
      let max = 0;
      await until(async () => {
        const row = (
          await db.query(
            "SELECT count(*) FILTER(WHERE status='RUNNING')::integer AS active,count(*) FILTER(WHERE status='COMPLETED')::integer AS done FROM jobs WHERE id=ANY($1::uuid[])",
            [jobs.map((j) => j.id)],
          )
        ).rows[0];
        assert.ok(row.active <= 2);
        max = Math.max(max, row.active);
        return row.done === 24;
      }, 'parallel batch');
      assert.equal(max, 2);
      assert.equal(
        (
          await db.query(
            'SELECT count(*)::integer AS n FROM job_attempts WHERE job_id=ANY($1::uuid[])',
            [jobs.map((j) => j.id)],
          )
        ).rows[0].n,
        24,
      );
    } finally {
      await Promise.all(processes.map((p) => stopProcess(p.child)));
      await db.end();
    }
  },
);

test(
  'libpq bounds an established connection whose peer accepts but never answers',
  { timeout: 15000 },
  async () => {
    const { createServer, connect } = await import('node:net');
    const { spawn } = await import('node:child_process');
    const { once } = await import('node:events');
    const { resolve } = await import('node:path');
    const destination = new URL(testDatabase!);
    let blackhole = false;
    const sockets = new Set<import('node:net').Socket>();
    const proxy = createServer((client) => {
      const upstream = connect(Number(destination.port || 5432), destination.hostname);
      sockets.add(client);
      sockets.add(upstream);
      client.on('data', (data) => {
        if (!blackhole) upstream.write(data);
      });
      upstream.on('data', (data) => {
        if (!blackhole) client.write(data);
      });
      for (const socket of [client, upstream]) {
        socket.on('error', () => {
          client.destroy();
          upstream.destroy();
        });
        socket.on('close', () => {
          sockets.delete(socket);
          client.destroy();
          upstream.destroy();
        });
      }
    });
    proxy.listen(0, '127.0.0.1');
    await once(proxy, 'listening');
    const uri = new URL(testDatabase!);
    uri.hostname = '127.0.0.1';
    uri.port = String((proxy.address() as import('node:net').AddressInfo).port);
    const child = spawn(
      resolve(process.env.TEST_CPP_DIR ?? 'build', 'taskforge-database-probe'),
      [],
      { env: { ...process.env, DATABASE_URL: uri.toString() }, stdio: ['pipe', 'pipe', 'pipe'] },
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    try {
      await until(async () => child.stdout.readableLength > 0, 'probe connection', 5000);
      const initial = child.stdout.read()?.toString();
      assert.match(initial ?? '', /connected/);
      blackhole = true;
      const began = Date.now();
      const exited = once(child, 'exit');
      child.stdin.write('\n');
      timer = setTimeout(() => {
        timedOut = true;
        child.kill('SIGKILL');
      }, 8000);
      const [code] = await exited;
      assert.equal(
        timedOut,
        false,
        'database I/O must have a client-side deadline, even when TCP stays connected',
      );
      assert.equal(code, 0);
      assert.ok(Date.now() - began < 8000);
    } finally {
      clearTimeout(timer);
      child.kill('SIGKILL');
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((r) => proxy.close(() => r()));
    }
  },
);
