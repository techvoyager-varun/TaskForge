import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { createServer, connect, type Socket, type AddressInfo } from 'node:net';
import { once } from 'node:events';
import pg from 'pg';
import { RedisLayer } from '../../apps/api/src/redis.js';
import { createApp } from '../../apps/api/src/app.js';
import { testDatabase } from '../support/config.js';
import { until } from '../support/processes.js';

test(
  'Redis shares atomic rate limits across replicas and rejects stale/foreign cache, outages and reconnects',
  { timeout: 30000 },
  async () => {
    assert.ok(process.env.TEST_REDIS_URL, 'TEST_REDIS_URL must configure isolated Redis');
    const target = new URL(process.env.TEST_REDIS_URL);
    let offline = false;
    const sockets = new Set<Socket>();
    const proxy = createServer((client) => {
      if (offline) {
        client.destroy();
        return;
      }
      const upstream = connect(Number(target.port || 6379), target.hostname);
      sockets.add(client);
      sockets.add(upstream);
      client.pipe(upstream);
      upstream.pipe(client);
      for (const s of [client, upstream]) {
        s.on('error', () => {
          client.destroy();
          upstream.destroy();
        });
        s.on('close', () => {
          sockets.delete(s);
          client.destroy();
          upstream.destroy();
        });
      }
    });
    proxy.listen(0, '127.0.0.1');
    await once(proxy, 'listening');
    const uri = new URL(target);
    uri.hostname = '127.0.0.1';
    uri.port = String((proxy.address() as AddressInfo).port);
    const prefix = 'test:' + randomUUID(),
      a = new RedisLayer(uri.toString(), prefix, 20, 5000),
      b = new RedisLayer(uri.toString(), prefix, 20, 5000);
    const db = new pg.Pool({ connectionString: testDatabase });
    const servers: ReturnType<ReturnType<typeof createApp>['listen']>[] = [];
    try {
      process.env.ADMIN_DATABASE_URL = testDatabase;
      await import('../../scripts/migrate.js');
      await Promise.all([a.start(), b.start()]);
      const token = 'tf_' + randomBytes(32).toString('hex');
      const p = (
        await db.query('INSERT INTO projects(name,token_hash) VALUES($1,$2) RETURNING id', [
          'redis-test',
          createHash('sha256').update(token).digest('hex'),
        ])
      ).rows[0];
      const q = (
        await db.query("INSERT INTO queues(project_id,name) VALUES($1,'cache') RETURNING id", [
          p.id,
        ])
      ).rows[0];
      const job = (
        await db.query(
          "INSERT INTO jobs(project_id,queue_id,type) VALUES($1,$2,'noop') RETURNING *",
          [p.id, q.id],
        )
      ).rows[0];
      const addresses: string[] = [];
      for (const layer of [a, b]) {
        const server = createApp(db, undefined, { redis: layer }).listen(0, '127.0.0.1');
        servers.push(server);
        await once(server, 'listening');
        addresses.push('http://127.0.0.1:' + (server.address() as AddressInfo).port + '/api/v1');
      }
      const get = async (path: string, replica = 0) =>
        fetch(addresses[replica] + path, { headers: { Authorization: 'Bearer ' + token } });
      let r = await get('/jobs/' + job.id);
      assert.equal(r.headers.get('X-TaskForge-Cache'), 'miss');
      r = await get('/jobs/' + job.id, 1);
      assert.equal(r.headers.get('X-TaskForge-Cache'), 'hit');
      assert.equal((await r.json()).data.status, 'READY');
      await db.query('SELECT cancel_job($1,$2)', [p.id, job.id]);
      r = await get('/jobs/' + job.id);
      assert.equal(r.headers.get('X-TaskForge-Cache'), 'miss');
      const cancelled = (await r.json()).data;
      assert.equal(cancelled.status, 'CANCELLED');
      await a
        .command()
        .set(
          a.key(p.id, job.id, String(cancelled.state_version)),
          JSON.stringify({ ...cancelled, project_id: randomUUID() }),
        );
      r = await get('/jobs/' + job.id);
      assert.equal(r.headers.get('X-TaskForge-Cache'), 'miss');
      assert.equal((await r.json()).data.project_id, p.id);
      const results = await Promise.all(
        Array.from({ length: 36 }, (_, i) => get('/project', i % 2)),
      );
      assert.equal(results.filter((r) => r.status === 200).length, 16);
      assert.equal(results.filter((r) => r.status === 429).length, 20);
      // Exercise the real Redis client transport and automatic reconnect, not a mocked command.
      offline = true;
      for (const socket of sockets) socket.destroy();
      await until(
        async () => !a.client.isReady && !b.client.isReady,
        'Redis clients notice outage',
      );
      const began = Date.now();
      assert.equal((await get('/project')).status, 503);
      assert.ok(Date.now() - began < 1500);
      assert.equal(
        await a.get(p.id, job.id, String(cancelled.state_version)),
        null,
        'cache must bypass Redis failure',
      );
      offline = false;
      await until(async () => a.client.isReady && b.client.isReady, 'Redis reconnect');
      await new Promise((r) => setTimeout(r, 5100));
      assert.equal((await get('/project')).status, 200);
      assert.equal(
        (await a.command().configGet('maxmemory-policy'))['maxmemory-policy'],
        'noeviction',
      );
      await a.command().configSet('maxmemory', '1');
      try {
        assert.equal(
          (await get('/project')).status,
          503,
          'memory exhaustion must fail closed instead of evicting admission counters',
        );
      } finally {
        await a.command().configSet('maxmemory', '67108864');
      }
      assert.ok(a.counters.errors > 0);
      assert.equal(b.counters.hits, 1);
      assert.ok(a.counters.misses >= 3);
    } finally {
      for (const s of servers) {
        s.closeAllConnections();
        await new Promise<void>((r) => s.close(() => r()));
      }
      a.close();
      b.close();
      for (const s of sockets) s.destroy();
      await new Promise<void>((r) => proxy.close(() => r()));
      await db.end();
    }
  },
);
