import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { testDatabase } from '../support/config.js';
import { createApp } from '../../apps/api/src/app.js';
const db = new pg.Pool({ connectionString: testDatabase, max: 40 });
const token = 'tf_' + randomBytes(32).toString('hex');
await db.query("INSERT INTO projects(name,token_hash) VALUES('stream-check',$1)", [
  createHash('sha256').update(token).digest('hex'),
]);
const server = createApp(db).listen(0, '127.0.0.1');
await new Promise<void>((r) => server.once('listening', r));
const controllers = Array.from({ length: 11 }, () => new AbortController());
try {
  const results = await Promise.all(
    controllers.map((c) =>
      fetch(`http://127.0.0.1:${(server.address() as any).port}/api/v1/events`, {
        headers: { Authorization: 'Bearer ' + token },
        signal: c.signal,
      }),
    ),
  );
  assert.equal(
    results.filter((r) => r.status === 200).length,
    10,
    'concurrent requests must reserve only ten stream slots',
  );
  assert.equal(results.filter((r) => r.status === 429).length, 1);
  console.log('PASS: concurrent SSE connection quota');
} finally {
  controllers.forEach((c) => c.abort());
  server.closeAllConnections();
  await new Promise<void>((r) => server.close(() => r()));
  await db.end();
}
