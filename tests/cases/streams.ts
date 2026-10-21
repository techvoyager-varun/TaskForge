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
const shutdown = new AbortController();
const server = createApp(db, undefined, { shutdown: shutdown.signal }).listen(0, '127.0.0.1');
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
  shutdown.abort();
  let deadline: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      Promise.all(results.filter((r) => r.status === 200).map((r) => r.text())),
      new Promise((_, reject) => {
        deadline = setTimeout(() => reject(new Error('SSE streams did not end on shutdown')), 2000);
      }),
    ]);
  } finally {
    clearTimeout(deadline);
  }
  console.log('PASS: concurrent SSE connection quota and graceful stream shutdown');
} finally {
  controllers.forEach((c) => c.abort());
  server.closeAllConnections();
  await new Promise<void>((r) => server.close(() => r()));
  await db.end();
}
