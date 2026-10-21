import 'dotenv/config';
import pg from 'pg';
import { setTimeout as delay } from 'node:timers/promises';
import { kafka, ensureTopic, publishBatch, health } from './pipeline.js';
const db = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 3,
  connectionTimeoutMillis: 3000,
  statement_timeout: 5000,
});
db.on('error', () => {});
const client = kafka(),
  admin = client.admin(),
  producer = client.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    transactionTimeout: 10000,
  });
let stopping = false;
for (const signal of ['SIGTERM', 'SIGINT'])
  process.on(signal, () => {
    stopping = true;
  });
try {
  await admin.connect();
  await ensureTopic(admin);
  await producer.connect();
  while (!stopping) {
    try {
      await db.query('SELECT report_offline_workers()');
      const batch = await publishBatch(db, producer);
      if (!batch.claimed) {
        await admin.fetchTopicMetadata();
        await health(db, 'publisher', true);
        await delay(250);
      }
    } catch {
      console.error(JSON.stringify({ event: 'outbox.retry' }));
      await health(db, 'publisher', false).catch(() => {});
      await delay(1000);
    }
  }
} finally {
  await producer.disconnect().catch(() => {});
  await admin.disconnect().catch(() => {});
  await db.end();
}
