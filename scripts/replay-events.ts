import 'dotenv/config';
import pg from 'pg';
import { kafka } from '../apps/events/src/pipeline.js';
import { envelope } from '../apps/events/src/events.js';
const topic = process.argv[2];
if (!topic || !/^taskforge\.replay\.[a-zA-Z0-9_-]{1,64}$/.test(topic))
  throw new Error('Provide a new taskforge.replay.<name> topic');
const db = new pg.Client({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 3000,
  statement_timeout: 10000,
});
const client = kafka(),
  admin = client.admin(),
  producer = client.producer({ idempotent: true, maxInFlightRequests: 1 });
try {
  await db.connect();
  await admin.connect();
  const fresh = await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic,
        numPartitions: 3,
        replicationFactor: 1,
        configEntries: [{ name: 'retention.ms', value: '604800000' }],
      },
    ],
  });
  if (!fresh) throw new Error('Replay requires a fresh topic; choose another name');
  await producer.connect();
  await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  const high = (
    await db.query('SELECT COALESCE(max(sequence_id),0)::text AS high FROM outbox_events')
  ).rows[0].high;
  let cursor = '0',
    sent = 0;
  while (true) {
    const batch = await db.query(
      'SELECT * FROM outbox_events WHERE sequence_id>$1 AND sequence_id<=$2 ORDER BY sequence_id LIMIT 100',
      [cursor, high],
    );
    if (!batch.rowCount) break;
    await producer.send({
      topic,
      acks: -1,
      messages: batch.rows.map((row) => ({
        key: row.aggregate_type + ':' + row.aggregate_id,
        value: JSON.stringify(envelope(row)),
      })),
    });
    sent += batch.rows.length;
    cursor = batch.rows.at(-1)!.sequence_id;
  }
  await db.query('COMMIT');
  console.log(
    JSON.stringify({
      topic,
      published: sent,
      high_water_sequence: high,
      note: 'Connect a new consumer group and projection namespace. Live delivery markers were not modified. This is a consistent snapshot of committed retained events, not a live subscription.',
    }),
  );
} finally {
  await producer.disconnect().catch(() => {});
  await admin.disconnect().catch(() => {});
  await db.end();
}
