import 'dotenv/config';
import pg from 'pg';
import { startConsumer, health } from './pipeline.js';
const namespace = process.env.PROJECTION_NAMESPACE ?? 'live';
if (
  namespace === 'live' &&
  process.env.KAFKA_GROUP &&
  process.env.KAFKA_GROUP !== 'taskforge-analytics-live-v1'
)
  throw new Error('The live namespace must use its stable group');
const db = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 6,
  connectionTimeoutMillis: 3000,
  statement_timeout: 5000,
});
db.on('error', () => {});
const runtime = await startConsumer(
  db,
  namespace,
  process.env.KAFKA_GROUP ?? 'taskforge-analytics-' + namespace + '-v1',
);
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  const timer = setTimeout(() => process.exit(1), 10000);
  timer.unref();
  await runtime.close();
  await health(db, 'consumer:' + namespace, false).catch(() => {});
  await db.end();
  clearTimeout(timer);
}
process.on('SIGTERM', () => void stop());
process.on('SIGINT', () => void stop());
