import 'dotenv/config';
import { createHash, randomBytes } from 'node:crypto';
import pg from 'pg';
const name = process.argv[2]?.trim();
if (!name || name.length > 100) throw new Error('Usage: npm run project:create -- "Project name"');
const db = new pg.Client({
  connectionString: process.env.ADMIN_DATABASE_URL ?? process.env.DATABASE_URL,
  connectionTimeoutMillis: 5000,
});
await db.connect();
const token = 'tf_' + randomBytes(32).toString('hex');
try {
  await db.query('BEGIN');
  const {
    rows: [project],
  } = await db.query<{ id: string }>(
    'INSERT INTO projects(name,token_hash) VALUES($1,$2) RETURNING id',
    [name, createHash('sha256').update(token).digest('hex')],
  );
  const {
    rows: [queue],
  } = await db.query<{ id: string }>(
    "INSERT INTO queues(project_id,name) VALUES($1,'default') RETURNING id",
    [project!.id],
  );
  await db.query('COMMIT');
  console.log(JSON.stringify({ project_id: project!.id, queue_id: queue!.id, token }, null, 2));
} catch (error) {
  await db.query('ROLLBACK');
  throw error;
} finally {
  await db.end();
}
