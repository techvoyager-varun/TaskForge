import 'dotenv/config';
import pg from 'pg';
import { z } from 'zod';
const days = z.coerce
  .number()
  .int()
  .min(7)
  .max(3650)
  .parse(process.env.RETENTION_DAYS ?? 30);
const apply = process.argv.includes('--apply');
const db = new pg.Client({
  connectionString: process.env.ADMIN_DATABASE_URL,
  connectionTimeoutMillis: 5000,
});
await db.connect();
try {
  await db.query('BEGIN');
  await db.query("SET LOCAL lock_timeout='2s'");
  const result = await db.query<{ id: string }>(
    `SELECT id FROM jobs WHERE workflow_id IS NULL AND status IN ('COMPLETED','CANCELLED','DEAD_LETTERED')
 AND finished_at<now()-$1::integer*interval '1 day' ORDER BY finished_at,id FOR UPDATE SKIP LOCKED LIMIT 500`,
    [days],
  );
  const ids = result.rows.map((row) => row.id);
  if (apply && ids.length) {
    await db.query('DELETE FROM execution_logs WHERE job_id=ANY($1::uuid[])', [ids]);
    await db.query('DELETE FROM job_attempts WHERE job_id=ANY($1::uuid[])', [ids]);
    await db.query('DELETE FROM dead_letter_jobs WHERE job_id=ANY($1::uuid[])', [ids]);
    await db.query('DELETE FROM audit_events WHERE resource_id=ANY($1::uuid[])', [ids]);
    await db.query('DELETE FROM jobs WHERE id=ANY($1::uuid[])', [ids]);
  }
  await db.query('COMMIT');
  console.log(
    JSON.stringify({
      mode: apply ? 'applied' : 'dry-run',
      eligible: ids.length,
      retention_days: days,
      batch_limit: 500,
      scope: 'terminal standalone jobs; workflows retained',
    }),
  );
} catch (error) {
  await db.query('ROLLBACK');
  throw error;
} finally {
  await db.end();
}
