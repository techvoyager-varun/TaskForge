import 'dotenv/config';
import { randomBytes, createHash } from 'node:crypto';
import pg from 'pg';
import { z } from 'zod';
const id = z.uuid().parse(process.argv[2]);
const db = new pg.Client({
  connectionString: process.env.ADMIN_DATABASE_URL,
  connectionTimeoutMillis: 5000,
});
await db.connect();
try {
  const token = 'tf_' + randomBytes(32).toString('hex');
  const result = await db.query('UPDATE projects SET token_hash=$1 WHERE id=$2 RETURNING id', [
    createHash('sha256').update(token).digest('hex'),
    id,
  ]);
  if (!result.rowCount) throw new Error('Project not found');
  console.log(JSON.stringify({ project_id: id, token }, null, 2));
} finally {
  await db.end();
}
