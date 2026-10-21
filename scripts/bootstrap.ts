import './migrate.js';
import 'dotenv/config';
import pg from 'pg';
const db = new pg.Client({
  connectionString: process.env.ADMIN_DATABASE_URL,
  connectionTimeoutMillis: 5000,
});
await db.connect();
try {
  for (const [name, role, variable] of [
    ['taskforge_api_login', 'taskforge_api', 'API_DB_PASSWORD'],
    ['taskforge_scheduler_login', 'taskforge_scheduler', 'SCHEDULER_DB_PASSWORD'],
    ['taskforge_worker_login', 'taskforge_worker', 'WORKER_DB_PASSWORD'],
    ['taskforge_publisher_login', 'taskforge_publisher', 'PUBLISHER_DB_PASSWORD'],
    ['taskforge_consumer_login', 'taskforge_consumer', 'CONSUMER_DB_PASSWORD'],
  ] as const) {
    const password = process.env[variable];
    if (!password || !/^[a-f0-9]{64}$/.test(password))
      throw new Error(`${variable} must be 64 hexadecimal characters; run scripts/setup-env.mjs`);
    const exists = await db.query('SELECT 1 FROM pg_roles WHERE rolname=$1', [name]);
    // Names are fixed above and the credential is strictly hex, not arbitrary SQL input.
    if (!exists.rowCount) await db.query(`CREATE ROLE ${name} LOGIN IN ROLE ${role}`);
    await db.query(`ALTER ROLE ${name} PASSWORD '${password}'`);
  }
  console.log('Provisioned independent least-privilege service logins.');
} finally {
  await db.end();
}
