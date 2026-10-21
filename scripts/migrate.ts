import 'dotenv/config';
import { readFile, readdir } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import pg from 'pg';
const client=new pg.Client({connectionString:process.env.ADMIN_DATABASE_URL??process.env.DATABASE_URL,connectionTimeoutMillis:5000});
await client.connect();
try {
  await client.query('SELECT pg_advisory_lock(8761234002)');
  await client.query('CREATE TABLE IF NOT EXISTS schema_migrations(name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())');
  const files=(await readdir('database/migrations')).filter(name=>/^\d+.*\.sql$/.test(name)).sort();
  for(const name of files){
    const sql=await readFile(`database/migrations/${name}`,'utf8');
    const checksum=createHash('sha256').update(sql).digest('hex');
    const previous=await client.query<{checksum:string}>('SELECT checksum FROM schema_migrations WHERE name=$1',[name]);
    if(previous.rows[0]) {
      if(previous.rows[0].checksum!==checksum)throw new Error(`Applied migration changed: ${name}`);
      continue;
    }
    // Each migration's BEGIN/COMMIT is replaced so schema + migration ledger commit atomically.
    await client.query('BEGIN');
    try {
      await client.query(sql.replace(/^BEGIN;\s*/,'').replace(/COMMIT;\s*$/,''));
      await client.query('INSERT INTO schema_migrations(name,checksum) VALUES($1,$2)',[name,checksum]);
      await client.query('COMMIT');
      console.log(`Applied ${name}`);
    }catch(error){await client.query('ROLLBACK');throw error;}
  }
}finally{await client.query('SELECT pg_advisory_unlock(8761234002)').catch(()=>{});await client.end();}
