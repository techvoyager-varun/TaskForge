import { RedisLayer } from './redis.js';
import 'dotenv/config';
import { resolve } from 'node:path';
import pg from 'pg';
import { z } from 'zod';
import { createApp } from './app.js';
const config = z
  .object({
    DATABASE_URL: z.string().min(1),
    PORT: z.coerce.number().int().min(1).max(65535).default(3000),
    HOST: z.string().default('127.0.0.1'),
  })
  .parse(process.env);
const pool = new pg.Pool({
  connectionString: config.DATABASE_URL,
  max: 20,
  connectionTimeoutMillis: 3000,
  statement_timeout: 5000,
  idle_in_transaction_session_timeout: 5000,
});
pool.on('error', (error) =>
  console.error(JSON.stringify({ event: 'database.pool_error', message: error.message })),
);
const shutdownSignal = new AbortController();
const redis = process.env.REDIS_URL ? new RedisLayer(process.env.REDIS_URL) : undefined;
if (process.env.NODE_ENV === 'production' && !redis)
  throw new Error('REDIS_URL is required in production');
if (redis)
  void redis
    .start()
    .catch(() => console.error(JSON.stringify({ event: 'redis.initial_connection_failed' })));
const server = createApp(pool, resolve('dist/web'), {
  redis,
  shutdown: shutdownSignal.signal,
}).listen(config.PORT, config.HOST, () =>
  console.log(JSON.stringify({ event: 'api.started', port: config.PORT })),
);
let stopping = false;
function shutdown() {
  if (stopping) return;
  stopping = true;
  shutdownSignal.abort();
  redis?.close();
  const deadline = setTimeout(() => process.exit(1), 10000);
  deadline.unref();
  server.close(() => {
    void pool.end().then(() => {
      clearTimeout(deadline);
      process.exitCode = 0;
    });
  });
  server.closeIdleConnections();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
