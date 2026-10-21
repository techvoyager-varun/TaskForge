import { randomBytes } from 'node:crypto';
import { writeFileSync, existsSync, readFileSync } from 'node:fs';
const path = '.env';
let text = existsSync(path) ? readFileSync(path, 'utf8') : '';
for (const name of [
  'POSTGRES_PASSWORD',
  'API_DB_PASSWORD',
  'SCHEDULER_DB_PASSWORD',
  'WORKER_DB_PASSWORD',
  'REDIS_PASSWORD',
  'PUBLISHER_DB_PASSWORD',
  'CONSUMER_DB_PASSWORD',
]) {
  if (!new RegExp('^' + name + '=', 'm').test(text))
    text +=
      (text && !text.endsWith('\n') ? '\n' : '') +
      name +
      '=' +
      randomBytes(32).toString('hex') +
      '\n';
}
if (!/^HTTP_PORT=/m.test(text)) text += 'HTTP_PORT=3000\n';
writeFileSync(path, text, { mode: 0o600 });
console.log(
  'Preserved existing configuration and generated any missing independent service credentials.',
);
