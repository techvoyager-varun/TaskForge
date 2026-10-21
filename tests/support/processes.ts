import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { resolve } from 'node:path';
import { testDatabase } from './config.js';
export function startEngine(kind: 'worker' | 'scheduler', slots = 4) {
  const uri = new URL(testDatabase!);
  uri.searchParams.set('options', '-c role=taskforge_' + kind);
  uri.searchParams.set('connect_timeout', '3');
  const child = spawn(resolve(process.env.TEST_CPP_DIR ?? 'build', 'taskforge-' + kind), [], {
    env: {
      ...process.env,
      DATABASE_URL: uri.toString().replace(/\+/g, '%20'),
      WORKER_CONCURRENCY: String(slots),
      WORKER_NAME: 'integration-worker',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  for (const stream of [child.stdout, child.stderr])
    stream?.on('data', (chunk) => {
      output = (output + chunk.toString()).slice(-16000);
    });
  child.on('error', (error) => {
    output += '\n' + error.message;
  });
  return { child, output: () => output };
}
export async function stopProcess(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const done = once(child, 'exit');
  child.kill('SIGTERM');
  const timer = setTimeout(() => child.kill('SIGKILL'), 6000);
  try {
    await done;
  } finally {
    clearTimeout(timer);
  }
}
export async function until<T>(
  fn: () => Promise<T>,
  message: string,
  timeout = 20000,
): Promise<NonNullable<T>> {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const value = await fn();
    if (value) return value as NonNullable<T>;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('Timed out: ' + message);
}
