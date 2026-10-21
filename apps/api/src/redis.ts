import { createClient } from 'redis';
import { createHash } from 'node:crypto';
import type { RequestHandler } from 'express';
import { HttpError } from './errors.js';
const admit = `local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('PEXPIRE',KEYS[1],ARGV[1]) end; return {n,redis.call('PTTL',KEYS[1])}`;
export class RedisLayer {
  readonly client;
  readonly counters = { hits: 0, misses: 0, errors: 0 };
  constructor(
    url: string,
    readonly prefix = 'taskforge',
    readonly limit = 300,
    readonly windowMs = 60000,
  ) {
    this.client = createClient({
      url,
      disableOfflineQueue: true,
      commandsQueueMaxLength: 256,
      socket: {
        connectTimeout: 1000,
        reconnectStrategy: (retries) => Math.min(100 + retries * 100, 2000),
      },
    });
    this.client.on('error', () => {
      this.counters.errors++;
    });
  }
  async start() {
    await this.client.connect();
  }
  close() {
    if (this.client.isOpen) this.client.destroy();
  }
  metrics() {
    return {
      configured: true,
      available: this.client.isReady,
      ...this.counters,
      hit_rate:
        this.counters.hits + this.counters.misses
          ? this.counters.hits / (this.counters.hits + this.counters.misses)
          : null,
    };
  }
  command() {
    return this.client.withCommandOptions({ timeout: 400 });
  }
  key(project: string, id: string, version: string) {
    return `${this.prefix}:job:${project}:${id}:${version}`;
  }
  async get(project: string, id: string, version: string): Promise<Record<string, unknown> | null> {
    try {
      if (!this.client.isReady) {
        this.counters.errors++;
        this.counters.misses++;
        return null;
      }
      const raw = await this.command().get(this.key(project, id, version));
      if (raw) {
        try {
          const data = JSON.parse(raw);
          if (
            data.id === id &&
            data.project_id === project &&
            String(data.state_version) === version &&
            Array.isArray(data.attempts)
          ) {
            this.counters.hits++;
            return data;
          }
        } catch {
          /* corrupt entries are misses */
        }
      }
    } catch {
      this.counters.errors++;
    }
    this.counters.misses++;
    return null;
  }
  async set(data: Record<string, unknown>) {
    try {
      if (!this.client.isReady) return;
      await this.command().set(
        this.key(String(data.project_id), String(data.id), String(data.state_version)),
        JSON.stringify(data),
        { EX: ['COMPLETED', 'CANCELLED', 'DEAD_LETTERED'].includes(String(data.status)) ? 60 : 5 },
      );
    } catch {
      this.counters.errors++;
    }
  }
  limiter(): RequestHandler {
    return async (req, res, next) => {
      try {
        if (!this.client.isReady) throw new Error('unavailable');
        const address = createHash('sha256')
          .update(req.ip ?? 'unknown')
          .digest('hex');
        const [count, ttl] = (await this.command().eval(admit, {
          keys: [`${this.prefix}:rate:${address}`],
          arguments: [String(this.windowMs)],
        })) as number[];
        res.set('RateLimit-Limit', String(this.limit));
        res.set('RateLimit-Remaining', String(Math.max(0, this.limit - count!)));
        if (count! > this.limit) {
          res.set('Retry-After', String(Math.max(1, Math.ceil(ttl! / 1000))));
          throw new HttpError(429, 'rate_limited', 'Too many requests; retry later');
        }
        next();
      } catch (error) {
        if (error instanceof HttpError) {
          next(error);
          return;
        }
        this.counters.errors++;
        next(
          new HttpError(
            503,
            'rate_limit_unavailable',
            'Admission temporarily unavailable; retry later',
          ),
        );
      }
    };
  }
}
