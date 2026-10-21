import type pg from 'pg';
import type { RedisLayer } from './redis.js';
export async function infrastructureSnapshot(pool: pg.Pool, project: string, redis?: RedisLayer) {
  const [outbox, projection, events, health, execution] = await Promise.all([
    pool.query(
      `SELECT count(*) FILTER(WHERE delivered_at IS NULL)::integer AS pending,count(*) FILTER(WHERE delivered_at IS NOT NULL)::integer AS delivered,COALESCE(max(extract(epoch FROM clock_timestamp()-occurred_at)) FILTER(WHERE delivered_at IS NULL),0)::float AS oldest_pending_seconds,COALESCE(max(attempts),0)::integer AS max_delivery_attempts FROM outbox_events WHERE project_id=$1`,
      [project],
    ),
    pool.query(
      "SELECT aggregate_type,status,count(*)::integer AS total FROM analytics_projection WHERE namespace='live' AND project_id=$1 GROUP BY aggregate_type,status",
      [project],
    ),
    pool.query(
      "SELECT event_type,total::text FROM analytics_event_counts WHERE namespace='live' AND project_id=$1 ORDER BY event_type",
      [project],
    ),
    pool.query(
      "SELECT component,available AND heartbeat_at>clock_timestamp()-interval '15 seconds' AS available,CASE WHEN heartbeat_at>clock_timestamp()-interval '15 seconds' AND available THEN lag::text ELSE NULL END AS lag,error_count::text,heartbeat_at FROM pipeline_health WHERE component IN ('publisher','consumer:live')",
    ),
    pool.query(
      `SELECT count(*) FILTER(WHERE a.assigned_at>now()-interval '1 minute')::integer AS assignments_last_minute,count(*) FILTER(WHERE a.status IN ('FAILED','TIMED_OUT'))::integer AS failed_attempts,count(*)::integer AS attempts,
   percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch FROM a.assigned_at-j.created_at)*1000) FILTER(WHERE a.attempt_no=1) AS queue_wait_p95_ms,
   percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch FROM a.finished_at-a.started_at)*1000) FILTER(WHERE a.started_at IS NOT NULL AND a.finished_at IS NOT NULL) AS execution_p95_ms
   FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.project_id=$1 AND a.assigned_at>now()-interval '24 hours'`,
      [project],
    ),
  ]);
  return {
    redis: redis?.metrics() ?? {
      configured: false,
      available: false,
      hits: 0,
      misses: 0,
      errors: 0,
      hit_rate: null,
    },
    kafka: {
      outbox: outbox.rows[0],
      projection: projection.rows,
      events: events.rows,
      health: health.rows,
      lag_scope: 'shared deployment',
    },
    execution: execution.rows[0],
    measured_at: new Date().toISOString(),
  };
}
