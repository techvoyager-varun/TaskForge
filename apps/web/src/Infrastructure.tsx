import { useEffect, useState } from 'react';
import { api } from './api';
interface Infrastructure {
  redis: {
    configured: boolean;
    available: boolean;
    hits: number;
    misses: number;
    errors: number;
    hit_rate: number | null;
  };
  kafka: {
    outbox: {
      pending: number;
      delivered: number;
      oldest_pending_seconds: number;
      max_delivery_attempts: number;
    };
    projection: { aggregate_type: string; status: string; total: number }[];
    events: { event_type: string; total: string }[];
    health: {
      component: string;
      available: boolean;
      lag: string | null;
      error_count: string;
      heartbeat_at: string;
    }[];
    lag_scope: string;
  };
  execution: {
    assignments_last_minute: number;
    attempts: number;
    failed_attempts: number;
    queue_wait_p95_ms: number | null;
    execution_p95_ms: number | null;
  };
  measured_at: string;
}
export function InfrastructureView({ token }: { token: string }) {
  const [data, setData] = useState<Infrastructure>(),
    [error, setError] = useState('');
  useEffect(() => {
    let disposed = false,
      busy = false;
    const load = async () => {
      if (busy) return;
      busy = true;
      try {
        const response = await api<{ data: Infrastructure }>(token, '/infrastructure');
        if (!disposed) {
          setData(response.data);
          setError('');
        }
      } catch (e) {
        if (!disposed) setError((e as Error).message);
      } finally {
        busy = false;
      }
    };
    void load();
    const timer = setInterval(() => void load(), 5000);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [token]);
  if (error)
    return (
      <p className="error" role="alert">
        Infrastructure measurements unavailable: {error}
      </p>
    );
  if (!data) return <p className="muted">Loading infrastructure measurements…</p>;
  const consumer = data.kafka.health.find((h) => h.component === 'consumer:live'),
    publisher = data.kafka.health.find((h) => h.component === 'publisher');
  const cards = [
    [
      'Redis',
      data.redis.configured
        ? data.redis.available
          ? 'Connected'
          : 'Unavailable'
        : 'Not configured',
      `${data.redis.hits} hits · ${data.redis.misses} misses · ${data.redis.errors} errors on this API process`,
    ],
    [
      'Cache hit rate',
      data.redis.hit_rate === null ? 'No samples' : (100 * data.redis.hit_rate).toFixed(1) + '%',
      'Job-detail cache · PostgreSQL version checked on every read',
    ],
    [
      'Outbox pending',
      String(data.kafka.outbox.pending),
      `${data.kafka.outbox.delivered} delivered · oldest pending ${Math.round(data.kafka.outbox.oldest_pending_seconds)}s`,
    ],
    [
      'Kafka consumer lag',
      consumer?.available && consumer.lag !== null ? consumer.lag : 'Unavailable',
      'Messages · shared deployment · durable consumer positions',
    ],
    [
      'Publisher / consumer',
      `${publisher?.available ? 'Online' : 'Unavailable'} / ${consumer?.available ? 'Online' : 'Unavailable'}`,
      'Reported heartbeats expire after 15 seconds',
    ],
    [
      'Assignments / minute',
      String(data.execution.assignments_last_minute),
      'Durable dispatches across all schedulers in this project',
    ],
    [
      'Queue wait p95',
      data.execution.queue_wait_p95_ms === null
        ? 'No samples'
        : Math.round(data.execution.queue_wait_p95_ms) + ' ms',
      'First assignment minus submission · 24 hours',
    ],
    [
      'Execution p95',
      data.execution.execution_p95_ms === null
        ? 'No samples'
        : Math.round(data.execution.execution_p95_ms) + ' ms',
      'Finished attempts · 24 hours',
    ],
    [
      'Failed attempts',
      `${data.execution.failed_attempts} / ${data.execution.attempts}`,
      'Failures and timeouts, including retried attempts · 24 hours',
    ],
  ];
  return (
    <>
      <div className="panel-heading">
        <h3>Infrastructure and scheduling</h3>
        <span className="muted">Measured {new Date(data.measured_at).toLocaleTimeString()}</span>
      </div>
      <div className="metric-grid three">
        {cards.map(([label, value, detail]) => (
          <section className="metric-card" key={label}>
            <div>
              <span>{label}</span>
            </div>
            <strong style={{ fontSize: '1.4rem' }}>{value}</strong>
            <p>{detail}</p>
          </section>
        ))}
      </div>
      <section className="panel">
        <div className="panel-heading">
          <h3>Kafka analytics projection</h3>
          <span className="muted">Derived state · live namespace</span>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Aggregate</th>
                <th>Status</th>
                <th>Count</th>
              </tr>
            </thead>
            <tbody>
              {data.kafka.projection.map((p) => (
                <tr key={p.aggregate_type + p.status}>
                  <td>{p.aggregate_type}</td>
                  <td>{p.status}</td>
                  <td>{p.total}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!data.kafka.projection.length && (
            <p className="muted">No consumed events for this project yet.</p>
          )}
        </div>
      </section>
    </>
  );
}
