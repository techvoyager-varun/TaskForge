import { useState, type FormEvent } from 'react';
import { CheckCheck, Activity, Layers, TriangleAlert, ArrowUpRight, Cpu, Plus } from 'lucide-react';
import { api } from './api';
import { count, Status, JobTable, Empty, date } from './components';
import type { Job, Queue, Worker, Overview, Project } from './types';
export function Throughput({ overview }: { overview: Overview }) {
  const now = new Date(overview.measured_at);
  now.setSeconds(0, 0);
  const bars = Array.from({ length: 30 }, (_, i) => {
    const minute = new Date(now.getTime() - (29 - i) * 60000);
    return {
      minute,
      value:
        overview.throughput.find((row) => new Date(row.minute).getTime() === minute.getTime())
          ?.completed ?? 0,
    };
  });
  const maximum = Math.max(1, ...bars.map((b) => b.value));
  return (
    <figure
      className="throughput-chart"
      aria-label="Completed jobs per minute over the last thirty minutes"
    >
      <div className="chart-scale">
        <span>{maximum}</span>
        <span>{Math.round(maximum / 2)}</span>
        <span>0</span>
      </div>
      <div className="chart-body">
        <div className="chart-bars">
          {bars.map((bar) => (
            <div
              className="bar-column"
              key={bar.minute.toISOString()}
              title={bar.minute.toLocaleTimeString() + ': ' + bar.value + ' completions'}
            >
              <div
                className="bar"
                style={{ height: (bar.value / maximum) * 100 + '%', minHeight: bar.value ? 3 : 0 }}
              />
            </div>
          ))}
        </div>
        <figcaption>
          <span>
            {bars[0]!.minute.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}
          </span>
          <span>Completed jobs / minute</span>
          <span>Now</span>
        </figcaption>
      </div>
    </figure>
  );
}
export function OverviewView({
  overview,
  jobs,
  queues,
  workers,
  openJob,
  showJobs,
}: {
  overview: Overview;
  jobs: Job[];
  queues: Queue[];
  workers: Worker[];
  openJob: (id: string) => void;
  showJobs: () => void;
}) {
  const c = overview.counts;
  const total = Object.values(c).reduce((a, b) => a + (b ?? 0), 0);
  const failed = c.DEAD_LETTERED ?? 0;
  const cards = [
    {
      label: 'Completed jobs',
      value: count(c.COMPLETED ?? 0),
      hint: 'Durably recorded results',
      Icon: CheckCheck,
      color: 'green',
    },
    {
      label: 'Running now',
      value: count(c.RUNNING ?? 0),
      hint: workers.filter((w) => w.online).length + ' connected project workers',
      Icon: Activity,
      color: 'orange',
    },
    {
      label: 'Queue backlog',
      value: count((c.PENDING ?? 0) + (c.READY ?? 0) + (c.RETRY_WAIT ?? 0)),
      hint: 'Pending, ready and retrying',
      Icon: Layers,
      color: 'blue',
    },
    {
      label: 'Dead-letter rate',
      value: (total ? (failed / total) * 100 : 0).toFixed(1) + '%',
      hint: failed + ' exhausted jobs · all time',
      Icon: TriangleAlert,
      color: 'red',
    },
  ];
  return (
    <>
      <div className="metric-grid">
        {cards.map(({ label, value, hint, Icon, color }) => (
          <section className="metric-card" key={label}>
            <div>
              <span>{label}</span>
              <span className={'metric-icon ' + color}>
                <Icon size={17} />
              </span>
            </div>
            <strong>{value}</strong>
            <p>{hint}</p>
          </section>
        ))}
      </div>
      <div className="overview-grid">
        <section className="panel">
          <div className="panel-heading">
            <div>
              <h3>Execution throughput</h3>
              <p>Completed jobs in the last 30 minutes</p>
            </div>
            <span className="live-label">
              <i />
              Live data
            </span>
          </div>
          <Throughput overview={overview} />
        </section>
        <section className="panel">
          <div className="panel-heading">
            <div>
              <h3>Queue activity</h3>
              <p>Current execution capacity</p>
            </div>
            <Layers size={18} className="muted" />
          </div>
          <div className="queue-summary">
            {queues.length ? (
              queues.slice(0, 4).map((q) => (
                <div key={q.id}>
                  <div className="row between">
                    <strong>{q.name}</strong>
                    <span className="mono">
                      {q.running}
                      <span className="muted"> / {q.concurrency_limit}</span>
                    </span>
                  </div>
                  <div className="progress">
                    <span
                      style={{
                        width: Math.min(100, (q.running / q.concurrency_limit) * 100) + '%',
                      }}
                    />
                  </div>
                  <p>
                    {q.backlog} waiting<span>{q.completed} completed</span>
                  </p>
                </div>
              ))
            ) : (
              <Empty title="No queues" />
            )}
          </div>
        </section>
      </div>
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h3>Recent jobs</h3>
            <p>The latest work submitted to your project</p>
          </div>
          <button className="text-button" onClick={showJobs}>
            View all jobs
            <ArrowUpRight size={15} />
          </button>
        </div>
        <JobTable jobs={jobs.slice(0, 6)} queues={queues} onOpen={openJob} />
      </section>
      <div className="info-strip">
        <span>
          <span className="success-dot" />
          PostgreSQL durable storage
        </span>
        <span>At-least-once execution</span>
        <span>Fenced lease ownership</span>
      </div>
    </>
  );
}
export function WorkersView({ workers }: { workers: Worker[] }) {
  return workers.length ? (
    <div className="worker-grid">
      {workers.map((w) => (
        <section className="panel worker-card" key={w.id}>
          <div className="row between">
            <span className="tile-icon">
              <Cpu size={23} />
            </span>
            <Status value={w.draining ? 'DRAINING' : w.online ? 'ONLINE' : 'OFFLINE'} />
          </div>
          <h3>{w.name}</h3>
          <code>{w.id.slice(0, 8)}</code>
          <div className="worker-slots">
            {Array.from({ length: Math.min(w.capacity, 16) }, (_, i) => (
              <span key={i} className={i < w.active_jobs ? 'occupied' : ''} />
            ))}
          </div>
          <div className="row between">
            <span className="muted">Active project jobs</span>
            <strong>
              {w.active_jobs} / {w.capacity}
            </strong>
          </div>
          <p className="muted small">Heartbeat {date(w.heartbeat_at)}</p>
        </section>
      ))}
    </div>
  ) : (
    <section className="panel">
      <Empty title="No project workers observed">
        Workers appear after their first assignment to this project.
      </Empty>
    </section>
  );
}
export function QueuesView({ queues }: { queues: Queue[] }) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <h3>Project queues</h3>
        <span className="muted">{queues.length} configured</span>
      </div>
      {queues.length ? (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Queue</th>
                <th>Running / limit</th>
                <th>Backlog</th>
                <th>Completed</th>
                <th>Dead-lettered</th>
              </tr>
            </thead>
            <tbody>
              {queues.map((q) => (
                <tr key={q.id}>
                  <td>
                    <strong>{q.name}</strong>
                    <code className="subtext">{q.id.slice(0, 8)}</code>
                  </td>
                  <td className="mono">
                    {q.running} / {q.concurrency_limit}
                  </td>
                  <td>{count(q.backlog)}</td>
                  <td>{count(q.completed)}</td>
                  <td>{count(q.dead_lettered)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty title="No queues configured" />
      )}
    </section>
  );
}
export function MetricsView({ overview }: { overview: Overview }) {
  return (
    <>
      <div className="metric-grid three">
        {(['p50', 'p95', 'p99'] as const).map((p) => (
          <section className="metric-card" key={p}>
            <div>
              <span>{p.toUpperCase()} completion latency</span>
              <Activity size={17} className="muted" />
            </div>
            <strong>
              {Math.round(overview.latency[`${p}_ms`]).toLocaleString()}
              <small> ms</small>
            </strong>
            <p>Submission to completion · last 24 hours</p>
          </section>
        ))}
      </div>
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h3>Execution throughput</h3>
            <p>Actual durable completions, one-minute buckets</p>
          </div>
          <span className="muted">{overview.latency.completed} completions in 24h</span>
        </div>
        <Throughput overview={overview} />
      </section>
      <p className="muted small">
        Latency includes queue wait, execution and retries. Empty datasets display zero. Prometheus
        metrics are available at the authenticated /api/v1/metrics endpoint.
      </p>
    </>
  );
}
export function SettingsView({
  token,
  project,
  queues,
  refresh,
  disconnect,
}: {
  token: string;
  project: Project;
  queues: Queue[];
  refresh: () => void;
  disconnect: () => void;
}) {
  const [name, setName] = useState('');
  const [slots, setSlots] = useState(4);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function create(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError('');
    try {
      await api(token, '/queues', { name, concurrency_limit: slots });
      setName('');
      refresh();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="settings-grid">
      <section className="panel settings-card">
        <h3>Project connection</h3>
        <label>
          Project name
          <input readOnly value={project.name} />
        </label>
        <label>
          Project identifier
          <input readOnly className="mono" value={project.id} />
        </label>
        <p className="muted">
          Your token is stored only for this browser session. Disconnect to remove it from this tab.
        </p>
        <button className="button secondary" onClick={disconnect}>
          Disconnect project
        </button>
      </section>
      <section className="panel settings-card">
        <h3>Create queue</h3>
        <p className="muted">
          Limit the number of active leases for a group of jobs. Up to 32 queues per project.
        </p>
        <form onSubmit={(e) => void create(e)}>
          <label>
            Queue name
            <input
              required
              maxLength={100}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. data-processing"
            />
          </label>
          <label>
            Concurrency limit
            <input
              type="number"
              min={1}
              max={256}
              value={slots}
              onChange={(e) => setSlots(Number(e.target.value))}
            />
          </label>
          {error && (
            <p className="error" role="alert">
              {error}
            </p>
          )}
          <button className="button primary" disabled={busy || queues.length >= 32}>
            <Plus size={16} />
            {busy ? 'Creating…' : 'Create queue'}
          </button>
        </form>
      </section>
    </div>
  );
}
