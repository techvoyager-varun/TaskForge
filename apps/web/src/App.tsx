import { useState, useEffect, useCallback, type FormEvent } from 'react';
import {
  Flame,
  LayoutDashboard,
  ListTodo,
  Cpu,
  GitBranch,
  Layers,
  ArchiveX,
  ChartNoAxesCombined,
  Settings,
  Plus,
  Search,
  ChevronRight,
  ChevronLeft,
  PanelLeftClose,
  RefreshCw,
  ArrowRight,
  LogOut,
} from 'lucide-react';
import { api, ApiError } from './api';
import { Busy, JobTable } from './components';
import { JobDialog } from './JobDialog';
import { SubmitDialog } from './SubmitDialog';
import { WorkflowView } from './WorkflowView';
import { OverviewView, WorkersView, QueuesView, MetricsView, SettingsView } from './views';
import type { Job, Queue, Worker, Workflow, Overview, Project, Page } from './types';
const pages = [
  { name: 'Overview', Icon: LayoutDashboard },
  { name: 'Jobs', Icon: ListTodo },
  { name: 'Workflows', Icon: GitBranch },
  { name: 'Workers', Icon: Cpu },
  { name: 'Queues', Icon: Layers },
  { name: 'Dead letters', Icon: ArchiveX },
  { name: 'Metrics', Icon: ChartNoAxesCombined },
  { name: 'Settings', Icon: Settings },
];
const subtitles: Record<string, string> = {
  Overview: 'Your execution platform, at a glance.',
  Jobs: 'Track every job from submission to completion.',
  Workflows: 'Build dependencies. Run work in parallel.',
  Workers: 'Monitor the processes that execute your jobs.',
  Queues: 'Keep workloads organized and concurrency under control.',
  'Dead letters': 'Inspect jobs that have exhausted their execution budget.',
  Metrics: 'Understand throughput and end-to-end latency.',
  Settings: 'Manage your project connection and queues.',
};
export default function App() {
  const [token, setToken] = useState(() => sessionStorage.getItem('taskforge_token') ?? '');
  const [page, setPage] = useState('Overview');
  const [collapsed, setCollapsed] = useState(false);
  const [project, setProject] = useState<Project>();
  const [overview, setOverview] = useState<Overview>();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [queues, setQueues] = useState<Queue[]>([]);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [error, setError] = useState('');
  const [connected, setConnected] = useState(false);
  const [selected, setSelected] = useState<string>();
  const [submitting, setSubmitting] = useState(false);
  const [search, setSearch] = useState('');
  const [query, setQuery] = useState('');
  const [status, setStatus] = useState('');
  const [cursor, setCursor] = useState<string | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [previous, setPrevious] = useState<(string | null)[]>([]);
  const disconnect = useCallback(() => {
    sessionStorage.removeItem('taskforge_token');
    setToken('');
    setProject(undefined);
    setOverview(undefined);
    setConnected(false);
  }, []);
  useEffect(() => {
    const timer = setTimeout(() => setQuery(search), 250);
    return () => clearTimeout(timer);
  }, [search]);
  const load = useCallback(
    async (snapshot?: Overview) => {
      if (!token) return;
      const params = new URLSearchParams({ limit: '30' });
      if (query) params.set('q', query);
      if (page === 'Dead letters') params.set('status', 'DEAD_LETTERED');
      else if (status && page === 'Jobs') params.set('status', status);
      if (cursor) params.set('cursor', cursor);
      try {
        const [p, o, j, q, w, f] = await Promise.all([
          api<{ data: Project }>(token, '/project'),
          snapshot
            ? Promise.resolve({ data: snapshot })
            : api<{ data: Overview }>(token, '/overview'),
          api<Page<Job>>(token, '/jobs?' + params),
          api<{ data: Queue[] }>(token, '/queues'),
          api<{ data: Worker[] }>(token, '/workers'),
          api<{ data: Workflow[] }>(token, '/workflows'),
        ]);
        setProject(p.data);
        setOverview(o.data);
        setJobs(j.data);
        setNextCursor(j.next_cursor);
        setQueues(q.data);
        setWorkers(w.data);
        setWorkflows(f.data);
        setError('');
      } catch (e) {
        if (e instanceof ApiError && e.status === 401) {
          disconnect();
          return;
        }
        setError((e as Error).message);
        setConnected(false);
      }
    },
    [token, query, status, cursor, page, disconnect],
  );
  useEffect(() => {
    if (!token) return;
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const abort = new AbortController();
    let loading = false;
    const update = async (data?: Overview) => {
      if (disposed || loading) return;
      loading = true;
      await load(data);
      loading = false;
    };
    void update();
    async function stream() {
      try {
        const response = await fetch('/api/v1/events', {
          headers: { Authorization: 'Bearer ' + token },
          signal: abort.signal,
        });
        if (!response.ok || !response.body) throw new Error('Live stream unavailable');
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (!disposed) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let boundary;
          while ((boundary = buffer.indexOf('\n\n')) >= 0) {
            const frame = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            const line = frame.split('\n').find((x) => x.startsWith('data: '));
            if (line) {
              setConnected(true);
              await update(JSON.parse(line.slice(6)) as Overview);
            }
          }
        }
      } catch {
        if (!disposed) setConnected(false);
      }
      if (!disposed) timer = setTimeout(() => void stream(), 3000);
    }
    void stream();
    const fallback = setInterval(() => {
      if (!disposed && !loading) void update();
    }, 10000);
    return () => {
      disposed = true;
      abort.abort();
      clearTimeout(timer);
      clearInterval(fallback);
    };
  }, [token, load]);
  const closeJob = useCallback(() => setSelected(undefined), []);
  const closeSubmit = useCallback(() => setSubmitting(false), []);
  const refresh = useCallback(() => {
    void load();
  }, [load]);
  if (!token)
    return (
      <Connect
        onConnect={(value) => {
          sessionStorage.setItem('taskforge_token', value);
          setToken(value);
        }}
      />
    );
  const navigate = (name: string) => {
    setPage(name);
    setCursor(null);
    setPrevious([]);
    setSearch('');
    setQuery('');
  };
  return (
    <div className={'app-shell ' + (collapsed ? 'sidebar-collapsed' : '')}>
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            navigate('Overview');
          }}
        >
          <span className="brand-mark">
            <Flame size={23} fill="currentColor" />
          </span>
          <span>
            TaskForge<small>EXECUTION PLATFORM</small>
          </span>
        </a>
        <button className="project-switch" onClick={() => navigate('Settings')}>
          <span className="project-avatar">{project?.name.slice(0, 1).toUpperCase() ?? 'T'}</span>
          <span>
            <strong>{project?.name ?? 'Connecting…'}</strong>
            <small>Project workspace</small>
          </span>
          <ChevronRight size={15} />
        </button>
        <span className="nav-label">WORKSPACE</span>
        <nav aria-label="Main navigation">
          {pages.map(({ name, Icon }) => (
            <button
              key={name}
              title={name}
              className={page === name ? 'active' : ''}
              onClick={() => navigate(name)}
            >
              <Icon size={19} />
              <span>{name}</span>
              {name === 'Dead letters' && !!overview?.counts.DEAD_LETTERED && (
                <b>{overview.counts.DEAD_LETTERED}</b>
              )}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="engine-note">
            <span className="orange-dot" />
            <div>
              <strong>Built for dependable work</strong>
              <p>C++ engine · PostgreSQL storage</p>
            </div>
          </div>
          <button className="account" onClick={disconnect} title="Disconnect project">
            <span className="account-avatar">{project?.name.slice(0, 1).toUpperCase() ?? 'T'}</span>
            <span>
              <strong>{project?.name ?? 'Project'}</strong>
              <small>Project access token</small>
            </span>
            <LogOut size={16} />
          </button>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="row">
            <button
              className="icon-button"
              aria-label="Toggle sidebar"
              onClick={() => setCollapsed(!collapsed)}
            >
              <PanelLeftClose size={19} />
            </button>
            <span className="breadcrumb">
              Workspace
              <ChevronRight size={13} />
              <strong>{page}</strong>
            </span>
          </div>
          <div className="row">
            <span className={'connection ' + (connected ? 'connected' : '')}>
              <i />
              {connected ? 'Live connection' : 'Reconnecting'}
            </span>
            <button className="icon-button" onClick={refresh} aria-label="Refresh data">
              <RefreshCw size={17} />
            </button>
            <span className="header-avatar">{project?.name.slice(0, 1).toUpperCase() ?? 'T'}</span>
          </div>
        </header>
        <main>
          <div className="page-heading">
            <div>
              <div className="eyebrow">WORKSPACE / {page.toUpperCase()}</div>
              <h1>{page === 'Overview' ? 'Project overview' : page}</h1>
              <p>{subtitles[page]}</p>
            </div>
            {!['Workflows', 'Settings'].includes(page) && (
              <button className="button primary" onClick={() => setSubmitting(true)}>
                <Plus size={17} />
                Submit job
              </button>
            )}
          </div>
          {error && (
            <p className="error" role="alert">
              {error}. Previously loaded data may be out of date.
            </p>
          )}
          {!overview || !project ? (
            <Busy />
          ) : (
            <>
              {page === 'Overview' && (
                <OverviewView
                  overview={overview}
                  jobs={jobs}
                  queues={queues}
                  workers={workers}
                  openJob={setSelected}
                  showJobs={() => navigate('Jobs')}
                />
              )}
              {['Jobs', 'Dead letters'].includes(page) && (
                <section className="panel">
                  <div className="jobs-toolbar">
                    <div className="search-field">
                      <Search size={17} />
                      <input
                        aria-label="Search jobs"
                        placeholder="Search handler or job ID…"
                        value={search}
                        onChange={(e) => {
                          setSearch(e.target.value);
                          setCursor(null);
                          setPrevious([]);
                        }}
                      />
                    </div>
                    {page === 'Jobs' && (
                      <select
                        aria-label="Filter job status"
                        value={status}
                        onChange={(e) => {
                          setStatus(e.target.value);
                          setCursor(null);
                          setPrevious([]);
                        }}
                      >
                        <option value="">All statuses</option>
                        {[
                          'PENDING',
                          'READY',
                          'RUNNING',
                          'COMPLETED',
                          'RETRY_WAIT',
                          'CANCEL_REQUESTED',
                          'CANCELLED',
                          'DEAD_LETTERED',
                        ].map((s) => (
                          <option key={s} value={s}>
                            {s.toLowerCase().replaceAll('_', ' ')}
                          </option>
                        ))}
                      </select>
                    )}
                    <span className="muted small">{jobs.length} jobs on this page</span>
                  </div>
                  <JobTable jobs={jobs} queues={queues} onOpen={setSelected} />
                  <div className="pagination">
                    <span>Sorted by newest submission</span>
                    <div className="row">
                      <button
                        className="button secondary"
                        disabled={!previous.length}
                        onClick={() => {
                          setCursor(previous.at(-1) ?? null);
                          setPrevious(previous.slice(0, -1));
                        }}
                      >
                        <ChevronLeft size={15} />
                        Previous
                      </button>
                      <button
                        className="button secondary"
                        disabled={!nextCursor}
                        onClick={() => {
                          setPrevious([...previous, cursor]);
                          setCursor(nextCursor);
                        }}
                      >
                        Next
                        <ChevronRight size={15} />
                      </button>
                    </div>
                  </div>
                </section>
              )}
              {page === 'Workers' && <WorkersView workers={workers} />}
              {page === 'Queues' && <QueuesView queues={queues} />}
              {page === 'Metrics' && <MetricsView overview={overview} token={token} />}
              {page === 'Workflows' && (
                <WorkflowView
                  token={token}
                  workflows={workflows}
                  queues={queues}
                  refresh={refresh}
                  openJob={setSelected}
                />
              )}
              {page === 'Settings' && (
                <SettingsView
                  token={token}
                  project={project}
                  queues={queues}
                  refresh={refresh}
                  disconnect={disconnect}
                />
              )}
            </>
          )}
          <footer className="page-footer">
            <span>
              TaskForge <b>v0.1</b>
            </span>
            <span>
              {overview
                ? 'Last measured ' + new Date(overview.measured_at).toLocaleTimeString()
                : 'Connecting to your project'}
            </span>
          </footer>
        </main>
      </div>
      {selected && <JobDialog token={token} id={selected} onClose={closeJob} onChanged={refresh} />}
      {submitting && (
        <SubmitDialog
          token={token}
          queues={queues}
          onClose={closeSubmit}
          onSubmitted={(id) => {
            setSubmitting(false);
            setSelected(id);
            refresh();
          }}
        />
      )}
    </div>
  );
}
function Connect({ onConnect }: { onConnect: (token: string) => void }) {
  const [token, setToken] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError('');
    try {
      await api(token.trim(), '/project');
      onConnect(token.trim());
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <main className="connect-screen">
      <section className="connect-card">
        <div className="connect-brand">
          <span className="brand-mark">
            <Flame size={28} fill="currentColor" />
          </span>
          <strong>TaskForge</strong>
        </div>
        <span className="eyebrow">DISTRIBUTED EXECUTION, MADE VISIBLE</span>
        <h1>
          Good work starts
          <br />
          with a connection.
        </h1>
        <p>
          Manage asynchronous jobs, watch your workers, and orchestrate workflows from one
          workspace.
        </p>
        <form onSubmit={(e) => void submit(e)}>
          <label>
            Project access token
            <input
              type="password"
              required
              autoComplete="off"
              spellCheck={false}
              placeholder="tf_…"
              value={token}
              onChange={(e) => setToken(e.target.value)}
            />
          </label>
          {error && (
            <p className="error" role="alert">
              {error}
            </p>
          )}
          <button className="button primary" disabled={busy}>
            {busy ? 'Connecting…' : 'Open workspace'}
            <ArrowRight size={17} />
          </button>
        </form>
        <p className="connect-footnote">
          Use the token generated when your project was created. It stays in this browser session.
        </p>
      </section>
      <div className="connect-decoration" aria-hidden="true">
        <div className="decor-node">submit</div>
        <div className="decor-line" />
        <div className="decor-node orange">schedule</div>
        <div className="decor-line" />
        <div className="decor-node">execute</div>
        <span>Durable by design.</span>
      </div>
    </main>
  );
}
