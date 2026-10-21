import { useState, useEffect } from 'react';
import { RefreshCw, Ban } from 'lucide-react';
import { api } from './api';
import { Modal, Status, date, Busy } from './components';
import type { Job, Log } from './types';
export function JobDialog({
  token,
  id,
  onClose,
  onChanged,
}: {
  token: string;
  id: string;
  onClose: () => void;
  onChanged: () => void;
}) {
  const [job, setJob] = useState<Job>();
  const [logs, setLogs] = useState<Log[]>([]);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    let disposed = false;
    async function load() {
      try {
        const [a, b] = await Promise.all([
          api<{ data: Job }>(token, '/jobs/' + id),
          api<{ data: Log[] }>(token, '/jobs/' + id + '/logs'),
        ]);
        if (!disposed) {
          setJob(a.data);
          setLogs(b.data);
        }
      } catch (e) {
        if (!disposed) setError((e as Error).message);
      }
    }
    void load();
    const timer = setInterval(() => void load(), 2000);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [id, token]);
  async function action(name: string) {
    setBusy(true);
    try {
      await api(token, '/jobs/' + id + '/' + name, {});
      onChanged();
      setJob((await api<{ data: Job }>(token, '/jobs/' + id)).data);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title="Execution details" onClose={onClose} wide>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {!job ? (
        <Busy />
      ) : (
        <>
          <div className="detail-heading">
            <div>
              <h3>{job.type}</h3>
              <code>{job.id}</code>
            </div>
            <Status value={job.status} />
          </div>
          <div className="detail-actions">
            {['PENDING', 'READY', 'RUNNING', 'RETRY_WAIT'].includes(job.status) && (
              <button
                className="button secondary"
                disabled={busy}
                onClick={() => void action('cancel')}
              >
                <Ban size={15} />
                Cancel job
              </button>
            )}
            {['DEAD_LETTERED', 'CANCELLED'].includes(job.status) && (
              <button
                className="button secondary"
                disabled={busy}
                onClick={() => void action('retry')}
              >
                <RefreshCw size={15} />
                Retry job
              </button>
            )}
          </div>
          <div className="detail-grid">
            <div>
              <label>Submitted</label>
              <p>{date(job.created_at)}</p>
            </div>
            <div>
              <label>Attempts</label>
              <p>
                {job.attempt_no} of {job.max_attempts}
              </p>
            </div>
          </div>
          {job.error && <p className="error">{job.error}</p>}
          <h4>Payload</h4>
          <pre>{JSON.stringify(job.payload, null, 2)}</pre>
          {job.result !== null && job.result !== undefined && (
            <>
              <h4>Result</h4>
              <pre className="result">{JSON.stringify(job.result, null, 2)}</pre>
            </>
          )}
          <h4>Attempt history</h4>
          <div className="attempts">
            {job.attempts?.length ? (
              job.attempts.map((a) => (
                <div className="attempt" key={a.attempt_no}>
                  <span className="attempt-number">{a.attempt_no}</span>
                  <div>
                    <Status value={a.status} />
                    <p>
                      {date(a.assigned_at)} · worker {a.worker_id.slice(0, 8)}
                    </p>
                    {a.error && <p className="text-error">{a.error}</p>}
                  </div>
                </div>
              ))
            ) : (
              <p className="muted">Waiting for scheduler assignment.</p>
            )}
          </div>
          <h4>Execution log</h4>
          <div className="log-view">
            {logs.length ? (
              logs.map((log) => (
                <div key={log.id}>
                  <time>{new Date(log.created_at).toLocaleTimeString()}</time>
                  <span className={log.level === 'error' ? 'text-error' : ''}>{log.message}</span>
                </div>
              ))
            ) : (
              <span>No execution events recorded yet.</span>
            )}
          </div>
        </>
      )}
    </Modal>
  );
}
