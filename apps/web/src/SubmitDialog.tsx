import { useState, type FormEvent } from 'react';
import { ArrowRight } from 'lucide-react';
import { api } from './api';
import { Modal } from './components';
import type { Queue, JobType } from './types';
const examples: Record<JobType, unknown> = {
  noop: {},
  echo: { message: 'Hello from TaskForge' },
  sum: { numbers: [10, 20, 30] },
  sleep: { duration_ms: 2000 },
  fail: { message: 'Retry demonstration', fail_until_attempt: 1 },
};
export function SubmitDialog({
  token,
  queues,
  onClose,
  onSubmitted,
}: {
  token: string;
  queues: Queue[];
  onClose: () => void;
  onSubmitted: (id: string) => void;
}) {
  const [type, setType] = useState<JobType>('echo');
  const [queue, setQueue] = useState(queues[0]?.id ?? '');
  const [payload, setPayload] = useState(JSON.stringify(examples.echo, null, 2));
  const [priority, setPriority] = useState(0);
  const [attempts, setAttempts] = useState(3);
  const [timeout, setTimeoutMs] = useState(30000);
  const [delay, setDelay] = useState(0);
  const [key] = useState(() => crypto.randomUUID());
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError('');
    try {
      const response = await api<{ data: { id: string } }>(
        token,
        '/jobs',
        {
          queue_id: queue,
          type,
          payload: JSON.parse(payload),
          priority,
          max_attempts: attempts,
          timeout_ms: timeout,
          ...(delay ? { available_at: new Date(Date.now() + delay * 1000).toISOString() } : {}),
        },
        { 'Idempotency-Key': key },
      );
      onSubmitted(response.data.id);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title="Submit a job" onClose={onClose}>
      <p className="muted modal-description">
        Choose a handler and send work to your durable queue.
      </p>
      <form onSubmit={(e) => void submit(e)}>
        <div className="form-row">
          <label>
            Handler
            <select
              value={type}
              onChange={(e) => {
                const next = e.target.value as JobType;
                setType(next);
                setPayload(JSON.stringify(examples[next], null, 2));
              }}
            >
              {Object.keys(examples).map((x) => (
                <option key={x}>{x}</option>
              ))}
            </select>
          </label>
          <label>
            Queue
            <select required value={queue} onChange={(e) => setQueue(e.target.value)}>
              <option value="" disabled>
                Select queue
              </option>
              {queues.map((q) => (
                <option value={q.id} key={q.id}>
                  {q.name}
                </option>
              ))}
            </select>
          </label>
        </div>
        <label>
          Payload <span className="label-hint">JSON</span>
          <textarea
            className="code-editor"
            rows={7}
            spellCheck={false}
            required
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
          />
        </label>
        <div className="form-row">
          <label>
            Priority
            <input
              type="number"
              min={-100}
              max={100}
              value={priority}
              onChange={(e) => setPriority(Number(e.target.value))}
            />
          </label>
          <label>
            Maximum attempts
            <input
              type="number"
              min={1}
              max={20}
              value={attempts}
              onChange={(e) => setAttempts(Number(e.target.value))}
            />
          </label>
        </div>
        <div className="form-row">
          <label>
            Timeout (ms)
            <input
              type="number"
              min={100}
              max={3600000}
              value={timeout}
              onChange={(e) => setTimeoutMs(Number(e.target.value))}
            />
          </label>
          <label>
            Delay (seconds)
            <input
              type="number"
              min={0}
              max={86400}
              value={delay}
              onChange={(e) => setDelay(Number(e.target.value))}
            />
          </label>
        </div>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        <footer>
          <button type="button" className="button secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="button primary" disabled={busy || !queue}>
            {busy ? 'Submitting…' : 'Submit job'}
            <ArrowRight size={16} />
          </button>
        </footer>
      </form>
    </Modal>
  );
}
