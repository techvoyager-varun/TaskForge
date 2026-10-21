import { X, ArrowUpRight, Inbox, LoaderCircle } from 'lucide-react';
import { useEffect, useRef, type ReactNode } from 'react';
import type { Job, Queue } from './types';
export const count = (value: number) =>
  new Intl.NumberFormat('en', {
    notation: value >= 100000 ? 'compact' : 'standard',
    maximumFractionDigits: 1,
  }).format(value);
export const date = (value: string) =>
  new Date(value).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
export function Status({ value }: { value: string }) {
  return (
    <span className={'status status-' + value.toLowerCase()}>
      <i />
      {value.toLowerCase().replaceAll('_', ' ')}
    </span>
  );
}
export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <Inbox size={32} />
      <h3>{title}</h3>
      <p>{children ?? 'New activity will appear here as your project processes work.'}</p>
    </div>
  );
}
export function Busy() {
  return (
    <div className="empty" role="status">
      <LoaderCircle className="spin" />
      Loading project data…
    </div>
  );
}
export function JobTable({
  jobs,
  queues,
  onOpen,
}: {
  jobs: Job[];
  queues: Queue[];
  onOpen: (id: string) => void;
}) {
  if (!jobs.length)
    return (
      <Empty title="No jobs here yet">Submit your first job to see the execution lifecycle.</Empty>
    );
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Job / handler</th>
            <th>Status</th>
            <th>Queue</th>
            <th>Attempt</th>
            <th>Submitted</th>
            <th>
              <span className="sr-only">Details</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id}>
              <td>
                <button className="job-link" onClick={() => onOpen(job.id)}>
                  <strong>{job.type}</strong>
                  <code>{job.id.slice(0, 8)}</code>
                </button>
              </td>
              <td>
                <Status value={job.status} />
              </td>
              <td>{queues.find((q) => q.id === job.queue_id)?.name ?? job.queue_id.slice(0, 8)}</td>
              <td>
                <span className="mono">
                  {job.attempt_no}
                  <span className="muted"> / {job.max_attempts}</span>
                </span>
              </td>
              <td className="muted nowrap">{date(job.created_at)}</td>
              <td>
                <button
                  className="icon-button"
                  aria-label={'Inspect job ' + job.id}
                  onClick={() => onOpen(job.id)}
                >
                  <ArrowUpRight size={16} />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
export function Modal({
  title,
  onClose,
  children,
  wide = false,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  wide?: boolean;
}) {
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const dialog = document.getElementById('active-dialog');
    const nodes = () =>
      Array.from(
        dialog?.querySelectorAll<HTMLElement>(
          'button:not([disabled]),input,select,textarea,a[href]',
        ) ?? [],
      );
    nodes()[0]?.focus();
    function key(event: KeyboardEvent) {
      if (event.key === 'Escape') closeRef.current();
      if (event.key === 'Tab') {
        const list = nodes();
        const first = list[0],
          last = list.at(-1);
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first?.focus();
        }
      }
    }
    document.addEventListener('keydown', key);
    const old = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', key);
      document.body.style.overflow = old;
      previous?.focus();
    };
  }, []);
  return (
    <div
      className="modal-backdrop"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <section
        id="active-dialog"
        className={'modal ' + (wide ? 'modal-wide' : '')}
        role="dialog"
        aria-modal="true"
        aria-labelledby="dialog-title"
      >
        <header>
          <h2 id="dialog-title">{title}</h2>
          <button className="icon-button" onClick={onClose} aria-label="Close dialog">
            <X size={20} />
          </button>
        </header>
        {children}
      </section>
    </div>
  );
}
