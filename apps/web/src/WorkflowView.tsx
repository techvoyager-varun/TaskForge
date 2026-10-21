import { useState, useEffect, useMemo, type FormEvent } from 'react';
import { ReactFlow, Background, Controls, Position, type Node, type Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Plus, Play, RotateCcw, Ban, GitBranch, ArrowLeft } from 'lucide-react';
import { api } from './api';
import { Modal, Status, date, Empty } from './components';
import type { Queue, Workflow } from './types';
function layout(
  workflow: Workflow,
  openJob: (id: string) => void,
): { nodes: Node[]; edges: Edge[] } {
  const source = workflow.nodes ?? [];
  const edges = workflow.edges ?? [];
  const levels = new Map(source.map((n) => [n.id, 0]));
  const indegree = new Map(source.map((n) => [n.id, 0]));
  const children = new Map(source.map((n) => [n.id, [] as string[]]));
  for (const e of edges) {
    children.get(e.parent_id)?.push(e.child_id);
    indegree.set(e.child_id, (indegree.get(e.child_id) ?? 0) + 1);
  }
  const ready = source.filter((n) => indegree.get(n.id) === 0).map((n) => n.id);
  for (let i = 0; i < ready.length; i++)
    for (const child of children.get(ready[i]!) ?? []) {
      levels.set(child, Math.max(levels.get(child) ?? 0, (levels.get(ready[i]!) ?? 0) + 1));
      indegree.set(child, indegree.get(child)! - 1);
      if (indegree.get(child) === 0) ready.push(child);
    }
  const positions = new Map<number, number>();
  return {
    nodes: source.map((n) => {
      const level = levels.get(n.id) ?? 0;
      const row = positions.get(level) ?? 0;
      positions.set(level, row + 1);
      return {
        id: n.id,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        ariaLabel: n.node_key + ' ' + n.type + ' ' + n.status,
        focusable: true,
        position: { x: level * 240, y: row * 140 },
        data: {
          label: (
            <button
              type="button"
              className="graph-label"
              onClick={(event) => {
                event.stopPropagation();
                openJob(n.id);
              }}
            >
              <strong>{n.node_key}</strong>
              <span>{n.type}</span>
              <Status value={n.status} />
            </button>
          ),
        },
        className: 'workflow-node',
        style: {
          width: 190,
          borderColor:
            n.status === 'COMPLETED' ? '#84bfa8' : n.status === 'RUNNING' ? '#ef673c' : '#d8dde5',
        },
      };
    }),
    edges: edges.map((e) => ({
      id: e.parent_id + e.child_id,
      source: e.parent_id,
      target: e.child_id,
      animated: source.find((n) => n.id === e.child_id)?.status === 'RUNNING',
      style: { stroke: '#a7b0bf', strokeWidth: 1.5 },
    })),
  };
}
export function WorkflowView({
  token,
  workflows,
  queues,
  refresh,
  openJob,
}: {
  token: string;
  workflows: Workflow[];
  queues: Queue[];
  refresh: () => void;
  openJob: (id: string) => void;
}) {
  const [selected, setSelected] = useState<string>();
  const [workflow, setWorkflow] = useState<Workflow>();
  const [history, setHistory] = useState<Workflow[]>([]);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!selected) {
      setWorkflow(undefined);
      return;
    }
    let disposed = false;
    async function load() {
      try {
        const [x, h] = await Promise.all([
          api<{ data: Workflow }>(token, '/workflows/' + selected),
          api<{ data: Workflow[] }>(token, '/workflows/' + selected + '/executions'),
        ]);
        if (!disposed) {
          setWorkflow(x.data);
          setHistory(h.data);
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
  }, [selected, token]);
  const graph = useMemo(
    () => (workflow ? layout(workflow, openJob) : { nodes: [], edges: [] }),
    [workflow, openJob],
  );
  async function action(name: string) {
    if (!selected) return;
    setBusy(true);
    setError('');
    try {
      const response = await api<{ data: Workflow }>(
        token,
        '/workflows/' + selected + '/' + name,
        {},
      );
      if (name === 'retry') setSelected(response.data.id);
      else setWorkflow((await api<{ data: Workflow }>(token, '/workflows/' + selected)).data);
      refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {selected && workflow ? (
        <>
          <div className="section-toolbar">
            <button className="text-button" onClick={() => setSelected(undefined)}>
              <ArrowLeft size={16} />
              All workflows
            </button>
            <div className="row">
              <Status value={workflow.status} />
              {workflow.status === 'DRAFT' && (
                <button
                  className="button primary"
                  disabled={busy}
                  onClick={() => void action('run')}
                >
                  <Play size={15} />
                  Run workflow
                </button>
              )}
              {['DRAFT', 'RUNNING'].includes(workflow.status) && (
                <button
                  className="button secondary"
                  disabled={busy}
                  onClick={() => void action('cancel')}
                >
                  <Ban size={15} />
                  Cancel
                </button>
              )}
              {['COMPLETED', 'FAILED', 'CANCELLED'].includes(workflow.status) && (
                <button
                  className="button secondary"
                  disabled={busy}
                  onClick={() => void action('retry')}
                >
                  <RotateCcw size={15} />
                  New execution
                </button>
              )}
            </div>
          </div>
          <section className="panel">
            <div className="panel-heading">
              <div>
                <h3>{workflow.name}</h3>
                <p>Select a node to inspect its execution.</p>
              </div>
              <span className="mono muted">{workflow.id.slice(0, 8)}</span>
            </div>
            <div className="workflow-canvas">
              <ReactFlow
                nodes={graph.nodes}
                edges={graph.edges}
                fitView
                nodesDraggable={false}
                nodesConnectable={false}
                onNodeClick={(_e, node) => openJob(node.id)}
              >
                <Background color="#dce1e9" gap={20} />
                <Controls showInteractive={false} />
              </ReactFlow>
            </div>
          </section>
          <section className="panel">
            <div className="panel-heading">
              <h3>Execution history</h3>
              <span className="muted">{history.length} loaded runs</span>
            </div>
            <div className="workflow-history">
              {history.map((run) => (
                <button
                  key={run.id}
                  className={'history-row ' + (run.id === selected ? 'selected' : '')}
                  onClick={() => setSelected(run.id)}
                >
                  <code>{run.id.slice(0, 8)}</code>
                  <span>{date(run.created_at)}</span>
                  <Status value={run.status} />
                  <span className="muted">
                    {run.completed_nodes}/{run.total_nodes} nodes
                  </span>
                </button>
              ))}
            </div>
          </section>
        </>
      ) : (
        <>
          <div className="section-toolbar">
            <p className="muted">Orchestrate dependent jobs with directed acyclic graphs.</p>
            <button className="button primary" onClick={() => setCreating(true)}>
              <Plus size={16} />
              Create workflow
            </button>
          </div>
          {workflows.length ? (
            <div className="workflow-cards">
              {workflows.map((w) => (
                <button
                  className="workflow-card panel"
                  key={w.id}
                  onClick={() => setSelected(w.id)}
                >
                  <div className="row between">
                    <span className="tile-icon">
                      <GitBranch size={20} />
                    </span>
                    <Status value={w.status} />
                  </div>
                  <h3>{w.name}</h3>
                  <p>
                    {w.completed_nodes} of {w.total_nodes} nodes completed
                  </p>
                  <div className="progress">
                    <span
                      style={{
                        width:
                          (w.total_nodes ? (w.completed_nodes / w.total_nodes) * 100 : 0) + '%',
                      }}
                    />
                  </div>
                  <footer>
                    <code>{w.id.slice(0, 8)}</code>
                    <span>{date(w.created_at)}</span>
                  </footer>
                </button>
              ))}
            </div>
          ) : (
            <section className="panel">
              <Empty title="Build your first workflow">
                Create a DAG to run independent work in parallel and dependencies in order.
              </Empty>
            </section>
          )}
        </>
      )}
      {creating && (
        <WorkflowCreator
          token={token}
          queues={queues}
          onClose={() => setCreating(false)}
          onCreated={(id) => {
            setCreating(false);
            setSelected(id);
            refresh();
          }}
        />
      )}
    </>
  );
}
function WorkflowCreator({
  token,
  queues,
  onClose,
  onCreated,
}: {
  token: string;
  queues: Queue[];
  onClose: () => void;
  onCreated: (id: string) => void;
}) {
  const [name, setName] = useState('Data processing pipeline');
  const [queue, setQueue] = useState(queues[0]?.id ?? '');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [definition, setDefinition] = useState(
    JSON.stringify(
      {
        nodes: [
          { key: 'prepare', type: 'echo', payload: { stage: 'prepare' } },
          { key: 'transform', type: 'sum', payload: { numbers: [10, 20, 30] } },
          { key: 'validate', type: 'noop' },
          { key: 'finish', type: 'echo', payload: { stage: 'finished' } },
        ],
        edges: [
          { from: 'prepare', to: 'transform' },
          { from: 'prepare', to: 'validate' },
          { from: 'transform', to: 'finish' },
          { from: 'validate', to: 'finish' },
        ],
      },
      null,
      2,
    ),
  );
  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError('');
    try {
      const data = await api<{ data: Workflow }>(token, '/workflows', {
        ...JSON.parse(definition),
        name,
        queue_id: queue,
      });
      onCreated(data.data.id);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title="Create workflow" onClose={onClose} wide>
      <form onSubmit={(e) => void submit(e)}>
        <div className="form-row">
          <label>
            Name
            <input
              required
              maxLength={100}
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </label>
          <label>
            Queue
            <select required value={queue} onChange={(e) => setQueue(e.target.value)}>
              {queues.map((q) => (
                <option key={q.id} value={q.id}>
                  {q.name}
                </option>
              ))}
            </select>
          </label>
        </div>
        <p className="muted">
          Define nodes and edges below. Cycles and invalid references are rejected before any jobs
          are created. Each node runs its own payload; predecessor results are inspectable
          separately.
        </p>
        <label>
          Graph definition
          <textarea
            className="code-editor"
            rows={16}
            value={definition}
            onChange={(e) => setDefinition(e.target.value)}
            spellCheck={false}
          />
        </label>
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
            {busy ? 'Creating…' : 'Create draft'}
          </button>
        </footer>
      </form>
    </Modal>
  );
}
