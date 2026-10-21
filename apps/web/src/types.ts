import type { JobStatus, JobType } from '../../../packages/contracts/src/index';
export type { JobStatus, JobType };
export interface Job {
  id: string;
  type: JobType;
  status: JobStatus;
  queue_id: string;
  priority: number;
  attempt_no: number;
  max_attempts: number;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
  error: string | null;
  payload?: unknown;
  result?: unknown;
  attempts?: Attempt[];
}
export interface Attempt {
  attempt_no: number;
  status: string;
  worker_id: string;
  assigned_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
}
export interface Queue {
  id: string;
  name: string;
  concurrency_limit: number;
  backlog: number;
  running: number;
  completed: number;
  dead_lettered: number;
  total: number;
}
export interface Worker {
  id: string;
  name: string;
  capacity: number;
  active_jobs: number;
  online: boolean;
  draining: boolean;
  heartbeat_at: string;
}
export interface Workflow {
  id: string;
  name: string;
  status: string;
  total_nodes: number;
  completed_nodes: number;
  created_at: string;
  finished_at: string | null;
  nodes?: WorkflowNode[];
  edges?: { parent_id: string; child_id: string }[];
}
export interface WorkflowNode {
  id: string;
  node_key: string;
  type: JobType;
  status: JobStatus;
}
export interface Overview {
  counts: Partial<Record<JobStatus, number>>;
  latency: { completed: number; p50_ms: number; p95_ms: number; p99_ms: number };
  throughput: { minute: string; completed: number }[];
  measured_at: string;
}
export interface Log {
  id: string;
  level: string;
  message: string;
  attempt_no: number;
  created_at: string;
}
export interface Page<T> {
  data: T[];
  next_cursor: string | null;
}
export interface Project {
  id: string;
  name: string;
}
