import { z } from 'zod';

export const jobStatuses = ['PENDING','READY','RUNNING','COMPLETED','RETRY_WAIT','CANCEL_REQUESTED','CANCELLED','DEAD_LETTERED'] as const;
export type JobStatus = typeof jobStatuses[number];
export const jobTypes = ['noop','echo','sum','sleep','fail'] as const;
export type JobType = typeof jobTypes[number];
const payloads = {
  noop: z.object({}).strict(),
  echo: z.record(z.string(), z.json()),
  sum: z.object({ numbers: z.array(z.number().finite().min(-1e100).max(1e100)).max(10000) }).strict(),
  sleep: z.object({ duration_ms: z.number().int().min(0).max(3600000) }).strict(),
  fail: z.object({ message: z.string().max(1000).default('intentional handler failure'), fail_until_attempt: z.number().int().min(0).max(100000).default(100000) }).strict(),
};
const jobFields = {
  type: z.enum(jobTypes), payload: z.record(z.string(),z.json()).default({}),
  priority: z.number().int().min(-100).max(100).default(0),
  max_attempts: z.number().int().min(1).max(20).default(3),
  timeout_ms: z.number().int().min(100).max(3600000).default(30000),
  available_at: z.iso.datetime({offset:true}).optional(),
};
function validatePayload(value: {type: JobType; payload: Record<string,unknown>}, ctx: z.RefinementCtx) {
  const parsed = payloads[value.type].safeParse(value.payload);
  if (!parsed.success) for (const issue of parsed.error.issues) ctx.addIssue({code:'custom',path:['payload',...issue.path],message:issue.message});
}
export const jobSchema = z.object({ queue_id: z.uuid(), ...jobFields }).strict().superRefine(validatePayload)
  .transform(value => ({...value,payload:payloads[value.type].parse(value.payload)}));
export type JobInput = z.infer<typeof jobSchema>;
export const queueSchema = z.object({name:z.string().trim().min(1).max(100),concurrency_limit:z.number().int().min(1).max(256).default(4)}).strict();
export interface Edge { from: string; to: string }
export function topologicalOrder(keys: string[], edges: Edge[]): string[] {
  const indegree = new Map(keys.map(key=>[key,0]));
  if(indegree.size!==keys.length) throw new Error('duplicate node key');
  const children = new Map(keys.map(key=>[key,[] as string[]]));
  const seen = new Set<string>();
  for(const {from,to} of edges) {
    if(!indegree.has(from)||!indegree.has(to)) throw new Error('edge references unknown node');
    const key=JSON.stringify([from,to]);
    if(seen.has(key)) throw new Error('duplicate edge');
    seen.add(key); children.get(from)!.push(to); indegree.set(to,indegree.get(to)!+1);
  }
  const ready=keys.filter(key=>indegree.get(key)===0);
  const order: string[]=[];
  // An index avoids Array.shift's O(V) movement on every iteration.
  for(let i=0;i<ready.length;i++) {
    const key=ready[i]!; order.push(key);
    for(const child of children.get(key)!) {
      const degree=indegree.get(child)!-1; indegree.set(child,degree);
      if(degree===0) ready.push(child);
    }
  }
  if(order.length!==keys.length) throw new Error('workflow contains a cycle');
  return order;
}
const nodeSchema=z.object({key:z.string().regex(/^[A-Za-z0-9_-]{1,64}$/),...jobFields}).strict().superRefine(validatePayload)
 .transform(value=>({...value,payload:payloads[value.type].parse(value.payload)}));
export const workflowSchema=z.object({
  name:z.string().trim().min(1).max(100),queue_id:z.uuid(),nodes:z.array(nodeSchema).min(1).max(200),
  edges:z.array(z.object({from:z.string(),to:z.string()}).strict()).max(2000),
}).strict().superRefine((value,ctx)=>{
  try{topologicalOrder(value.nodes.map(node=>node.key),value.edges);}catch(error){ctx.addIssue({code:'custom',path:['edges'],message:(error as Error).message});}
});
export type WorkflowInput=z.infer<typeof workflowSchema>;
export function canonicalJson(value: unknown): string {
  if(Array.isArray(value)) return '['+value.map(canonicalJson).join(',')+']';
  if(value!==null&&typeof value==='object') return '{'+Object.entries(value).filter(([,v])=>v!==undefined).sort(([a],[b])=>a<b?-1:a>b?1:0).map(([k,v])=>JSON.stringify(k)+':'+canonicalJson(v)).join(',')+'}';
  return JSON.stringify(value) ?? 'null';
}
// A canonical representation is shared with the API; hashing uses node:crypto there.
export const requestHash=canonicalJson;
