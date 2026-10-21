import { Router } from 'express';
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { z } from 'zod';
import { workflowSchema,type WorkflowInput } from '../../../packages/contracts/src/index.js';
import { transaction } from './database.js';
import { HttpError } from './errors.js';
import { listSchema,decodeCursor,page } from './pagination.js';
interface WorkflowRow extends pg.QueryResultRow {id:string;name:string;created_at:Date;status:string}
async function createWorkflow(client:pg.PoolClient,project:string,input:WorkflowInput){
 await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[project]);
 const queue=await client.query('SELECT id FROM queues WHERE id=$1 AND project_id=$2',[input.queue_id,project]);
 if(!queue.rowCount)throw new HttpError(404,'queue_not_found','Queue not found');
 const active=await client.query<{count:string}>("SELECT count(*) FROM jobs WHERE project_id=$1 AND status IN ('PENDING','READY','RUNNING','RETRY_WAIT','CANCEL_REQUESTED')",[project]);
 if(Number(active.rows[0]!.count)+input.nodes.length>10000)throw new HttpError(429,'project_quota','Project active-job quota exceeded');
 const {rows:[workflow]}=await client.query<WorkflowRow>('INSERT INTO workflows(project_id,name) VALUES($1,$2) RETURNING *',[project,input.name]);
 const ids=new Map(input.nodes.map(node=>[node.key,randomUUID()]));
 for(const node of input.nodes)await client.query(`INSERT INTO jobs(id,project_id,queue_id,workflow_id,node_key,type,payload,priority,max_attempts,timeout_ms,available_at,status)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11::timestamptz,now()),'PENDING')`,
  [ids.get(node.key),project,input.queue_id,workflow!.id,node.key,node.type,JSON.stringify(node.payload),node.priority,node.max_attempts,node.timeout_ms,node.available_at??null]);
 for(const edge of input.edges)await client.query('INSERT INTO workflow_edges(workflow_id,parent_id,child_id) VALUES($1,$2,$3)',[workflow!.id,ids.get(edge.from),ids.get(edge.to)]);
 await client.query("INSERT INTO audit_events(project_id,action,resource_id) VALUES($1,'workflow.create',$2)",[project,workflow!.id]);
 return workflow!;
}
export function workflowsRouter(pool:pg.Pool){
 const router=Router();
 router.post('/',async(req,res)=>res.status(201).json({data:await transaction(pool,client=>createWorkflow(client,res.locals.project.id,workflowSchema.parse(req.body)))}));
 router.get('/',async(req,res)=>{
  const filter=listSchema.parse(req.query);const cursor=decodeCursor(filter.cursor);
  if(filter.status)z.enum(['DRAFT','RUNNING','COMPLETED','FAILED','CANCELLED']).parse(filter.status);
  const result=await pool.query<WorkflowRow>(`SELECT f.*,count(j.id)::integer AS total_nodes,count(j.id) FILTER(WHERE j.status='COMPLETED')::integer AS completed_nodes
   FROM workflows f LEFT JOIN jobs j ON j.workflow_id=f.id WHERE f.project_id=$1 AND ($2::text IS NULL OR f.status=$2)
   AND ($3::timestamptz IS NULL OR (f.created_at,f.id)<($3::timestamptz,$4::uuid)) GROUP BY f.id ORDER BY f.created_at DESC,f.id DESC LIMIT $5`,
   [res.locals.project.id,filter.status??null,cursor?.created_at??null,cursor?.id??null,filter.limit+1]);
  res.json(page(result.rows,filter.limit));
 });
 router.get('/:id',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const {rows:[workflow]}=await pool.query('SELECT * FROM workflows WHERE id=$1 AND project_id=$2',[id,res.locals.project.id]);
  if(!workflow)throw new HttpError(404,'workflow_not_found','Workflow not found');
  const nodes=await pool.query('SELECT id,node_key,type,status,queue_id,priority,max_attempts,timeout_ms,payload,available_at,created_at,finished_at FROM jobs WHERE workflow_id=$1 ORDER BY created_at,node_key',[id]);
  const edges=await pool.query('SELECT parent_id,child_id FROM workflow_edges WHERE workflow_id=$1',[id]);
  res.json({data:{...workflow,nodes:nodes.rows,edges:edges.rows}});
 });
 router.post('/:id/run',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const state=await transaction(pool,async client=>{
   const {rows:[workflow]}=await client.query('SELECT * FROM workflows WHERE id=$1 AND project_id=$2 FOR UPDATE',[id,res.locals.project.id]);
   if(!workflow)throw new HttpError(404,'workflow_not_found','Workflow not found');
   if(workflow.status!=='DRAFT')throw new HttpError(409,'workflow_state','Only draft workflows can start');
   await client.query("UPDATE workflows SET status='RUNNING' WHERE id=$1",[id]);
   await client.query("INSERT INTO audit_events(project_id,action,resource_id) VALUES($1,'workflow.run',$2)",[res.locals.project.id,id]);
   return 'RUNNING';
  });res.status(202).json({data:{id,status:state}});
 });
 router.post('/:id/cancel',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  await transaction(pool,async client=>{
   const {rows:[workflow]}=await client.query('SELECT * FROM workflows WHERE id=$1 AND project_id=$2 FOR UPDATE',[id,res.locals.project.id]);
   if(!workflow)throw new HttpError(404,'workflow_not_found','Workflow not found');
   const jobs=await client.query<{id:string}>('SELECT id FROM jobs WHERE workflow_id=$1 ORDER BY id',[id]);
   for(const job of jobs.rows)await client.query('SELECT cancel_job($1::uuid,$2::uuid)',[res.locals.project.id,job.id]);
   if(workflow.status==='DRAFT')await client.query("UPDATE workflows SET status='CANCELLED' WHERE id=$1",[id]);
  });res.status(202).json({data:{id}});
 });
 router.get('/:id/executions',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const owned=await pool.query('SELECT id FROM workflows WHERE id=$1 AND project_id=$2',[id,res.locals.project.id]);
  if(!owned.rowCount)throw new HttpError(404,'workflow_not_found','Workflow not found');
  const result=await pool.query(`SELECT a.*,j.node_key FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.workflow_id=$1 ORDER BY a.assigned_at LIMIT 500`,[id]);
  res.json({data:result.rows});
 });
 return router;
}
