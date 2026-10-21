import { Router } from 'express';
import { createHash } from 'node:crypto';
import type pg from 'pg';
import { z } from 'zod';
import { canonicalJson,jobSchema,jobStatuses } from '../../../packages/contracts/src/index.js';
import { HttpError } from './errors.js';
import { transaction } from './database.js';
import { decodeCursor,listSchema,page } from './pagination.js';
interface JobRow extends pg.QueryResultRow {id:string;created_at:Date;request_hash:string|null}
export function jobsRouter(pool:pg.Pool){
 const router=Router();
 router.post('/',async(req,res)=>{
  const input=jobSchema.parse(req.body);const project:string=res.locals.project.id;
  const key=req.get('Idempotency-Key');
  if(key&&!/^[\x21-\x7e]{1,128}$/.test(key))throw new HttpError(400,'invalid_idempotency_key','Idempotency-Key must contain 1..128 visible ASCII characters');
  const fingerprint=createHash('sha256').update(canonicalJson(input)).digest('hex');
  const result=await transaction(pool,async client=>{
   // Serializes the project quota and same-key lookup without serializing other projects.
   await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[project]);
   if(key){const previous=await client.query<JobRow>('SELECT * FROM jobs WHERE project_id=$1 AND idempotency_key=$2',[project,key]);
    if(previous.rows[0]){
     if(previous.rows[0].request_hash!==fingerprint)throw new HttpError(409,'idempotency_conflict','This key belongs to a different request');
     return {job:previous.rows[0],created:false};
    }
   }
   const queue=await client.query('SELECT id FROM queues WHERE id=$1 AND project_id=$2',[input.queue_id,project]);
   if(!queue.rowCount)throw new HttpError(404,'queue_not_found','Queue not found');
   const active=await client.query<{count:string}>("SELECT count(*) FROM jobs WHERE project_id=$1 AND status IN ('PENDING','READY','RUNNING','RETRY_WAIT','CANCEL_REQUESTED')",[project]);
   if(Number(active.rows[0]!.count)>=10000)throw new HttpError(429,'project_quota','Project has reached its 10000 active-job quota');
   const {rows:[job]}=await client.query<JobRow>(`INSERT INTO jobs(project_id,queue_id,type,payload,priority,max_attempts,timeout_ms,available_at,idempotency_key,request_hash,status)
    VALUES($1,$2,$3,$4,$5,$6,$7,COALESCE($8::timestamptz,now()),$9,$10,CASE WHEN $8::timestamptz>now() THEN 'PENDING' ELSE 'READY' END) RETURNING *`,
    [project,input.queue_id,input.type,JSON.stringify(input.payload),input.priority,input.max_attempts,input.timeout_ms,input.available_at??null,key??null,key?fingerprint:null]);
   await client.query("INSERT INTO audit_events(project_id,action,resource_id) VALUES($1,'job.submit',$2)",[project,job!.id]);
   return {job:job!,created:true};
  });
  res.status(result.created?201:200).json({data:result.job});
 });
 router.get('/',async(req,res)=>{
  const filter=listSchema.parse(req.query);const cursor=decodeCursor(filter.cursor);
  if(filter.status)z.enum(jobStatuses).parse(filter.status);
  const rows=await pool.query<JobRow>(`SELECT id,type,status,priority,queue_id,workflow_id,node_key,attempt_no,max_attempts,created_at,updated_at,finished_at,error
   FROM jobs WHERE project_id=$1 AND ($2::text IS NULL OR status=$2) AND ($3::uuid IS NULL OR queue_id=$3)
   AND ($4::text IS NULL OR type ILIKE '%'||$4||'%' OR id::text ILIKE $4||'%')
   AND ($5::timestamptz IS NULL OR (created_at,id)<($5::timestamptz,$6::uuid)) ORDER BY created_at DESC,id DESC LIMIT $7`,
   [res.locals.project.id,filter.status??null,filter.queue_id??null,filter.q??null,cursor?.created_at??null,cursor?.id??null,filter.limit+1]);
  res.json(page(rows.rows,filter.limit));
 });
 router.get('/:id',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const {rows:[job]}=await pool.query<JobRow>('SELECT * FROM jobs WHERE id=$1 AND project_id=$2',[id,res.locals.project.id]);
  if(!job)throw new HttpError(404,'job_not_found','Job not found');
  const attempts=await pool.query('SELECT * FROM job_attempts WHERE job_id=$1 ORDER BY attempt_no',[id]);
  res.json({data:{...job,attempts:attempts.rows}});
 });
 for(const action of ['cancel','retry'] as const){
  router.post(`/:id/${action}`,async(req,res)=>{
   const id=z.uuid().parse(req.params.id);
   const {rows:[row]}=await pool.query<{state:string|null}>(`SELECT ${action}_job($1::uuid,$2::uuid) AS state`,[res.locals.project.id,id]);
   if(!row?.state)throw new HttpError(404,'job_not_found','Job not found');
   if(row.state==='CONFLICT')throw new HttpError(409,'retry_conflict','Only cancelled or dead-lettered standalone jobs can be retried; workflow retries create a new execution');
   res.json({data:{id,status:row.state}});
  });
 }
 router.get('/:id/logs',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const filter=z.object({after:z.coerce.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER).default(0),limit:z.coerce.number().int().min(1).max(200).default(100)}).strict().parse(req.query);
  const owned=await pool.query('SELECT id FROM jobs WHERE id=$1 AND project_id=$2',[id,res.locals.project.id]);
  if(!owned.rowCount)throw new HttpError(404,'job_not_found','Job not found');
  const result=await pool.query('SELECT * FROM execution_logs WHERE job_id=$1 AND id>$2 ORDER BY id LIMIT $3',[id,filter.after,filter.limit]);
  res.json({data:result.rows,next_after:result.rows.at(-1)?.id??String(filter.after)});
 });
 return router;
}
