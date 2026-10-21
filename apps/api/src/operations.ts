import { Router } from 'express';
import type pg from 'pg';
import { z } from 'zod';
import { queueSchema } from '../../../packages/contracts/src/index.js';
import { HttpError } from './errors.js';
import { transaction } from './database.js';

export async function snapshot(pool:pg.Pool,project:string){
 const [counts,latency,recent]=await Promise.all([
  pool.query<{status:string;count:string}>('SELECT status,count(*) FROM jobs WHERE project_id=$1 GROUP BY status',[project]),
  pool.query(`SELECT count(*)::integer AS completed,
   COALESCE(percentile_cont(0.5) WITHIN GROUP(ORDER BY extract(epoch FROM (finished_at-created_at))*1000),0) AS p50_ms,
   COALESCE(percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch FROM (finished_at-created_at))*1000),0) AS p95_ms,
   COALESCE(percentile_cont(0.99) WITHIN GROUP(ORDER BY extract(epoch FROM (finished_at-created_at))*1000),0) AS p99_ms
   FROM jobs WHERE project_id=$1 AND status='COMPLETED' AND finished_at>now()-interval '24 hours'`,[project]),
  pool.query(`SELECT date_trunc('minute',finished_at) AS minute,count(*)::integer AS completed FROM jobs
   WHERE project_id=$1 AND status='COMPLETED' AND finished_at>now()-interval '30 minutes' GROUP BY 1 ORDER BY 1`,[project])
 ]);
 return {counts:Object.fromEntries(counts.rows.map(row=>[row.status,Number(row.count)])),latency:latency.rows[0],throughput:recent.rows,measured_at:new Date().toISOString()};
}
export function operationsRouter(pool:pg.Pool){
 const router=Router();
 router.get('/project',(_req,res)=>res.json({data:res.locals.project}));
 router.post('/queues',async(req,res)=>{
  const input=queueSchema.parse(req.body);
  const queue=await transaction(pool,async client=>{
   await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[res.locals.project.id]);
   const count=await client.query<{count:string}>('SELECT count(*) FROM queues WHERE project_id=$1',[res.locals.project.id]);
   if(Number(count.rows[0]!.count)>=32)throw new HttpError(429,'queue_quota','Project has reached its 32 queue quota');
   return (await client.query('INSERT INTO queues(project_id,name,concurrency_limit) VALUES($1,$2,$3) RETURNING *',[res.locals.project.id,input.name,input.concurrency_limit])).rows[0];
  });res.status(201).json({data:queue});
 });
 router.get('/queues',async(_req,res)=>{
  const result=await pool.query(`SELECT q.id,q.name,q.concurrency_limit,q.created_at,count(j.id)::integer AS total,
   count(j.id) FILTER(WHERE j.status IN ('READY','PENDING','RETRY_WAIT'))::integer AS backlog,
   count(j.id) FILTER(WHERE j.status IN ('RUNNING','CANCEL_REQUESTED'))::integer AS running,
   count(j.id) FILTER(WHERE j.status='COMPLETED')::integer AS completed,
   count(j.id) FILTER(WHERE j.status='DEAD_LETTERED')::integer AS dead_lettered
   FROM queues q LEFT JOIN jobs j ON j.queue_id=q.id WHERE q.project_id=$1 GROUP BY q.id ORDER BY q.created_at`,[res.locals.project.id]);
  res.json({data:result.rows});
 });
 router.get('/queues/:id/stats',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const queue=await pool.query('SELECT id,name,concurrency_limit FROM queues WHERE id=$1 AND project_id=$2',[id,res.locals.project.id]);
  if(!queue.rowCount)throw new HttpError(404,'queue_not_found','Queue not found');
  const stats=await pool.query('SELECT status,count(*)::integer FROM jobs WHERE queue_id=$1 GROUP BY status',[id]);
  res.json({data:{...queue.rows[0],counts:stats.rows}});
 });
 router.get('/workers',async(_req,res)=>{
  const result=await pool.query(`SELECT w.id,w.name,w.capacity,w.draining,w.heartbeat_at,w.started_at,
   w.heartbeat_at>clock_timestamp()-interval '10 seconds' AS online,
   (SELECT count(*)::integer FROM jobs j WHERE j.worker_id=w.id AND j.project_id=$1 AND j.status IN ('RUNNING','CANCEL_REQUESTED')) AS active_jobs
   FROM workers w WHERE EXISTS(SELECT 1 FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.worker_id=w.id AND j.project_id=$1)
   ORDER BY w.heartbeat_at DESC LIMIT 100`,[res.locals.project.id]);res.json({data:result.rows});
 });
 router.get('/workers/:id',async(req,res)=>{
  const id=z.uuid().parse(req.params.id);
  const worker=await pool.query(`SELECT w.id,w.name,w.capacity,w.draining,w.heartbeat_at,w.started_at FROM workers w WHERE w.id=$1
   AND EXISTS(SELECT 1 FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.worker_id=w.id AND j.project_id=$2)`,[id,res.locals.project.id]);
  if(!worker.rowCount)throw new HttpError(404,'worker_not_found','Worker not found');res.json({data:worker.rows[0]});
 });
 router.get('/overview',async(_req,res)=>res.json({data:await snapshot(pool,res.locals.project.id)}));
 router.get('/metrics',async(_req,res)=>{
  const data=await snapshot(pool,res.locals.project.id);
  const lines=['# HELP taskforge_jobs Current durable jobs by status','# TYPE taskforge_jobs gauge'];
  for(const [status,count] of Object.entries(data.counts))lines.push(`taskforge_jobs{status="${status}"} ${count}`);
  lines.push('# HELP taskforge_completion_latency_ms End-to-end completion latency over the last 24 hours','# TYPE taskforge_completion_latency_ms gauge');
  for(const percentile of ['p50','p95','p99'])lines.push(`taskforge_completion_latency_ms{percentile="${percentile}"} ${data.latency[`${percentile}_ms`]}`);
  res.type('text/plain; version=0.0.4').send(lines.join('\n')+'\n');
 });
 const streams=new Map<string,number>();
 router.get('/events',async(req,res)=>{
  const project:string=res.locals.project.id;
  if((streams.get(project)??0)>=10)throw new HttpError(429,'stream_limit','Project has reached its 10 live stream limit');
  // Reserve synchronously, before the first await, so concurrent opens cannot bypass the cap.
  streams.set(project,(streams.get(project)??0)+1);
  let closed=false,busy=false,ticks=0;
  let timer:ReturnType<typeof setInterval>|undefined,expires:ReturnType<typeof setTimeout>|undefined;
  function release(){
   if(closed)return;closed=true;clearInterval(timer);clearTimeout(expires);
   const remaining=(streams.get(project)??1)-1;if(remaining)streams.set(project,remaining);else streams.delete(project);
  }
  res.once('close',release);
  let initial;
  try{initial=await snapshot(pool,project);}catch(error){release();throw error;}
  if(res.destroyed){release();return;}
  res.set({'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive','X-Accel-Buffering':'no'});res.flushHeaders();
  res.write('event: snapshot\ndata: '+JSON.stringify(initial)+'\n\n');
  timer=setInterval(()=>{
   if(busy||closed)return;busy=true;ticks++;
   void (async()=>{
    try{
     if(ticks%15===0){
      const auth=await pool.query('SELECT id FROM projects WHERE id=$1 AND token_hash=$2',[project,res.locals.tokenHash]);
      if(!auth.rowCount){res.end();return;}
     }
     const data=await snapshot(pool,project);
     if(res.writableLength>65536){res.end();return;}
     if(!res.destroyed&&!closed)res.write('event: snapshot\ndata: '+JSON.stringify(data)+'\n\n');
    }catch{res.end();}finally{busy=false;}
   })();
  },2000);
  expires=setTimeout(()=>res.end(),15*60*1000);
 });
 return router;
}
