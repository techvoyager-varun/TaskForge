import {cpus,totalmem,platform,release,arch,availableParallelism} from 'node:os';
import {performance} from 'node:perf_hooks';
import {randomUUID} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const base=process.env.TASKFORGE_URL??'http://127.0.0.1:3000';
const token=process.env.TASKFORGE_TOKEN;
const queue=process.env.TASKFORGE_QUEUE;
const count=Number(process.env.BENCH_COUNT??80);
const concurrency=Number(process.env.BENCH_CONCURRENCY??8);
const duration=Number(process.env.BENCH_DURATION_MS??0);
const handler=process.env.BENCH_HANDLER??(duration?'sleep':'noop');
if(!['noop','sleep','sum'].includes(handler))throw new Error('BENCH_HANDLER must be noop, sleep, or sum');
const payload=handler==='sum'?{numbers:Array.from({length:10000},(_,i)=>i+1)}:handler==='sleep'?{duration_ms:duration}:{};
if(!token||!queue)throw new Error('Set TASKFORGE_TOKEN and TASKFORGE_QUEUE. TASKFORGE_URL is optional.');
if(!Number.isInteger(count)||count<1||count>100)throw new Error('BENCH_COUNT must be 1..100 (one API result page)');
if(!Number.isInteger(concurrency)||concurrency<1||concurrency>32)throw new Error('BENCH_CONCURRENCY must be 1..32');
if(!Number.isInteger(duration)||duration<0||duration>30000)throw new Error('BENCH_DURATION_MS must be 0..30000');
const run=randomUUID();let throttled=0;
async function request(path,body,key){
 for(let retry=0;retry<4;retry++){
  const response=await fetch(base+'/api/v1'+path,{method:body===undefined?'GET':'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json',...(key?{'Idempotency-Key':key}:{})},...(body===undefined?{}:{body:JSON.stringify(body)})});
  if(response.status===429){throttled++;await new Promise(r=>setTimeout(r,Math.min(60000,Number(response.headers.get('Retry-After')??5)*1000)));continue;}
  if(!response.ok)throw new Error('HTTP '+response.status+': '+await response.text());
  return response.json();
 }
 throw new Error('API rate limit did not clear');
}
const ids=new Set();const submitted=[];let next=0;const start=performance.now();const startedAt=new Date().toISOString();
await Promise.all(Array.from({length:concurrency},async()=>{
 while(next<count){const index=next++;const began=performance.now();const response=await request('/jobs',{queue_id:queue,type:handler,payload,max_attempts:3,timeout_ms:60000},run+':'+index);ids.add(response.data.id);submitted.push(performance.now()-began);}
}));
const submissionSeconds=(performance.now()-start)/1000;
let jobs=[];const deadline=performance.now()+180000;
while(performance.now()<deadline){
 const response=await request('/jobs?limit=100&queue_id='+queue);jobs=response.data.filter(job=>ids.has(job.id));
 if(jobs.length===count&&jobs.every(job=>job.status==='COMPLETED'))break;
 if(jobs.some(job=>['DEAD_LETTERED','CANCELLED'].includes(job.status)))throw new Error('A benchmark job failed or was cancelled');
 await new Promise(r=>setTimeout(r,200));
}
if(jobs.length!==count||jobs.some(job=>job.status!=='COMPLETED'))throw new Error('Benchmark completion deadline exceeded; no success measurement emitted');
const wallSeconds=(performance.now()-start)/1000;
if(handler==='sum'){const check=await request('/jobs/'+jobs[0].id);if(check.data.result?.sum!==50005000)throw new Error('Computed result did not match the sum workload');}
const percentile=(values,p)=>{const sorted=[...values].sort((a,b)=>a-b);return sorted[Math.ceil(sorted.length*p)-1]??0;};
const latency=jobs.map(job=>new Date(job.finished_at).getTime()-new Date(job.created_at).getTime());
const result={run,started_at:startedAt,workload:handler==='noop'?'synthetic noop':handler==='sum'?'sum 10000 numbers':'synthetic sleep',job_count:count,submission_concurrency:concurrency,duration_ms:duration,submission_seconds:submissionSeconds,submission_jobs_per_second:count/submissionSeconds,observed_wall_seconds:wallSeconds,observed_completion_jobs_per_second:count/wallSeconds,completion_latency_ms:{p50:percentile(latency,.5),p95:percentile(latency,.95),p99:percentile(latency,.99)},submission_latency_ms:{p50:percentile(submitted,.5),p95:percentile(submitted,.95),p99:percentile(submitted,.99)},extra_attempts:jobs.reduce((n,j)=>n+j.attempt_no-1,0),rate_limited_requests:throttled,machine:{platform:platform(),release:release(),arch:arch(),cpu:cpus()[0]?.model,logical_cores:availableParallelism(),ram_gib:Math.round(totalmem()/1024**3*10)/10,node:process.version},caveat:'Single short run, HTTP-observed completion rate includes polling delay. No claim of sustained capacity. Use a dedicated empty queue.'};
if(process.env.BENCH_OUTPUT)await writeFile(process.env.BENCH_OUTPUT,JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result,null,2));
