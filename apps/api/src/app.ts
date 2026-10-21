import express from 'express';
import helmet from 'helmet';
import { rateLimit } from 'express-rate-limit';
import { createHash,randomUUID } from 'node:crypto';
import type pg from 'pg';
import { z } from 'zod';
import pino from 'pino';
import { jobsRouter } from './jobs.js';
import { workflowsRouter } from './workflows.js';
import { operationsRouter } from './operations.js';
import { HttpError } from './errors.js';
const logger=pino({level:process.env.LOG_LEVEL??'info'});
export function createApp(pool:pg.Pool,webDirectory?:string){
 const app=express();
 app.disable('x-powered-by');
 app.use(helmet({contentSecurityPolicy:{directives:{'script-src':["'self'"],'style-src':["'self'","'unsafe-inline'"],'connect-src':["'self'"],'img-src':["'self'",'data:'],'font-src':["'self'"]}}}));
 app.use((req,res,next)=>{
  const id=randomUUID();res.locals.requestId=id;res.set('X-Request-Id',id);
  const start=performance.now();res.on('finish',()=>logger.info({request_id:id,method:req.method,path:req.path,status:res.statusCode,duration_ms:Math.round(performance.now()-start)},'request'));
  next();
 });
 app.get('/health/live',(_req,res)=>res.json({status:'ok'}));
 app.get('/health/ready',async(_req,res)=>{await pool.query('SELECT 1');res.json({status:'ready'});});
 app.use('/api',rateLimit({windowMs:60000,limit:300,standardHeaders:'draft-8',legacyHeaders:false,message:{error:{code:'rate_limited',message:'Too many requests; retry later'}}}));
 app.use(express.json({limit:'128kb',strict:true}));
 app.use('/api/v1',async(req,res,next)=>{
  const auth=req.get('Authorization');
  const match=auth?.match(/^Bearer (tf_[a-f0-9]{64})$/);
  if(!match)throw new HttpError(401,'unauthorized','A project Bearer token is required');
  const tokenHash=createHash('sha256').update(match[1]!).digest('hex');
  const project=await pool.query<{id:string;name:string}>('SELECT id,name FROM projects WHERE token_hash=$1',[tokenHash]);
  if(!project.rows[0])throw new HttpError(401,'unauthorized','Invalid project token');
  res.locals.project=project.rows[0];next();
 });
 app.use('/api/v1/jobs',jobsRouter(pool));
 app.use('/api/v1/workflows',workflowsRouter(pool));
 app.use('/api/v1',operationsRouter(pool));
 app.use('/api',(_req,_res,next)=>next(new HttpError(404,'not_found','Endpoint not found')));
 if(webDirectory){app.use(express.static(webDirectory,{index:false}));app.get('/{*path}',(_req,res)=>res.sendFile('index.html',{root:webDirectory}));}
 app.use((_req,_res,next)=>next(new HttpError(404,'not_found','Endpoint not found')));
 app.use((error:unknown,_req:express.Request,res:express.Response,_next:express.NextFunction)=>{
  if(res.headersSent){res.end();return;}
  if(error instanceof z.ZodError){res.status(400).json({error:{code:'validation_error',message:'Request validation failed',details:error.issues},request_id:res.locals.requestId});return;}
  if(error instanceof HttpError){res.status(error.status).json({error:{code:error.code,message:error.message,details:error.details},request_id:res.locals.requestId});return;}
  const code=(error as {code?:string}).code;const status=(error as {status?:number}).status;
  if(code==='23505'){res.status(409).json({error:{code:'conflict',message:'Resource already exists'}});return;}
  if(status===400||status===413){res.status(status).json({error:{code:'invalid_body',message:status===413?'Request exceeds 128 KB':'Invalid JSON body'}});return;}
  logger.error({request_id:res.locals.requestId,code,message:error instanceof Error?error.message:'Unknown error'},'request failed');
  res.status(503).json({error:{code:'service_unavailable',message:'Request could not be confirmed; retry with the same idempotency key'},request_id:res.locals.requestId});
 });
 return app;
}
