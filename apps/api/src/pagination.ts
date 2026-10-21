import { z } from 'zod';
import { HttpError } from './errors.js';
const cursorSchema=z.object({created_at:z.iso.datetime({offset:true}),id:z.uuid()});
export const listSchema=z.object({limit:z.coerce.number().int().min(1).max(100).default(30),cursor:z.string().max(300).optional(),status:z.string().max(30).optional(),q:z.string().max(100).optional(),queue_id:z.uuid().optional()}).strict();
export function decodeCursor(value?:string):{created_at:string;id:string}|null{
 if(!value)return null;
 try{return cursorSchema.parse(JSON.parse(Buffer.from(value,'base64url').toString('utf8')));}catch{throw new HttpError(400,'invalid_cursor','Invalid pagination cursor');}
}
export function page<T extends {id:string;created_at:Date}>(rows:T[],limit:number){
 const data=rows.slice(0,limit);const last=data.at(-1);
 return {data,next_cursor:rows.length>limit&&last?Buffer.from(JSON.stringify({created_at:last.created_at.toISOString(),id:last.id})).toString('base64url'):null};
}
