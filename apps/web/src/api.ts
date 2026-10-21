export class ApiError extends Error{constructor(public status:number,message:string){super(message);}}
export async function api<T>(token:string,path:string,body?:unknown,extra:Record<string,string>={}):Promise<T>{
 const response=await fetch('/api/v1'+path,{method:body===undefined?'GET':'POST',headers:{Authorization:'Bearer '+token,...(body===undefined?{}:{'Content-Type':'application/json'}),...extra},...(body===undefined?{}:{body:JSON.stringify(body)})});
 const data=await response.json();
 if(!response.ok)throw new ApiError(response.status,data.error?.details?.[0]?.message??data.error?.message??'Request failed');
 return data as T;
}
