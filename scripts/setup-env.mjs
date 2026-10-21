import { randomBytes } from 'node:crypto';
import { writeFileSync, existsSync } from 'node:fs';
if(existsSync('.env')){console.log('.env already exists; preserved existing configuration.');}
else{
 const lines=['POSTGRES_PASSWORD','API_DB_PASSWORD','SCHEDULER_DB_PASSWORD','WORKER_DB_PASSWORD'].map(name=>name+'='+randomBytes(32).toString('hex'));
 lines.push('HTTP_PORT=3000');writeFileSync('.env',lines.join('\n')+'\n',{mode:0o600,flag:'wx'});
 console.log('Created .env with independent random database credentials. Keep this file private.');
}
