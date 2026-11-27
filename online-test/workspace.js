/* M1.3e.1 integrated online workspace. No automatic writes. */
'use strict';
(function(g){
const err=(c,m)=>new PGRemote.SourceError(c,m);
function create({broker,fetchImpl=fetch}){
 let adapter=null,journal=null,identity=null,approved=false,control=false,head=null,stage=null,epoch=0;
 function clear(){epoch++;adapter=journal=identity=head=stage=null;approved=control=false;broker.disconnect();}
 function ready(){if(!approved||!adapter||!broker.getToken())throw err('auth_required','Connect and confirm the Google account first');}
 function current(){ready();if(!head)throw err('project_missing','Open a committed project first');return head;}
 function guard(e){if(e!==epoch)throw err('session_closed','Workspace closed during operation');}
 async function connect(){clear();const e=epoch;try{await broker.connect();guard(e);adapter=PGDriveJournal.create({getToken:broker.getToken,fetchImpl});journal=PGJournal.create(adapter);identity=await adapter.identify();guard(e);return {...identity};}catch(x){clear();throw x;}}
 function confirm(id){if(!identity||id!==identity.id||!broker.getToken())throw err('account_unknown','Account confirmation does not match');approved=true;}
 async function discover(){ready();const e=epoch;const ids=await adapter.discover();guard(e);const out=[];for(const id of ids){try{const s=await journal.inspect(id);guard(e);out.push({id,state:s.state,name:s.head?.project.name??null,copy:s.head?.project.copy??null,revision:s.head?.project.revision??null});}catch(x){guard(e);out.push({id,state:'blocked',code:x.code||'validation_error'});}}return out;}
 async function testConnection(){ready();const e=epoch,b=PGDriveLab.sample(),key=await PG.sha(b);const existing=await adapter.list('payload','TEST_ONLY_M13E1',key);const r=existing.length?existing[0]:await adapter.create({kind:'payload',projectId:'TEST_ONLY_M13E1',key},b);const fresh=PGDriveJournal.create({getToken:broker.getToken,fetchImpl});if((await fresh.identify()).id!==identity.id||await PG.sha(await fresh.get(r.id))!==key)throw err('hash_mismatch','Control verification failed');guard(e);control=true;return {format:'pokegrading.online-control.v1',fileId:r.id,bytes:b.size,sha256:key};}
 async function prepare(blob){ready();if(!control)throw err('control_required','Run the synthetic connection check first');stage=null;const e=epoch,r=await PG.restore(blob);guard(e);const refs=new Map(PG.payloads(r.project).map(a=>[a.sha256,a]));const rows=await adapter.list('payload',r.project.projectId);const matched=new Set(rows.filter(x=>refs.has(x.key)&&refs.get(x.key).bytes===x.bytes).map(x=>x.key));guard(e);stage=r;return {stagedMatches:matched.size,name:r.project.name,copy:r.project.copy,projectId:r.project.projectId,payloads:r.blobs.size,bytes:[...r.blobs.values()].reduce((n,b)=>n+b.size,0)};}
 async function publish(id,progress=()=>{}){ready();if(!control||!stage||stage.project.projectId!==id)throw err('save_not_confirmed','Prepare and explicitly confirm this backup');const e=epoch,r=stage;const result=await journal.save(r.project,h=>r.blobs.get(h),null,progress);guard(e);head=result.head;stage=null;return {status:result.status,project:structuredClone(head.project),commit:head.key};}
 async function open(id){ready();head=null;const e=epoch,n=await journal.open(id);guard(e);head=n;return structuredClone(n.project);}
 async function evidence(h){const n=current(),e=epoch;const a=PG.payloads(n.project).find(x=>x.sha256===h);if(!a)throw err('unknown_source','Source not in committed project');const b=await n.resolver.read(h);guard(e);return {asset:{...a},blob:b};}
 async function verifyAll(progress=()=>{}){const n=current(),e=epoch,rows=[];for(const a of n.index.entries){guard(e);try{await n.resolver.read(a.sha256);guard(e);rows.push({sha256:a.sha256,status:'verified'});}catch(x){guard(e);rows.push({sha256:a.sha256,status:x.code||'read_error'});}progress(rows.length,n.index.entries.length);}return {format:'pokegrading.online-validation.v1',commit:n.key,projectId:n.project.projectId,revision:n.project.revision,verified:rows.filter(x=>x.status==='verified').length,total:rows.length,rows};}
 async function exportBackup(){const n=current(),e=epoch,b=await PG.exportProject(n.project,h=>n.resolver.read(h));guard(e);return b;}
 return Object.freeze({connect,confirm,disconnect:clear,discover,testConnection,prepare,publish,open,evidence,verifyAll,exportBackup});
}
g.PGOnline={create};})(globalThis);
