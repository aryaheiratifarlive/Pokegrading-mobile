/* M1.3c research: append-only project journal. Provider abstraction, NOT a live Drive adapter. */
'use strict';
(function(g){
const F='pokegrading.remote-commit.v1', I='pokegrading.source-index.v1', MAX=1024*1024;
const fail=(code,msg)=>{throw new PGRemote.SourceError(code,msg)};
const hashOK=x=>typeof x==='string'&&/^[a-f0-9]{64}$/.test(x);
const idOK=x=>typeof x==='string'&&/^[A-Za-z0-9_-]{1,200}$/.test(x);
const json=x=>new Blob([PG.canonical(x)],{type:'application/json'});
const digest=x=>PG.sha(json(x));
function requireOK(x,c,m){if(!x)fail(c,m);}
function same(a,b){return PG.canonical(a)===PG.canonical(b)}
function create(provider){
 // Contract: list(kind,projectId,key?) returns complete visible results or throws; get(id) returns Blob; create({kind,projectId,key},Blob) creates only, never overwrites. No CAS assumption.
 async function listed(kind,projectId,key){const rows=await provider.list(kind,projectId,key);requireOK(Array.isArray(rows)&&rows.length<=512&&rows.every(x=>x&&idOK(x.id)&&x.kind===kind&&x.projectId===projectId&&hashOK(x.key)&&(key===undefined||x.key===key))&&new Set(rows.map(x=>x.id)).size===rows.length,'listing_invalid','Incomplete or invalid listing');return rows;}
 async function checked(id,bytes,h){const b=await provider.get(id);requireOK(b instanceof Blob&&b.size===bytes,'size_mismatch','Stored object length mismatch');requireOK(await PG.sha(b)===h,'hash_mismatch','Stored object digest mismatch');return b;}
 async function parse(row){const b=await provider.get(row.id);requireOK(b instanceof Blob&&b.size>0&&b.size<=MAX,'manifest_invalid','Invalid manifest length');requireOK(await PG.sha(b)===row.key,'manifest_invalid','Manifest digest mismatch');let m;try{m=JSON.parse(await b.text())}catch{fail('manifest_invalid','Invalid manifest JSON')};requireOK(m&&m.format===F&&m.project?.projectId===row.projectId&&(m.parent===null||hashOK(m.parent)),'manifest_invalid','Invalid manifest identity');await PG.verifyExchange(m.project);const r=await PGRemote.create(m.project,m.index,a=>provider.get(a.fileId));return {key:row.key,project:m.project,index:m.index,parent:m.parent,resolver:r};}
 async function inspect(projectId){const nodes=new Map();for(const row of await listed('commit',projectId)){const n=await parse(row);if(!nodes.has(n.key))nodes.set(n.key,n);}
 if(!nodes.size)return {state:'empty',head:null,nodes};
 let roots=0;const parents=new Set();for(const n of nodes.values()){if(n.parent===null){roots++;continue}const p=nodes.get(n.parent);requireOK(p,'history_incomplete','Missing parent commit; no head selected');validateSuccessor(p.project,n.project);parents.add(n.parent);}
 const heads=[...nodes.values()].filter(n=>!parents.has(n.key));return {state:roots===1&&heads.length===1?'ready':'conflict',head:roots===1&&heads.length===1?heads[0]:null,nodes,heads};}
 function validateSuccessor(p,n){requireOK(['projectId','specimenId','sessionId','name','printing','copy','created'].every(k=>p[k]===n[k]),'identity_conflict','Project identity or metadata changed');try{PG.planCommit(p,n,p.revision)}catch(e){fail('revision_conflict',e.message)}}
 function checkBase(s,expected){requireOK(s.state!=='conflict','revision_conflict','Competing revisions; explicit reconciliation required');requireOK((s.head?.key??null)===expected,'stale_base','Project changed since it was opened');}
 async function open(projectId){const s=await inspect(projectId);requireOK(s.state==='ready',s.state==='empty'?'project_missing':'revision_conflict','No unambiguous committed project');const n=s.head;await PRC.replay(n.project,h=>n.resolver.read(h));return n;}
 async function stage(p,a,read,prior){const old=prior?.index.entries.find(x=>x.sha256===a.sha256);if(old){await checked(old.fileId,a.bytes,a.sha256);return old;}
 const b=await read(a.sha256);requireOK(b instanceof Blob&&b.size===a.bytes,'size_mismatch','Source absent or length mismatch before upload');requireOK(await PG.sha(b)===a.sha256,'hash_mismatch','Source changed before upload');
 const rows=await listed('payload',p.projectId,a.sha256);if(rows.length){const row=rows.sort((a,b)=>a.id.localeCompare(b.id))[0];await checked(row.id,a.bytes,a.sha256);return {sha256:a.sha256,bytes:a.bytes,fileId:row.id};}
 const row=await provider.create({kind:'payload',projectId:p.projectId,key:a.sha256},b);requireOK(row&&idOK(row.id),'upload_unknown','Missing upload receipt; reconcile before retry');await checked(row.id,a.bytes,a.sha256);return {sha256:a.sha256,bytes:a.bytes,fileId:row.id};}
 async function save(p,read,expected=null,progress=()=>{}){p=structuredClone(p);await PG.verifyExchange(p);const s=await inspect(p.projectId);
 // Replaying exact current snapshot is a no-op even with its prior base token; verify the existing bytes.
 if(s.state==='ready'&&same(s.head.project,p)){for(const a of s.head.index.entries)await s.head.resolver.read(a.sha256);return {status:'duplicate',head:s.head};}
 checkBase(s,expected);if(s.head)validateSuccessor(s.head.project,p);
 const refs=[...new Map(PG.payloads(p).map(a=>[a.sha256,a])).values()].sort((a,b)=>a.sha256.localeCompare(b.sha256));const entries=[];for(const a of refs){progress(entries.length,refs.length,a.name||a.sha256.slice(0,12),a.bytes);try{entries.push(await stage(p,a,read,s.head));}catch(e){fail(e.code||'upload_unknown',`Payload ${entries.length+1}/${refs.length} (${a.name||a.sha256.slice(0,12)}, ${a.bytes} bytes): ${e.message}`);}}progress(refs.length,refs.length,'Publishing project record',0);
 const index={format:I,provider:'google-drive',projectId:p.projectId,specimenId:p.specimenId,sessionId:p.sessionId,revision:p.revision,projectSha256:await PGRemote.digest(p),entries};const r=await PGRemote.create(p,index,a=>provider.get(a.fileId));await PRC.replay(p,h=>r.read(h));
 const m={format:F,parent:expected,project:p,index},b=json(m),key=await PG.sha(b);requireOK(b.size<=MAX,'manifest_invalid','Manifest too large');checkBase(await inspect(p.projectId),expected);
 let receipt;try{receipt=await provider.create({kind:'commit',projectId:p.projectId,key},b);}catch{fail('publish_unknown','Commit outcome unknown; reconcile by listing before retrying');}requireOK(receipt&&idOK(receipt.id),'publish_unknown','Missing commit receipt');await checked(receipt.id,b.size,key);
 const after=await inspect(p.projectId);requireOK(after.state==='ready'&&after.head.key===key,'publication_conflict','Published revision not the sole visible head; reconcile before continuing');return {status:'published',head:after.head};}
 return Object.freeze({inspect,open,save});
}
g.PGJournal={create};})(globalThis);
