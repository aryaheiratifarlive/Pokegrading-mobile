/* M1.2a recovery candidate: same database; no project migration. */
'use strict';
async function preflightProjectCommit(store,p,blobs,expected){
  await PG.verifyExchange(p);
  const current=await store.get(p.projectId), result=PG.planCommit(current,p,expected);
  const refs=new Map(PG.payloads(p).map(a=>[a.sha256,a]));
  for(const [h,b] of blobs){const a=refs.get(h);if(!a||!(b instanceof Blob)||b.size!==a.bytes||await PG.sha(b)!==h)throw Error('Unexpected or altered staged payload');}
  const read=async h=>result==='duplicate'?store.read(p.projectId,h):blobs.get(h)||await store.read(p.projectId,h);
  for(const [h,a] of refs){const b=await read(h);if(!b||b.size!==a.bytes||await PG.sha(b)!==h)throw Error('Missing or altered commit payload: '+a.name);}
  await PRC.replay(p,read);
  return {current,result};
}
// Read failures are reported separately from proven byte mismatches.
async function inspectStoredPayloads(store,p,progress=()=>{}){
 await PG.verifyExchange(p);const refs=[...new Map(PG.payloads(p).map(a=>[a.sha256,a])).values()],results=[];
 for(const a of refs){let status='verified',detail='';try{const b=await store.read(p.projectId,a.sha256);if(!b)status='missing';else if(b.size!==a.bytes){status='size_mismatch';detail=`${b.size} vs ${a.bytes}`;}else if(await PG.sha(b)!==a.sha256)status='hash_mismatch';}catch(e){status='read_error';detail=e.message||String(e);}results.push({name:a.name,sha256:a.sha256,bytes:a.bytes,status,detail});progress(results.length,refs.length,results.at(-1));}
 return results;
}
// Exact same manifest, verified backup, only failed bytes replaced; no metadata writes.
async function recoverIdenticalProject(store,restored,progress=()=>{}){
 const p=restored.project;await PG.verifyExchange(p);const current=await store.get(p.projectId);
 if(!current||PG.canonical(current)!==PG.canonical(p))throw Error('Recovery requires the exact existing project and revision; nothing changed.');
 const refs=new Map(PG.payloads(p).map(a=>[a.sha256,a]));
 if(restored.blobs.size!==refs.size)throw Error('Recovery backup payload count mismatch');
 // Validate the entire supplied backup before the first write, independently of the disk state.
 for(const [h,a] of refs){const b=restored.blobs.get(h);if(!b||b.size!==a.bytes||await PG.sha(b)!==h)throw Error('Invalid recovery source: '+a.name);}
 await PRC.replay(p,h=>restored.blobs.get(h));
 const before=await inspectStoredPayloads(store,p,progress),repaired=[];
 for(const a of before){if(a.status==='verified')continue;const bytes=await restored.blobs.get(a.sha256).arrayBuffer();await store.repairPayload(p,a.sha256,bytes);const b=await store.read(p.projectId,a.sha256);if(!b||b.size!==a.bytes||await PG.sha(b)!==a.sha256)throw Error('Recovery readback failed: '+a.name+'; keep the backup.');repaired.push(a.name);progress(repaired.length,before.filter(x=>x.status!=='verified').length,{name:a.name,status:'repaired'});}
 const after=await inspectStoredPayloads(store,p,progress);
 if(after.some(x=>x.status!=='verified'))throw Error('Recovery incomplete; keep original backup.');
 return {before,after,repaired};
}
function openProjectStore(){return new Promise((resolve,reject)=>{
 const req=indexedDB.open('pokegrading-m1-1b-candidate-v1',1);
 req.onupgradeneeded=()=>{req.result.createObjectStore('projects',{keyPath:'projectId'});req.result.createObjectStore('media');};req.onerror=()=>reject(req.error);req.onblocked=()=>reject(Error('Close other candidate tabs to open storage'));
 req.onsuccess=()=>{const db=req.result;db.onversionchange=()=>db.close();
 function get(store,key){return new Promise((res,rej)=>{const tx=db.transaction(store),r=key===undefined?tx.objectStore(store).getAll():tx.objectStore(store).get(key);tx.oncomplete=()=>res(r.result);tx.onabort=tx.onerror=()=>rej(tx.error||Error('Read failed'));});}
 const api={list:()=>get('projects'),get:id=>get('projects',id),read:async(id,h)=>{const value=await get('media',id+':'+h);return value instanceof ArrayBuffer?new Blob([value]):value;},repairPayload:async(p,h,bytes)=>{
 const a=PG.payloads(p).find(x=>x.sha256===h);if(!a||!(bytes instanceof ArrayBuffer)||bytes.byteLength!==a.bytes||await PG.sha(new Blob([bytes]))!==h)throw Error('Invalid recovery payload');
 return new Promise((res,rej)=>{const t=db.transaction(['projects','media'],'readwrite');let error;const r=t.objectStore('projects').get(p.projectId);r.onsuccess=()=>{try{if(PG.canonical(r.result??null)!==PG.canonical(p))throw Error('Project changed; recovery stopped');t.objectStore('media').put(bytes,p.projectId+':'+h);}catch(e){error=e;t.abort();}};t.oncomplete=()=>res();t.onabort=t.onerror=()=>rej(error||t.error||Error('Recovery transaction failed'));});
 },commit:async(p,blobs,expected)=>{
 const pre=await preflightProjectCommit(api,p,blobs,expected);
 return new Promise((res,rej)=>{const t=db.transaction(['projects','media'],'readwrite'),ps=t.objectStore('projects'),ms=t.objectStore('media');let result,error;
 const abort=e=>{error=e;try{t.abort()}catch{}};
 const r=ps.get(p.projectId);r.onsuccess=()=>{try{if(PG.canonical(r.result??null)!==PG.canonical(pre.current??null))throw Error('Project changed during verification; reopen and retry');result=PG.planCommit(r.result,p,expected);if(result==='duplicate')return;
 const all=ps.getAll();all.onsuccess=()=>{try{if(all.result.some(x=>x.projectId!==p.projectId&&(x.specimenId===p.specimenId||x.sessionId===p.sessionId)))throw Error('Physical identity already assigned to another project');
 for(const [h,b] of blobs)ms.put(b,p.projectId+':'+h);ps.put(p);}catch(e){abort(e)}};
 }catch(e){abort(e)}};
 t.oncomplete=()=>res(result);t.onabort=t.onerror=()=>rej(error||t.error||Error('Atomic commit aborted'));
 });}};resolve(api);
 };
});}
