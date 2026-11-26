/* M1.1b isolated candidate database. No migration of previous stores. */
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
function openProjectStore(){return new Promise((resolve,reject)=>{
 const req=indexedDB.open('pokegrading-m1-1b-candidate-v1',1);
 req.onupgradeneeded=()=>{req.result.createObjectStore('projects',{keyPath:'projectId'});req.result.createObjectStore('media');};req.onerror=()=>reject(req.error);req.onblocked=()=>reject(Error('Close other candidate tabs to open storage'));
 req.onsuccess=()=>{const db=req.result;db.onversionchange=()=>db.close();
 function get(store,key){return new Promise((res,rej)=>{const tx=db.transaction(store),r=key===undefined?tx.objectStore(store).getAll():tx.objectStore(store).get(key);tx.oncomplete=()=>res(r.result);tx.onabort=tx.onerror=()=>rej(tx.error||Error('Read failed'));});}
 const api={list:()=>get('projects'),get:id=>get('projects',id),read:(id,h)=>get('media',id+':'+h),commit:async(p,blobs,expected)=>{
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
