/* M1.3a research-only, read-only remote evidence resolver. No persistent credentials. */
'use strict';
(function(g){
class SourceError extends Error{constructor(code,message){super(message);this.name='SourceError';this.code=code;}}
const fail=(code,msg)=>{throw new SourceError(code,msg)};
const idOK=x=>typeof x==='string'&&/^[A-Za-z0-9_-]{1,200}$/.test(x);
const hashOK=x=>typeof x==='string'&&/^[a-f0-9]{64}$/.test(x);
const digest=p=>PG.sha(new Blob([PG.canonical(p)]));
async function bind(p,index){
 await PG.verifyExchange(p);
 if(!index||index.format!=='pokegrading.source-index.v1'||index.provider!=='google-drive'||['projectId','specimenId','sessionId','revision'].some(k=>index[k]!==p[k])||index.projectSha256!==await digest(p))fail('project_binding','Source index does not belong to this exact project revision');
 const refs=new Map();for(const a of PG.payloads(p)){const prior=refs.get(a.sha256);if(prior&&prior.bytes!==a.bytes)fail('index_invalid','Conflicting payload sizes');refs.set(a.sha256,a);}
 if(!Array.isArray(index.entries)||index.entries.length!==refs.size)fail('index_invalid','Incomplete source index');
 const byHash=new Map(), ids=new Set();for(const x of index.entries){if(!x||!hashOK(x.sha256)||!idOK(x.fileId)||!refs.has(x.sha256)||refs.get(x.sha256).bytes!==x.bytes||byHash.has(x.sha256)||ids.has(x.fileId))fail('index_invalid','Invalid or duplicate source binding');byHash.set(x.sha256,Object.freeze({fileId:x.fileId,...refs.get(x.sha256)}));ids.add(x.fileId);}
 return byHash;
}
function driveTransport({getToken,fetchImpl=fetch}){return async a=>{
 let token;try{token=await getToken();}catch{fail('auth_required','Google authorization required');}if(typeof token!=='string'||!token.trim())fail('auth_required','Google authorization required');
 let res;try{res=await fetchImpl('https://www.googleapis.com/drive/v3/files/'+encodeURIComponent(a.fileId)+'?alt=media',{headers:{Authorization:'Bearer '+token},cache:'no-store',redirect:'error',credentials:'omit',referrerPolicy:'no-referrer'});}catch{fail('network_error','Could not reach authorized source');}
 if(res.status===401)fail('auth_required','Authorization expired');if(res.status===403)fail('access_denied','Source access denied or quota restricted');if(res.status===404)fail('source_unavailable','Source unavailable or not shared with this app');if(res.status===429)fail('rate_limited','Provider rate limit; retry later');if(res.status!==200)fail('provider_error','Source provider returned HTTP '+res.status);
 // Full object only: no partial content may be presented as a verified original.
 let bytes;try{const reader=res.body.getReader();let total=0,parts=[];try{for(;;){const {done,value}=await reader.read();if(done)break;total+=value.byteLength;if(total>a.bytes){await reader.cancel();fail('size_mismatch','Source exceeds expected length');}parts.push(value);}}finally{reader.releaseLock();}bytes=new Blob(parts);}catch(e){if(e instanceof SourceError)throw e;fail('read_error','Source response could not be read');}
 return bytes;
};}
async function create(p,index,readRemote){const refs=await bind(p,index);return Object.freeze({read:async h=>{const a=refs.get(h);if(!a)fail('unknown_source','Unknown source reference');const b=await readRemote(a);if(!(b instanceof Blob))fail('read_error','No source bytes returned');if(b.size!==a.bytes)fail('size_mismatch','Source length differs: '+a.name);if(await PG.sha(b)!==h)fail('hash_mismatch','Source digest differs: '+a.name);return b.slice(0,b.size,a.mime||'application/octet-stream');},count:refs.size});}
 g.PGRemote={create,driveTransport,digest,SourceError};
})(globalThis);
