/* M1.3b: synthetic-only Drive connection lab. Never uploads card evidence. */
'use strict';
(function(g){
const SCOPE='https://www.googleapis.com/auth/drive.file', FMT='pokegrading.drive-lab-receipt.v1', SAMPLE='PokeGrading M1.3b synthetic connection control. No card or personal data.\n';
const err=(code,msg)=>new PGRemote.SourceError(code,msg);const idOK=s=>typeof s==='string'&&/^[A-Za-z0-9_-]{1,200}$/.test(s);const clientOK=s=>typeof s==='string'&&/^[0-9]+-[A-Za-z0-9_-]+\.apps\.googleusercontent\.com$/.test(s);
function broker({oauth,clientId,now=()=>Date.now()}){
 if(!clientOK(clientId)||!oauth?.initTokenClient)throw err('config','A valid web OAuth client ID and Google Identity library are required');
 let token=null,until=0,epoch=0,pending=null;
 const clear=()=>{token=null;until=0;epoch++;if(pending){const p=pending;pending=null;p.reject(err('session_closed','Connection closed'));}};
 const getToken=()=>{if(token&&now()<until-30000)return token;token=null;until=0;return null;};
 function connect(){if(pending)return Promise.reject(err('busy','Authorization already in progress'));clear();const e=epoch;return new Promise((resolve,reject)=>{pending={reject};function finish(error,r){if(e!==epoch||!pending)return;pending=null;if(error){reject(error);return;}const scopes=String(r?.scope||'').trim().split(/\s+/);const life=Number(r?.expires_in);if(r?.error||typeof r?.access_token!=='string'||!r.access_token||!Number.isFinite(life)||life<=30||scopes.length!==1||scopes[0]!==SCOPE){reject(err('auth_denied','Drive file authorization not granted as requested'));return;}token=r.access_token;until=now()+life*1000;resolve(true);}
 try{oauth.initTokenClient({client_id:clientId,scope:SCOPE,include_granted_scopes:false,callback:r=>finish(null,r),error_callback:()=>finish(err('popup_failed','Google authorization popup failed or was closed'))}).requestAccessToken({prompt:'select_account'});}catch{finish(err('popup_failed','Google authorization could not start'));}});}
 function revoke(){const t=getToken();clear();if(!t)return Promise.reject(err('auth_required','Reconnect or remove access in Google Account settings'));return new Promise((resolve,reject)=>{try{oauth.revoke(t,r=>r?.successful===true?resolve(true):reject(err('revoke_failed','Revocation not confirmed; use Google Account permissions')));}catch{reject(err('revoke_failed','Revocation not confirmed'));}});}
 return Object.freeze({connect,getToken,disconnect:clear,revoke});
}
const sample=()=>new Blob([SAMPLE],{type:'text/plain'});
async function validateReceipt(r){const b=sample();if(!r||Object.keys(r).sort().join(',')!=='bytes,fileId,format,sha256'||r.format!==FMT||!idOK(r.fileId)||r.bytes!==b.size||r.sha256!==await PG.sha(b))throw err('receipt_invalid','Not a valid synthetic connection receipt');return Object.freeze({...r});}
function lab({getToken,fetchImpl=fetch}){
 const transport=PGRemote.driveTransport({getToken,fetchImpl});
 async function verify(r){r=await validateReceipt(r);const b=await transport(r);if(b.size!==r.bytes)throw err('size_mismatch','Control length mismatch');if(await PG.sha(b)!==r.sha256)throw err('hash_mismatch','Control digest mismatch');return true;}
 async function create(){const t=await getToken();if(!t)throw err('auth_required','Connect to Drive first');const b=sample(),h=await PG.sha(b),boundary='pg_'+crypto.randomUUID().replaceAll('-','');const metadata={name:'PokeGrading_M1_3b_TEST_ONLY_'+crypto.randomUUID()+'.txt',mimeType:'text/plain',appProperties:{purpose:'pokegrading-m1-3b-synthetic-control',sha256:h}};const body=new Blob(['--'+boundary+'\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n',JSON.stringify(metadata),'\r\n--'+boundary+'\r\nContent-Type: text/plain\r\n\r\n',b,'\r\n--'+boundary+'--\r\n']);let res;try{res=await fetchImpl('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,size,sha256Checksum',{method:'POST',headers:{Authorization:'Bearer '+t,'Content-Type':'multipart/related; boundary='+boundary},body,cache:'no-store',redirect:'error',credentials:'omit',referrerPolicy:'no-referrer'});}catch{throw err('upload_unknown','Upload outcome unknown; inspect Drive before retrying');}if(!res.ok)throw err(res.status===401?'auth_required':res.status===403?'access_denied':res.status===429?'rate_limited':'upload_failed','Upload returned HTTP '+res.status+'; inspect Drive before retrying');let m;try{m=await res.json();}catch{throw err('upload_unknown','Upload receipt unreadable; inspect Drive before retrying');}if(!idOK(m.id)||String(m.size)!==String(b.size)||(m.sha256Checksum&&m.sha256Checksum!==h))throw err('upload_unknown','Upload receipt differs; inspect Drive before retrying');return validateReceipt({format:FMT,fileId:m.id,bytes:b.size,sha256:h});}
 return Object.freeze({create,verify});
}
g.PGDriveLab={broker,lab,validateReceipt,sample,SCOPE,clientOK};
})(globalThis);
