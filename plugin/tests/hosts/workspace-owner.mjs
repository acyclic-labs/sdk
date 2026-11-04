import assert from 'node:assert/strict';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Processes,Spool,SpoolEnded} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';

// Only this launched owner's announcement admits clients. Its immutable
// endpoint can outlive the previous owner, including across a restart.
export async function startWorkspaceOwner({scope,binary,workspace,state,env,root,args=[],capture=()=>{}}) {
 const owner=await scope.startReportedOwned(binary,['--workspace',workspace,'--host-workspace','--host-state-directory',state,...args],
   {cwd:workspace,env,scopeCapture:capture,scopeMountRoot:root,...(process.platform!=='win32'?{scopeReportSignal:'SIGTERM'}:{})});
 const events=new Spool(owner.child.stdout,owner.closed,
  {key:event=>event?.type==='workspace-owner-ready'?['ready']:[],unique:true});
 try{await events.keyed('ready');}
 catch(error){throw error instanceof SpoolEnded&&error.kind==='source'&&error.cause instanceof Error?error.cause:error;}
 return owner;
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 const scope=new Processes();
 const cancellation=new AbortController();
 let ended,failed,ready=false;
 const input=new Promise((yes,no)=>{
  ended=()=>{if(!ready)cancellation.abort(new Error('workspace owner input closed before readiness'));yes();};
  failed=error=>{cancellation.abort(error);no(error);};
  process.stdin.once('end',ended);process.stdin.once('error',failed);process.stdin.resume();
 });
 input.catch(()=>{});
 try {
  const [binary,workspace,state,root,...extra]=process.argv.slice(2);
  assert.ok(binary&&workspace&&state&&root&&!extra.length,'expected GRAPHCODER WORKSPACE STATE ROOT');
  await withOwnedCase({processes:scope,signal:cancellation.signal,run:async()=>{
   const owner=await startWorkspaceOwner({scope,binary,workspace,state,root,env:process.env});
   ready=true;
   console.log(JSON.stringify({event:'workspace-owner-ready'}));
   await Promise.race([input,owner.closed.then(()=>{throw new Error('workspace owner ended before input closure');})]);
  }});
 }catch(error){
  console.error('step=workspace owner readiness: '+error.message+'; recovery='+JSON.stringify(process.argv));process.exitCode=1;
 }finally{
  process.stdin.pause();process.stdin.off('end',ended);process.stdin.off('error',failed);
  await scope.close();
 }
}
