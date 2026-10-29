import assert from 'node:assert/strict';
import {existsSync,watch} from 'node:fs';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';

// Subscribe before the snapshot. Both native model and command acceptance use
// this one readiness barrier and the caller's existing process/tree custody.
export async function startWorkspaceOwner({scope,binary,workspace,state,env,root,capture=()=>{}}) {
 const descriptor=join(workspace,'.acyclic-sdk/host-files/owner.json');
 let ready,failed;
 const notified=new Promise((yes,no)=>{ready=yes;failed=no;});
 const watcher=watch(workspace,{recursive:true});
 watcher.on('change',()=>{if(existsSync(descriptor))ready();});watcher.on('error',failed);
 try {
  const owner=scope.startOwned(binary,['--workspace',workspace,'--host-workspace','--host-state-directory',state],
   {cwd:workspace,env,scopeMountRoot:root,...(process.platform!=='win32'?{scopeReportSignal:'SIGTERM'}:{})});
  owner.closed.then(record=>{capture(record);failed(new Error('workspace owner exited before descriptor'));},failed);
  if(existsSync(descriptor))ready();
  await notified;return owner;
 } finally {watcher.close();}
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 const scope=new Processes();
 try {
  const [binary,workspace,state,root,...extra]=process.argv.slice(2);
  assert.ok(binary&&workspace&&state&&root&&!extra.length,'expected GRAPHCODER WORKSPACE STATE ROOT');
  await withOwnedCase({processes:scope,run:async()=>{
   const owner=await startWorkspaceOwner({scope,binary,workspace,state,root,env:process.env});
   console.log(JSON.stringify({event:'workspace-owner-ready'}));
   let ended,failed;
   const input=new Promise((yes,no)=>{ended=yes;failed=no;process.stdin.once('end',ended);process.stdin.once('error',failed);process.stdin.resume();});
   try {await Promise.race([input,owner.closed.then(()=>{throw new Error('workspace owner ended before input closure');})]);}
   finally {process.stdin.pause();process.stdin.off('end',ended);process.stdin.off('error',failed);}
  }});
 }catch(error){
  console.error('step=workspace owner readiness: '+error.message+'; recovery='+JSON.stringify(process.argv));process.exitCode=1;
 }finally{await scope.close();}
}
