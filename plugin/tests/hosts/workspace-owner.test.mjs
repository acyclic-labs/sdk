import assert from 'node:assert/strict';
import {mkdirSync,mkdtempSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';

test('workspace startup requires its original process readiness and retains cancellation and capture failures',async()=>{
 await Promise.all(['cancel-before','cancel-live','ready-before','exit','descriptor-without-ready','exit-capture','ready-capture','ready-async-capture'].map(async mode=>{
  const root=mkdtempSync(join(realpathSync(tmpdir()),'workspace-owner-'));
  const scope=new Processes(),cancellation=new AbortController();
  const failure=new Error('original input closed');
  const captureFailure=new Error('step=record workspace owner terminal; recovery=node --test plugin/tests/hosts/workspace-owner.test.mjs');
  let child,events,launched;
  const admission=new Promise(resolve=>{launched=resolve;});
  // The real process and stdout exercise readiness custody. Shipping owner
  // admission is also required by the installed native-host scenarios.
  const program=mode==='exit'||mode==='exit-capture'?'process.exit(3)':mode==='descriptor-without-ready'?'process.exit(0)':`
   console.log(JSON.stringify(${mode.startsWith('ready')?'{type:"workspace-owner-ready"}':'{started:true}'}));
   process.stdin.resume();`;
  const retained=running=>{
   child=running;
   events=new Spool(child.child.stdout,child.closed,{key:event=>event.started?['started']:[]});
   launched(events);
   return child;
  };
  const owner={startReportedOwned(_binary,_args,options){
   return retained(scope.start(process.execPath,['-e',program],options));}};
  try{
   if(mode==='cancel-before')cancellation.abort(failure);
   if(mode.startsWith('ready')||mode==='descriptor-without-ready'){
    mkdirSync(join(root,'.acyclic-sdk/host-files'),{recursive:true});
    writeFileSync(join(root,'.acyclic-sdk/host-files/owner.json'),'{}');
   }
   const running=withOwnedCase({processes:scope,signal:cancellation.signal,run:async()=>{
    const started=startWorkspaceOwner({scope:owner,binary:process.execPath,workspace:root,
     state:root,root,env:process.env,capture:()=>{
      if(mode==='ready-async-capture')return Promise.reject(captureFailure);
      if(mode.endsWith('capture'))throw captureFailure;
     }});
    started.catch(()=>{});
    if(mode==='cancel-live'){
     await (await admission).keyed('started');
     cancellation.abort(failure);
    }
    return started;
   }});
   if(mode.endsWith('capture'))await assert.rejects(running,error=>error===captureFailure);
   else if(mode==='ready-before')assert.equal((await running).run,child);
   else if(mode==='exit'||mode==='descriptor-without-ready')await assert.rejects(running,error=>error.name==='SpoolEnded'&&error.kind==='eof');
   else await assert.rejects(running,error=>error===failure);
   assert.equal(scope.started,mode==='cancel-before'?0:1);
   assert.equal(scope.children.size,0,'startup publication joins the same physical child');
  }finally{
   try{
    if(mode.endsWith('capture'))await assert.rejects(scope.close(),error=>error===captureFailure);
    else await scope.close();
   }finally{rmSync(root,{recursive:true});}
  }
 }));
});
