import assert from 'node:assert/strict';
import {mkdirSync,mkdtempSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';

test('workspace startup cancellation and early exit settle the original watch and process owner',async()=>{
 for(const mode of ['cancel-before','cancel-live','ready-before','exit']){
  const root=mkdtempSync(join(realpathSync(tmpdir()),'workspace-owner-'));
  const scope=new Processes(),cancellation=new AbortController();
  const failure=new Error('original input closed');
  let child,events;
  // Real child programs produce their own output and exit; no host response is fabricated.
  const owner={startOwned(_binary,_args,options){
   child=scope.start(process.execPath,['-e',mode==='exit'?'process.exit(3)':
    'console.log(JSON.stringify({started:true}));process.stdin.resume()'],options);
   events=new Spool(child.child.stdout,child.closed,{key:event=>event.started?['started']:[]});
   return child;
  }};
  try{
   if(mode==='cancel-before')cancellation.abort(failure);
   if(mode==='ready-before'){
    mkdirSync(join(root,'.acyclic-sdk/host-files'),{recursive:true});
    writeFileSync(join(root,'.acyclic-sdk/host-files/owner.json'),'{}');
   }
   const running=withOwnedCase({processes:scope,run:async()=>{
    const started=startWorkspaceOwner({scope:owner,binary:process.execPath,workspace:root,
     state:root,root,env:process.env,signal:cancellation.signal});
    started.catch(()=>{});
    if(mode==='cancel-live'){
     await events.keyed('started');
     cancellation.abort(failure);
    }
    return started;
   }});
   if(mode==='ready-before')assert.equal((await running).run,child);
   else if(mode==='exit')await assert.rejects(running,/workspace owner exited before descriptor/);
   else await assert.rejects(running,error=>error===failure);
   assert.equal(scope.started,mode==='cancel-before'?0:1);
   assert.equal(scope.children.size,0,'startup publication joins the same physical child');
  }finally{await scope.close();rmSync(root,{recursive:true});}
 }
});
