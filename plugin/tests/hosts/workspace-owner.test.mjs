import assert from 'node:assert/strict';
import {mkdirSync,mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
const nativeReady=JSON.parse(readFileSync(new URL('../../../scripts/graphcoder-e2e/fixtures/native-report-ready.json',import.meta.url))).ready;

test('workspace startup cancellation and early exit settle the original watch and process owner',async()=>{
 for(const mode of ['cancel-before','cancel-live','ready-before','exit','descriptor-without-ready']){
  const root=mkdtempSync(join(realpathSync(tmpdir()),'workspace-owner-'));
  const scope=new Processes(),cancellation=new AbortController();
  const failure=new Error('original input closed');
  let child,events,launched;
  const admission=new Promise(resolve=>{launched=resolve;});
  // This isolates the real process/pipe/watch mechanism. Only the ready frame
  // is replayed from the retained native capture, rebound to this launch.
  // Full workspace-owner acceptance uses the actual shipping executable.
  const program=mode==='exit'?'process.exit(3)':mode==='descriptor-without-ready'?'process.exit(0)':`
   const fs=require('node:fs'),net=require('node:net');
   const frame=JSON.stringify({...${JSON.stringify(nativeReady)},generation:process.env.ACYCLIC_REPORT_GENERATION})+'\\n';
   if(process.env.ACYCLIC_REPORT_PIPE)net.connect(process.env.ACYCLIC_REPORT_PIPE).end(frame);
   else if(process.env.ACYCLIC_REPORT_FD){const fd=Number(process.env.ACYCLIC_REPORT_FD);fs.writeSync(fd,frame);fs.closeSync(fd);}
   console.log(JSON.stringify({started:true}));process.stdin.resume();`;
  const retained=running=>{
   child=running;
   events=new Spool(child.child.stdout,child.closed,{key:event=>event.started?['started']:[]});
   launched(events);
   return child;
  };
  const owner={startOwned(_binary,_args,options){return retained(scope.start(process.execPath,['-e',program],options));},
   async startReportedOwned(_binary,_args,options){return retained(await scope.startReported(process.execPath,['-e',program],options));}};
  try{
   if(mode==='cancel-before')cancellation.abort(failure);
   if(mode==='ready-before'||mode==='descriptor-without-ready'){
    mkdirSync(join(root,'.acyclic-sdk/host-files'),{recursive:true});
    writeFileSync(join(root,'.acyclic-sdk/host-files/owner.json'),'{}');
   }
   const running=withOwnedCase({processes:scope,signal:cancellation.signal,run:async()=>{
    const started=startWorkspaceOwner({scope:owner,binary:process.execPath,workspace:root,
     state:root,root,env:process.env});
    started.catch(()=>{});
    if(mode==='cancel-live'){
     await (await admission).keyed('started');
     cancellation.abort(failure);
    }
    return started;
   }});
   if(mode==='ready-before')assert.equal((await running).run,child);
   else if(mode==='exit')await assert.rejects(running,/workspace owner exited before descriptor|report capability ended/);
   else if(mode==='descriptor-without-ready')await assert.rejects(running,{code:'REPORT_CONTROL_ENDED'});
   else await assert.rejects(running,error=>error===failure);
   assert.equal(scope.started,mode==='cancel-before'?0:1);
   assert.equal(scope.children.size,0,'startup publication joins the same physical child');
  }finally{await scope.close();rmSync(root,{recursive:true});}
 }
});
