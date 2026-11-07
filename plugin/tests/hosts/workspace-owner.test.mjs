import assert from 'node:assert/strict';
import {mkdirSync,mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
const nativeReady=JSON.parse(readFileSync(new URL('../../../scripts/graphcoder-e2e/fixtures/native-report-ready.json',import.meta.url))).ready;
const workspaceReady=JSON.parse(readFileSync(new URL('../../../scripts/graphcoder-e2e/fixtures/workspace-owner-ready.json',import.meta.url))).ready;

test('workspace startup joins serving readiness, cancellation and the original process owner',async()=>{
 await Promise.all(['cancel-before','cancel-live','ready-before','exit','descriptor-without-ready','stale-descriptor','control-without-ready','exit-capture','ready-capture','ready-async-capture'].map(async mode=>{
  const root=mkdtempSync(join(realpathSync(tmpdir()),'workspace-owner-'));
  const scope=new Processes(),cancellation=new AbortController();
  const failure=new Error('original input closed');
  const captureFailure=new Error('step=record workspace owner terminal; recovery=node --test plugin/tests/hosts/workspace-owner.test.mjs');
  const originalCapture=error=>error===captureFailure||error.cause===captureFailure;
  let child,events,launched;
  const admission=new Promise(resolve=>{launched=resolve;});
  // This isolates the real process/pipe mechanism. Both ready frames come
  // from native captures; only the report generation is rebound to this launch.
  // Full workspace-owner acceptance uses the actual shipping executable.
  const program=mode==='exit'?'process.exit(3)':mode==='descriptor-without-ready'?'process.exit(0)':`
   const fs=require('node:fs'),net=require('node:net');
   const frame=JSON.stringify({...${JSON.stringify(nativeReady)},generation:process.env.ACYCLIC_REPORT_GENERATION})+'\\n';
   const closed=()=>{${mode==='exit-capture'?'process.exit(3)':`console.log(JSON.stringify({started:true}));${mode.startsWith('ready')?'console.log('+JSON.stringify(JSON.stringify(workspaceReady))+');':''}process.stdin.resume();`}};
   const data=${mode==='control-without-ready'?"''":'frame'};
   if(process.env.ACYCLIC_REPORT_PIPE)net.connect(process.env.ACYCLIC_REPORT_PIPE).end(data,closed);
   else if(process.env.ACYCLIC_REPORT_FD){const fd=Number(process.env.ACYCLIC_REPORT_FD);fs.writeSync(fd,data);fs.closeSync(fd);closed();}`;
  const retained=running=>{
   child=running;
   events=new Spool(child.child.stdout,child.closed,{key:event=>event.started?['started']:[]});
   launched(events);
   return child;
  };
  const owner={async startReportedOwned(_binary,_args,options){
   return retained(await scope.startReported(process.execPath,['-e',program],options));}};
  try{
   if(mode==='cancel-before')cancellation.abort(failure);
   if(mode.startsWith('ready')||mode==='descriptor-without-ready'||mode==='stale-descriptor'){
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
    if(mode==='stale-descriptor'){
     await (await admission).keyed('started');
     await child.reportReady;
     child.input.end();
    }
    return started;
   }});
   if(mode.endsWith('capture'))await assert.rejects(running,originalCapture);
   else if(mode==='ready-before')assert.equal((await running).run,child);
   else if(mode==='exit'||mode==='descriptor-without-ready'||mode==='control-without-ready'||mode==='stale-descriptor')
    await assert.rejects(running,error=>error.code==='REPORT_CONTROL_ENDED'||(error.name==='SpoolEnded'&&error.kind==='eof'));
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
