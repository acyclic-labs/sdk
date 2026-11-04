// Model-free native sessions use the real sandbox and original recorded output.
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {chmod,mkdir,readFile,writeFile} from 'node:fs/promises';
import {existsSync,readFileSync,readSync,writeSync} from 'node:fs';
import {createConnection,createServer} from 'node:net';
import {delimiter,isAbsolute,join,relative,resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {McpClient} from '../../shared/mcp-client.mjs';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment,scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {commandCancellation,withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
import {nativeHost} from './native-host.mjs';

const driver=fileURLToPath(import.meta.url);
const recovery='node plugin/tests/hosts/managed-session.mjs ACYCLIC GRAPHCODER CODEX lifecycle|recursive|resume';
const scope=new Processes();
const cancellation=['host','effects','descendant'].includes(process.argv[2])?undefined:commandCancellation();
const fail=detail=>new Error('step=managed native session: '+detail+'; recovery='+recovery);
const options=()=>({cwd:process.cwd(),env:process.env});
const tool=(client,name,reference,prompt)=>client.request(name,{reference,...(prompt===undefined?{}:{prompt})})
 .then(result=>{
  assert.ok(!result.isError,JSON.stringify(result));
  const value=JSON.parse(result.content[0].text);
  assert.ok(value&&typeof value==='object'&&!Array.isArray(value),'expected a coordination result object');
  return value;
 });
function connect(child){
 return new McpClient(child,{name:'native-session-conformance',version:'1',recovery});
}
async function closeBridge(bridge){
 bridge.input.end();
 const result=await bridge.closed;
 assert.equal(result.code,0,result.stderr);
}
async function holdForCancel(){
 const gate=createConnection({host:'127.0.0.1',port:Number(process.env.FIXTURE_BARRIER)});
 await once(gate,'connect');
 gate.write(JSON.stringify({cwd:process.cwd()})+'\n');
 // Only the original process-tree cancellation may retire this held child.
 await Promise.race([once(gate,'data'),once(gate,'close')]);
 throw fail('readiness owner closed or sent unexpected input');
}

async function effects(prompt){
 const agents=await scope.run(process.env.FIXTURE_ACYCLIC,['agents'],options());
 assert.equal(agents.code,0,JSON.stringify({step:'sandbox workspace discovery',
  code:agents.code,signal:agents.signal,stdout:agents.stdout,stderr:agents.stderr}));
 await writeFile('.volumes/home/native-private.txt',prompt);
 await assert.rejects(writeFile(process.env.FIXTURE_OUTSIDE,'denied'),
  error=>['EACCES','EPERM','EROFS'].includes(error.code));
 assert.ok(!existsSync(process.env.FIXTURE_OUTSIDE),'sandbox allowed an outside write');
 await writeFile(prompt+'.txt','native');
}

async function host(){
 const args=process.argv.slice(3),workspace=process.env.FIXTURE_WORKSPACE;
 if(!args.includes('exec')){
  assert.deepEqual(args,['--sandbox','workspace-write','--ask-for-approval','never','--add-dir',workspace]);
  assert.equal(process.cwd(),process.env.ACYCLIC_WORKSPACE);
  assert.equal(process.cwd(),workspace);
  assert.deepEqual(JSON.parse(process.env.ACYCLIC_HOST_ARGS),args);
  await writeFile('root-launch.json',JSON.stringify({task:process.env.ACYCLIC_TASK,args}));
  return;
 }
 assert.deepEqual(args.slice(6,8),['exec','--json']);
 assert.equal(args[args.indexOf('--sandbox')+1],'workspace-write');
 assert.equal(args[args.indexOf('--ask-for-approval')+1],'never');
 assert.equal(args[args.indexOf('--cd')+1],process.cwd());
 const child=relative(join(workspace,'.volumes/agents'),process.cwd());
 assert.ok(child&&child!=='..'&&!child.startsWith('..'+sep)&&!isAbsolute(child));
 assert.equal(await readFile('AGENTS.md','utf8'),'native instruction discovery fixture');
 // Read one byte directly. A blocked-input child must not drain the remainder
 // into a language stream buffer before the original cancellation assertion.
 const first=Buffer.alloc(1);
 assert.equal(readSync(0,first,0,1,null),1,'native prompt closed before its first byte');
 if(first.toString()==='B')await holdForCancel();
 const prompt=first.toString()+readFileSync(0,'utf8');
 assert.ok(['failed','cancelled','recursive','first','resumed'].includes(prompt));
 await writeFile('native-launch.json',JSON.stringify({cwd:process.cwd(),args}));
 const events=(await readFile(process.env.FIXTURE_EVENTS,'utf8')).match(/.*\n/g);
 process.stdout.write(events[0]);
 const base=[...JSON.parse(process.env.FIXTURE_CODEX_ARGS),'sandbox','-c','sandbox_mode="workspace-write"','-c','approval_policy="never"',
  '-c','sandbox_workspace_write.writable_roots='+JSON.stringify([args[args.indexOf('--add-dir')+1]]),'--'];
 if(prompt==='resumed')assert.equal(await readFile('.volumes/home/native-private.txt','utf8'),'first');
 const checked=await scope.run(process.env.FIXTURE_CODEX,[...base,process.execPath,driver,'effects',prompt],options());
 assert.equal(checked.code,0,JSON.stringify({step:'native sandbox effects',
  program:process.env.FIXTURE_CODEX,args:[...base,process.execPath,driver,'effects',prompt],
  cwd:process.cwd(),code:checked.code,signal:checked.signal,stdout:checked.stdout,stderr:checked.stderr}));
 if(prompt==='recursive'){
  const nested=scope.start(process.env.FIXTURE_ACYCLIC,['__mcp','codex'],options());
  try{
   const client=connect(nested.child);
   await tool(client,'spawn','nested','first');
   const finished=await tool(client,'wait','nested');
   assert.equal(finished.reply.kind,'answered');
   assert.ok(finished.reply.outcome.Succeeded!==undefined,JSON.stringify(finished));
   const merged=await scope.run(process.env.FIXTURE_CODEX,[...base,process.env.FIXTURE_ACYCLIC,'fs','merge','agents/nested'],options());
   assert.equal(merged.code,0,merged.stderr);
   assert.equal(await readFile('first.txt','utf8'),'native');
   assert.ok(!existsSync('.acyclic-sdk'),'nested custody was authored into the child project');
  }finally{await closeBridge(nested);}
 }
 if(prompt==='cancelled'){
  const descendant=scope.start(process.execPath,[driver,'descendant'],options());
  const ready=new Spool(descendant.child.stdout,descendant.closed,
   {key:event=>event?.type==='descendant-ready'?['ready']:[],unique:true});
  descendant.input.end();
  await ready.keyed('ready');
  await writeFile('descendant.pid',String(descendant.child.pid));
  await holdForCancel();
 }
 if(prompt==='failed'){process.exitCode=17;return;}
 if(prompt==='resumed')assert.equal(args[args.indexOf('resume')+1],JSON.parse(events[0]).thread_id);
 process.stdout.write(events.slice(1).join(''));
}

async function scenario(){
 const [acyclic,graphcoder,codex,name,...extra]=process.argv.slice(2);
 assert.ok(acyclic&&graphcoder&&codex&&!extra.length&&['lifecycle','recursive','resume'].includes(name),recovery);
 const native=await nativeHost('codex',{E2E_CODEX:resolve(codex)});
 const capture=JSON.parse(await readFile(new URL('./fixtures/native-completion.json',import.meta.url),'utf8'))
  .records.find(record=>record.host==='codex'&&record.mode==='installed'&&record.code===0);
 assert.ok(capture,'restore the original native completion fixture');
 const session=JSON.parse(capture.stdout.split('\n')[0]).thread_id;
 const root=scratchDirectory('managed-native-session-',{E2E_SCRATCH_ROOT:process.env.HOME});
 const workspace=join(root,'workspace'),state=join(root,'state'),bin=join(root,'bin'),fixture=join(root,'native-events.jsonl');
 await Promise.all([mkdir(workspace),mkdir(state),mkdir(bin),writeFile(fixture,capture.stdout)]);
 const platform=Object.fromEntries(Object.entries(process.env)
  .filter(([key])=>/^(SystemRoot|WINDIR|ComSpec|PATHEXT|E2E_PROCESS_HOST)$/i.test(key)));
 const env=environment(root,{...platform,PATH:bin+delimiter+process.env.PATH,
  FIXTURE_NODE:process.execPath,FIXTURE_DRIVER:driver,FIXTURE_CODEX:native.program,
  FIXTURE_CODEX_ARGS:JSON.stringify(native.args),FIXTURE_EVENTS:fixture,
  FIXTURE_ACYCLIC:resolve(acyclic),FIXTURE_WORKSPACE:workspace,FIXTURE_OUTSIDE:join(state,'denied')});
 await writeFile(join(workspace,'AGENTS.md'),'native instruction discovery fixture');
 const launcher=join(bin,process.platform==='win32'?'codex.cmd':'codex');
 await writeFile(launcher,process.platform==='win32'
  ?'@"%FIXTURE_NODE%" "%FIXTURE_DRIVER%" host %*\r\n'
  :'#!/bin/sh\nexec "$FIXTURE_NODE" "$FIXTURE_DRIVER" host "$@"\n');
 if(process.platform!=='win32')await chmod(launcher,0o755);
 const barrier=createServer(),gates=new Set();
 let pendingReady,owner,bridge,client,parent;
 barrier.on('error',error=>pendingReady?.(error));
 barrier.listen(0,'127.0.0.1');await once(barrier,'listening');
 env.FIXTURE_BARRIER=String(barrier.address().port);
 const opts={cwd:workspace,env};
 const announce=detail=>writeSync(2,'NATIVE_FIXTURE_PASS '+detail+'\n');
 const startOwner=async()=>{owner=await startWorkspaceOwner({scope,binary:resolve(graphcoder),workspace,state,root,env});};
 const stopOwner=async()=>{const previous=owner;owner=undefined;if(previous){previous.input.end();await previous.closed;}};
 const startBridge=async()=>{
  const args=JSON.parse(await readFile(join(workspace,'root-launch.json'),'utf8')).args;
  bridge=scope.start(resolve(acyclic),['__mcp','codex'],{...opts,
   env:{...env,ACYCLIC_HOST_ARGS:JSON.stringify(args),ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:parent}});
  client=connect(bridge.child);
 };
 const stopBridge=async()=>{const previous=bridge;bridge=undefined;if(previous)await closeBridge(previous);};
 const call=(toolName,reference,prompt)=>tool(client,toolName,reference,prompt);
 const request=async action=>{
  const result=await scope.run(resolve(acyclic),['__workspace'],opts,JSON.stringify(action));
  assert.equal(result.code,0,result.stdout+result.stderr);return JSON.parse(result.stdout);
 };
 function ready(){
  const announcement=new Promise((yes,no)=>{
   pendingReady=no;
   barrier.once('connection',socket=>{
    gates.add(socket);
    const closed=new Promise((_,reject)=>socket.once('close',()=>reject(fail('native readiness connection closed'))));
    const events=new Spool(socket,closed,{key:event=>event?.cwd?['ready']:[],unique:true});
    events.keyed('ready').then(event=>{pendingReady=undefined;yes(event);},no);
   });
  });
  // A refused spawn can retire the readiness owner before held() reaches its
  // wait. Keep that original promise observable without an unhandled rejection.
  announcement.catch(()=>{});return announcement;
 }
 async function held(reference,prompt){
  const announcement=ready();
  const spawned=await call('spawn',reference,prompt),waiter=call('wait',reference);
  const event=await Promise.race([announcement,waiter.then(value=>{throw fail('child settled before readiness: '+JSON.stringify(value));})]);
  assert.deepEqual(event,{cwd:spawned.cwd});return {spawned,waiter};
 }
 const merge=reference=>request({kind:'fs',task:parent,command:'merge agents/'+reference});
 const authored=async file=>assert.equal(await readFile(join(workspace,file),'utf8'),'native');
 try{
  const base=['--sandbox','workspace-write','--ask-for-approval','never','--add-dir',workspace,'exec','--json','--cd',workspace];
  for(const tail of [['--help'],['resume','fixture-id','--help']]){
   const parsed=await scope.run(native.program,[...native.args,...base,...tail],opts);assert.equal(parsed.code,0,parsed.stdout+parsed.stderr);
  }
  await startOwner();
  const launched=await scope.run(resolve(acyclic),['host','codex','--','--sandbox','workspace-write','--ask-for-approval','never'],opts);
  assert.equal(launched.code,0,launched.stdout+launched.stderr);
  parent=JSON.parse(await readFile(join(workspace,'root-launch.json'),'utf8')).task;
  await startBridge();announce('root-launch');
  if(name==='lifecycle'){
   await call('spawn','failed','failed');const finished=await call('wait','failed');
   assert.equal(finished.reply.kind,'answered');assert.ok(finished.reply.outcome.Failed,JSON.stringify(finished));
   assert.match(finished.reply.outcome.Failed.message,/native host exited[^\n]*\b17\b/,JSON.stringify(finished));
   assert.ok(!existsSync(join(workspace,'failed.txt')),'failed child write leaked before merge');
   await merge('failed');await authored('failed.txt');announce('failed-capture-merge');
   const cancelled=await held('cancelled','cancelled');
   const pid=Number(await readFile(join(cancelled.spawned.cwd,'descendant.pid'),'utf8'));
   const stopped=await call('cancel','cancelled');await cancelled.waiter;
   assert.equal(stopped.control,'interrupt');assert.ok(stopped.status.settled);
   if(process.platform==='linux'){
    const stat='/proc/'+pid+'/stat';assert.ok(!existsSync(stat)||readFileSync(stat,'utf8').split(' ')[2]==='Z','descendant still running after cancel');
   }else assert.throws(()=>process.kill(pid,0),error=>error.code==='ESRCH','descendant still running after cancel');
   assert.ok(!existsSync(join(workspace,'cancelled.txt')),'cancelled child write leaked before merge');
   await merge('cancelled');await authored('cancelled.txt');announce('cancelled-capture-merge');
   const blocked=await held('blocked-input','B'+'x'.repeat(32767));
   const interrupted=await call('cancel','blocked-input');await blocked.waiter;
   assert.ok(interrupted.status.settled);assert.equal(interrupted.control,'interrupt');announce('blocked-input-cancel');
  }
  if(name==='recursive'){
   const recursive=await call('spawn','recursive','recursive'),finished=await call('wait','recursive');
   assert.ok(!existsSync(join(recursive.cwd,'.acyclic-sdk')),'coordination metadata entered project history');
   assert.equal(finished.reply.kind,'answered');assert.ok(finished.reply.outcome.Succeeded!==undefined,JSON.stringify(finished));
   await merge('recursive');await authored('recursive.txt');await authored('first.txt');announce('recursive-native-session-merge');
  }
  if(name==='resume'){
   const original=await call('spawn','retained','first');await call('wait','retained');
   await stopBridge();await stopOwner();await startOwner();await startBridge();
   const resumed=await call('resume','retained','resumed');await call('wait','retained');
   assert.notEqual(resumed.cwd,original.cwd,'retained view was not recreated');
   assert.ok(!existsSync(join(workspace,'resumed.txt')),'resumed child write leaked before merge');
   await merge('retained');await authored('resumed.txt');
   const launch=JSON.parse(await readFile(join(workspace,'native-launch.json'),'utf8'));
   assert.ok(launch.args.includes('resume')&&launch.args.includes(session));
   assert.equal((await request({kind:'hook',host:'codex',event:'NativeSession',input:{task:resumed.task}})).id,session);
   announce('resume-recreated-view-merge');assert.equal((await call('archive','retained')).control,'archive');
  }
  await stopBridge();await stopOwner();
  console.log(JSON.stringify({suite:'managed-native-session-'+name,native_agent_fixture:true,
   real_codex_workspace_write_sandbox:true,model_calls:0,cwd_and_discovery:true,outside_denied:true}));
 }finally{
  pendingReady?.(fail('readiness owner retired'));
  for(const gate of gates)gate.destroy();
  await new Promise((yes,no)=>barrier.close(error=>error?no(error):yes()));
 }
}

try{
 await withOwnedCase({processes:scope,signal:cancellation?.signal,run:async()=>{
  const mode=process.argv[2];
  if(mode==='host')await host();
  else if(mode==='effects')await effects(process.argv[3]);
  // Only process-tree retirement releases this probe. Closing its parent's
  // stdin pipe must leave it alive so incomplete cancellation stays visible.
  else if(mode==='descendant'){
   const held=new Int32Array(new SharedArrayBuffer(4));
   readFileSync(0); // Original parent closes stdin before accepting readiness.
   writeSync(1,'{"type":"descendant-ready"}\n');
   Atomics.wait(held,0,0);
  }
  else await scenario();
 }});
}catch(error){
 writeSync(2,fail(error.stack??error.message).message+'\n');process.exitCode=1;
}finally{cancellation?.dispose();}
