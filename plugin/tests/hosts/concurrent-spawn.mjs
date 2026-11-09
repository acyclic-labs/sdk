// Real native-host MCP call schedules, without a model or readiness timer.
import assert from 'node:assert/strict';
import {mkdirSync,readFileSync,writeFileSync,chmodSync,rmSync,openSync,writeSync,closeSync} from 'node:fs';
import {join,resolve,relative,delimiter} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer,connect} from 'node:net';
import {once} from 'node:events';
import {finished} from 'node:stream/promises';
import {scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {fixtureEnvironment,workspaceOwner,checked,start,coordination,socketLine} from './native-fixture.mjs';
import {concurrentCorpus,fuzz} from './native-calls.mjs';

const recovery='node plugin/tests/hosts/concurrent-spawn.mjs ACYCLIC GRAPHCODER codex|claude-code [HEX]';
const policies={
 codex:['--sandbox','workspace-write','--ask-for-approval','never'],
 'claude-code':['--settings','{"sandbox":{"enabled":true,"allowUnsandboxedCommands":false}}',
  '--permission-mode','default','--model','claude-haiku-5-5'],
};

export async function native(){
 const env=process.env,host=env.FIXTURE_HOST,cwd=process.cwd();
 if(!process.argv.slice(2).includes({codex:'exec','claude-code':'-p'}[host])){
  writeFileSync('root.json',JSON.stringify({task:env.ACYCLIC_TASK,flags:JSON.parse(env.ACYCLIC_HOST_ARGS)}));return;
 }
 const view=relative(join(env.FIXTURE_WORKSPACE,'.volumes/agents'),cwd);
 assert.ok(view&&!view.startsWith('..'));
 const started=JSON.parse(await checked(env.FIXTURE_ACYCLIC,['__hook',host,'SessionStart'],{cwd,env},
  JSON.stringify({hook_event_name:'SessionStart',session_id:cwd.split('/').at(-1),cwd})));
 assert.ok(started.hookSpecificOutput);
 let prompt='';for await(const chunk of process.stdin)prompt+=chunk;
 if(prompt==='parent'){
  const bridge=start(env.FIXTURE_ACYCLIC,['__mcp',host],{cwd,env});
  try{
   const trace=await fuzz(Buffer.from(env.FIXTURE_CALL_PROGRAM,'hex'),coordination(bridge,recovery));
   writeFileSync(env.FIXTURE_TRACE,JSON.stringify(trace));
   bridge.child.stdin.end();assert.equal((await bridge.closed).code,0,bridge.stderr);
  }finally{await bridge.stop();}
 }else{
  const [side,turn]=prompt.split(':');
  assert.ok(['left','right','lost'].includes(side));
  const file=openSync(join(cwd,side+'.txt'),'w');
  const socket=connect({host:'127.0.0.1',port:Number(env.FIXTURE_RENDEZVOUS)});
  const closed=finished(socket,{cleanup:true});closed.catch(()=>{});
  try{
   writeSync(file,prompt);
   await once(socket,'connect');socket.write(prompt+'\n');
   assert.equal(await socketLine(socket),turn??'retired');
  }finally{
   // Complete both halves of this one-line exchange. Destroying the socket
   // after its answer can reset the peer before its FIN has been consumed.
   socket.end();socket.resume();
   try{await closed;}finally{closeSync(file);}
  }
 }
 process.stdout.write(readFileSync(env.FIXTURE_EVENTS));
}

export async function concurrentSpawn(binaryArg,ownerArg,host,data){
 assert.ok(binaryArg&&ownerArg&&Object.hasOwn(policies,host),`step=admit concurrent host; recovery=${recovery}`);
 const binary=resolve(binaryArg),graphcoder=resolve(ownerArg),started=performance.now();
 const root=scratchDirectory('plugins-concurrent-spawn-'),workspace=join(root,'workspace'),bins=join(root,'bin');
 mkdirSync(workspace);mkdirSync(bins);
 const completion=JSON.parse(readFileSync(new URL('fixtures/native-completion.json',import.meta.url),'utf8')).records.find(row=>row.host===host);
 assert.ok(completion,`step=admit captured completion; recovery=${recovery}`);
 const events=join(root,'native-events.jsonl');writeFileSync(events,completion.stdout);
 const sockets=new Set(),turns=new Map(),lost=Promise.withResolvers(),failed=Promise.withResolvers();
 // Each native turn keeps its original file open until its peer reaches the
 // same turn. A parked wait can precede the second spawn on the original pipe.
 const server=createServer(socket=>{
  sockets.add(socket);socket.once('close',()=>sockets.delete(socket));
  finished(socket,{cleanup:true}).catch(failed.resolve);
  (async()=>{
   const prompt=await socketLine(socket);
   // No more input follows this prompt; retain EOF/error custody after the
   // line iterator returns, including while a peer or producer is parked.
   socket.resume();
   if(prompt==='lost'){lost.resolve(socket);return;}
   const [side,turn]=prompt.split(':');assert.ok(['left','right'].includes(side));
   let pair=turns.get(turn);if(!pair){pair=new Map();turns.set(turn,pair);}
   assert.ok(!pair.has(side),'a native turn was duplicated');pair.set(side,socket);
   if(pair.size===2){for(const peer of pair.values())peer.end(turn+'\n');turns.delete(turn);}
  })().catch(failed.resolve);
 });
 await new Promise((yes,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',yes);});
 const env=fixtureEnvironment(root,{PATH:bins+delimiter+process.env.PATH,
  FIXTURE_HOST:host,FIXTURE_ACYCLIC:binary,FIXTURE_WORKSPACE:workspace,FIXTURE_EVENTS:events,
  FIXTURE_CALL_PROGRAM:Buffer.from(data).toString('hex'),FIXTURE_TRACE:join(root,'concurrent-calls.json'),
  FIXTURE_RENDEZVOUS:String(server.address().port),FIXTURE_CONCURRENT_MODULE:import.meta.url});
 const executable=join(bins,{codex:'codex','claude-code':'claude'}[host]);
 writeFileSync(executable,'#!/usr/bin/env node\nconst {native}=await import(process.env.FIXTURE_CONCURRENT_MODULE);\nawait native();\n');chmodSync(executable,0o755);
 let stopOwner,bridge;
 const observe=promise=>Promise.race([promise,failed.promise.then(error=>{throw error;})]);
 try{
  stopOwner=await workspaceOwner(graphcoder,workspace,join(root,'state'),root,env);
  await checked(binary,['host',host,'--',...policies[host]],{cwd:workspace,env});
  const parent=JSON.parse(readFileSync(join(workspace,'root.json'),'utf8'));
  bridge=start(binary,['__mcp',host],{cwd:workspace,env:{...env,
   ACYCLIC_HOST_ARGS:JSON.stringify(parent.flags),ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:parent.task}});
  const tool=coordination(bridge,recovery);
  await observe(tool('spawn','parent','parent'));
  const finished=await observe(tool('wait','parent'));
  assert.equal(finished.reply.kind,'answered');assert.ok(Object.hasOwn(finished.reply.outcome??{},'Succeeded'),JSON.stringify(finished));
  const trace=JSON.parse(readFileSync(env.FIXTURE_TRACE,'utf8'));
  await observe(tool('spawn','lost','lost'));
  // Observe the actual live child before admitting its wait. It cannot settle
  // until this socket is released, so killing the MCP producer cannot race an
  // answer. The original pipe/process close must reject the outstanding call.
  const waiting=tool('wait','lost').then(value=>({value}),error=>({error}));
  const terminal=waiting.then(result=>{throw new Error(`step=observe held native child: ${JSON.stringify(result.value??result.error?.message)}; recovery=${recovery}`);});
  const socket=await observe(Promise.race([lost.promise,terminal]));
  bridge.child.kill('SIGKILL');
  const result=await waiting;
  assert.match(result.error?.message??'',/native coordination (?:input|output|process) closed/);
  const closed=await bridge.closed;
  trace.lost_producer={exit:closed.code,signal:closed.signal,failure:result.error.message};
  // Release the held child and observe its original settlement through a fresh
  // connection, so the control leaves no live child behind.
  socket.end('retired\n');
  const resumed=start(binary,['__mcp',host],{cwd:workspace,env:{...env,
   ACYCLIC_HOST_ARGS:JSON.stringify(parent.flags),ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:parent.task}});
  try{
   const settled=await observe(coordination(resumed,recovery)('wait','lost'));
   assert.equal(settled.reply.kind,'answered');assert.ok(Object.hasOwn(settled.reply.outcome??{},'Succeeded'));
   resumed.child.stdin.end();assert.equal((await resumed.closed).code,0,resumed.stderr);
  }finally{await resumed.stop();}
  await stopOwner();stopOwner=undefined;
  return {suite:'concurrent-spawn-'+host,model_calls:0,concurrent_spawns_settled:true,trace,seconds:(performance.now()-started)/1000};
 }finally{
  for(const socket of sockets)socket.destroy();
  await new Promise((yes,no)=>server.close(error=>error?no(error):yes()));
  try{if(bridge)await bridge.stop();}finally{if(stopOwner)await stopOwner();rmSync(root,{recursive:true,force:true});}
 }
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{
  const [binary,owner,host,hex,...extra]=process.argv.slice(2);
  assert.ok(!extra.length&&(hex===undefined||/^(?:[0-9a-fA-F]{2})*$/.test(hex)),`step=decode concurrent call program; recovery=${recovery}`);
  for(const data of hex===undefined?concurrentCorpus:[Buffer.from(hex,'hex')])console.log(JSON.stringify(await concurrentSpawn(binary,owner,host,data)));
 }catch(error){console.error(`step=qualify concurrent native calls: ${error.stack}; recovery=${recovery}`);process.exitCode=1;}
}
