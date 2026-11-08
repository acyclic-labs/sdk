import assert from 'node:assert/strict';
import {copyFileSync,mkdirSync,mkdtempSync,readFileSync,writeFileSync,chmodSync,existsSync,rmSync,readdirSync} from 'node:fs';
import {join,resolve,delimiter} from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {fixtureEnvironment,workspaceOwner,checked,start,coordination,barrier,ready,socketLine} from './native-fixture.mjs';

export async function managedSession(argv,clock=()=>performance.now()) {
 const [binaryArg,ownerArg,codexArg,scenario,...extra]=argv;
 const recovery='node plugin/tests/hosts/managed-session.mjs ACYCLIC GRAPHCODER CODEX lifecycle|recursive|resume';
 assert.ok(binaryArg&&ownerArg&&codexArg&&!extra.length&&['lifecycle','recursive','resume'].includes(scenario),`step=admit managed session; recovery=${recovery}`);
 const [binary,graphcoder,codex]=[binaryArg,ownerArg,codexArg].map(value=>resolve(value));
 const started=clock(),root=mkdtempSync(join(process.env.HOME,'.plugins-session-proof-'));
 const workspace=join(root,'workspace'),state=join(root,'state'),bins=join(root,'bin');
 for(const path of [workspace,state,bins])mkdirSync(path);
 const gate=await barrier(),gates=[];
 const completion=JSON.parse(readFileSync(new URL('fixtures/native-completion.json',import.meta.url),'utf8')).records.find(row=>row.host==='codex');
 const events=join(root,'native-events.jsonl');writeFileSync(events,completion.stdout);
 const env=fixtureEnvironment(root,{PATH:bins+delimiter+process.env.PATH,FIXTURE_CODEX:codex,FIXTURE_EVENTS:events,
  FIXTURE_ACYCLIC:binary,FIXTURE_WORKSPACE:workspace,FIXTURE_OUTSIDE:join(state,'denied'),
  FIXTURE_SUPPORT:new URL('./native-fixture.mjs',import.meta.url).href,FIXTURE_BARRIER:String(gate.port)});
 writeFileSync(join(workspace,'AGENTS.md'),'native instruction discovery fixture');
 const fake=join(bins,'codex');copyFileSync(new URL('./native-session-child.mjs',import.meta.url),fake);chmodSync(fake,0o755);
 let stopOwner,bridge,tool;
 const phase=name=>console.log('NATIVE_FIXTURE_PASS',name,(clock()-started)/1000);
 const ownerStart=async()=>{stopOwner=await workspaceOwner(graphcoder,workspace,state,root,env);phase('workspace-owner-ready');};
 const ownerStop=async()=>{const previous=stopOwner;stopOwner=undefined;if(previous){await previous();phase('workspace-owner-retired');}};
 const closeBridge=async()=>{const previous=bridge;bridge=undefined;tool=undefined;if(previous){previous.child.stdin.end();assert.equal((await previous.closed).code,0,previous.stderr);phase('bridge-retired');}};
 const startBridge=task=>{
  const inherited=JSON.parse(readFileSync(join(workspace,'root-launch.json'),'utf8')).args;
  bridge=start(binary,['__mcp','codex'],{cwd:workspace,env:{...env,ACYCLIC_HOST_ARGS:JSON.stringify(inherited),ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:task}});
  tool=coordination(bridge,recovery);
 };
 const client=async action=>JSON.parse(await checked(binary,['__workspace'],{cwd:workspace,env},JSON.stringify(action)));
 const succeeded=async reference=>{
  const result=await tool('wait',reference);
  console.log('NATIVE_FIXTURE_TURN',JSON.stringify({reference,terminal:result,seconds:(clock()-started)/1000}));
  assert.equal(result.reply.kind,'answered');assert.notEqual(result.reply.outcome.Succeeded,undefined);return result;
 };
 // Arm the original connection before dispatching the producer. Settlement
 // always wins with an error if the native process never reaches readiness.
 const spawnHeld=async(reference,prompt)=>{
  const abort=new AbortController(),connection=gate.next(abort.signal);connection.catch(()=>{});
  try{
   const spawned=await tool('spawn',reference,prompt),waiter=tool('wait',reference);waiter.catch(()=>{});
   const socket=await ready(connection,waiter,abort,recovery);gates.push(socket);
   assert.deepEqual(JSON.parse(await socketLine(socket)),{cwd:spawned.cwd});
   return {view:spawned.cwd,waiter};
  }catch(error){abort.abort();throw error;}
 };
 try{
  const flags=['--sandbox','workspace-write','--ask-for-approval','never','--add-dir',workspace,'exec','--json','--cd',workspace];
  for(const tail of [['--help'],['resume','fixture-id','--help']])await checked(codex,[...flags,...tail],{cwd:workspace,env});
  await ownerStart();await checked(binary,['host','codex','--','--sandbox','workspace-write','--ask-for-approval','never'],{cwd:workspace,env});
  const parent=JSON.parse(readFileSync(join(workspace,'root-launch.json'),'utf8')).task;startBridge(parent);phase('root-launch');
  if(scenario==='lifecycle'){
   await tool('spawn','failed','failed');const failed=await tool('wait','failed');
   assert.equal(failed.reply.kind,'answered');assert.ok(failed.reply.outcome.Failed);
   assert.ok(!existsSync(join(workspace,'failed.txt')));
   await client({kind:'fs',task:parent,command:'merge agents/failed'});assert.equal(readFileSync(join(workspace,'failed.txt'),'utf8'),'native');phase('failed-capture-merge');
   const cancelled=await spawnHeld('cancelled','cancelled');phase('cancelled-native-ready');
   const pid=readFileSync(join(cancelled.view,'descendant.pid'),'utf8');
   const stopped=await tool('cancel','cancelled');await cancelled.waiter;
   assert.equal(stopped.control,'interrupt');assert.ok(stopped.status.settled);
   let stat;try{stat=readFileSync(`/proc/${pid}/stat`,'utf8');}catch(error){if(error.code!=='ENOENT')throw error;}
   assert.ok(stat===undefined||stat.split(' ')[2]==='Z','descendant still running after cancel');
   assert.ok(!existsSync(join(workspace,'cancelled.txt')));
   await client({kind:'fs',task:parent,command:'merge agents/cancelled'});assert.equal(readFileSync(join(workspace,'cancelled.txt'),'utf8'),'native');phase('cancelled-capture-merge');
   const blocked=await spawnHeld('blocked-input','B'+'x'.repeat(32767));
   const interrupted=await tool('cancel','blocked-input');await blocked.waiter;
   assert.ok(interrupted.status.settled);assert.equal(interrupted.control,'interrupt');phase('blocked-input-cancel');
  }
  if(scenario==='recursive'){
   await tool('spawn','recursive','recursive');await succeeded('recursive');
   await client({kind:'fs',task:parent,command:'merge agents/recursive'});assert.equal(readFileSync(join(workspace,'recursive.txt'),'utf8'),'native');phase('recursive-native-session-merge');
  }
  if(scenario==='resume'){
   await tool('spawn','retained','first');const original=await succeeded('retained');
   await closeBridge();await ownerStop();await ownerStart();startBridge(parent);
   await tool('resume','retained','resumed');const resumed=await succeeded('retained');
   assert.notEqual(resumed.cwd,original.cwd);assert.ok(!existsSync(join(workspace,'resumed.txt')));
   await client({kind:'fs',task:parent,command:'merge agents/retained'});assert.equal(readFileSync(join(workspace,'resumed.txt'),'utf8'),'native');phase('resume-recreated-view-merge');
   assert.equal((await tool('archive','retained')).control,'archive');phase('retained-archived');
  }
  await closeBridge();await ownerStop();
  console.log(JSON.stringify({suite:'managed-native-session-'+scenario,native_agent_fixture:true,real_codex_workspace_write_sandbox:true,model_calls:0,cwd_and_discovery:true,outside_denied:true,seconds:(clock()-started)/1000}));
 }catch(error){
  const agents=join(workspace,'.volumes/agents');
  if(existsSync(agents))for(const view of readdirSync(agents)){
   const file=join(agents,view,'.volumes/home/native-error.txt');if(existsSync(file))console.error('FIXTURE_NATIVE_ERROR',view,readFileSync(file,'utf8'));
  }
  throw error;
 }finally{
  try{for(const socket of gates)socket.destroy();await gate.close();}
  finally{try{await closeBridge();}finally{await ownerStop();rmSync(root,{recursive:true,force:true});}}
 }
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
 try{await managedSession(process.argv.slice(2));}
 catch(error){console.error(`step=managed native session: ${error.stack}; recovery command: node ${fileURLToPath(import.meta.url)} ACYCLIC GRAPHCODER CODEX lifecycle|recursive|resume`);process.exitCode=1;}
}
