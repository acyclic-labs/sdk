import assert from 'node:assert/strict';
import {copyFileSync,mkdirSync,mkdtempSync,readFileSync,writeFileSync,chmodSync,rmSync} from 'node:fs';
import {join,resolve,delimiter} from 'node:path';
import {fixtureEnvironment,workspaceOwner,checked,start,coordination,nativeEventFixture,barrier,ready,socketLine} from './native-fixture.mjs';

const [binaryArg,ownerArg,host,...extra]=process.argv.slice(2);
const recovery='node plugin/tests/hosts/shared-mount-scale.mjs ACYCLIC GRAPHCODER codex|claude-code|pi';
try{
 assert.ok(binaryArg&&ownerArg&&!extra.length&&['codex','claude-code','pi'].includes(host),`step=admit shared scale; recovery=${recovery}`);
 const binary=resolve(binaryArg),graphcoder=resolve(ownerArg),started=performance.now();
 const root=mkdtempSync(join(process.env.HOME,'.plugins-shared-scale-')),workspace=join(root,'workspace'),bins=join(root,'bin');
 mkdirSync(workspace);mkdirSync(bins);
 const flags={codex:['--sandbox','workspace-write','--ask-for-approval','never'],
  'claude-code':['--settings','{"sandbox":{"enabled":true,"allowUnsandboxedCommands":false}}','--permission-mode','default','--model','claude-haiku-5-5'],pi:['--tools','read,write,bash']}[host];
 const gate=await barrier(),gates=[],paths=[],waits=[],samples=[];
 const env=fixtureEnvironment(root,{PATH:bins+delimiter+process.env.PATH,FIXTURE_EVENTS:nativeEventFixture(host).path,
  FIXTURE_FLAGS:JSON.stringify(flags),FIXTURE_HOST:host,FIXTURE_WORKSPACE:workspace,FIXTURE_BARRIER:String(gate.port),
  FIXTURE_SCALE:'1',FIXTURE_SUPPORT:new URL('./native-fixture.mjs',import.meta.url).href,
  ACYCLIC_CACHED_DESCRIPTORS:'1',ACYCLIC_ADMITTED_DESCRIPTORS:'1'});
 writeFileSync(join(workspace,'AGENTS.md'),'shared scale discovery fixture');
 const executable=join(bins,{codex:'codex','claude-code':'claude',pi:'pi'}[host]);
 copyFileSync(new URL('./native-session-child.mjs',import.meta.url),executable);chmodSync(executable,0o755);
 let stopOwner,bridge;
 try{
  stopOwner=await workspaceOwner(graphcoder,workspace,join(root,'state'),root,env);
  await checked(binary,['host',host,'--',...flags],{cwd:workspace,env});
  const parent=JSON.parse(readFileSync(join(workspace,'root.json'),'utf8'));
  const client=async action=>JSON.parse(await checked(binary,['__workspace'],{cwd:workspace,env},JSON.stringify(action)));
  bridge=start(binary,['__mcp',host],{cwd:workspace,env:{...env,ACYCLIC_HOST_ARGS:JSON.stringify(parent.flags),ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:parent.task}});
  const tool=coordination(bridge,recovery);
  async function spawnHeld(reference,prompt){
   const abort=new AbortController(),connection=gate.next(abort.signal);connection.catch(()=>{});
   try{
    const location=await tool('spawn',reference,prompt),waiter=tool('wait',reference);waiter.catch(()=>{});
    const socket=await ready(connection,waiter,abort,recovery);gates.push(socket);
    assert.equal(await socketLine(socket),reference);return {location,waiter};
   }catch(error){abort.abort();throw error;}
  }
  await assert.rejects(spawnHeld('early','exit-before-connect'),/child settled before readiness:.*exited-before-connect/);
  const live=async()=>{
   for(const socket of gates){
    const reply=(async()=>{
     for await(const bytes of socket.iterator({destroyOnReturn:false}))return bytes;
     throw new Error(`step=sample live native session: readiness producer closed; recovery=${recovery}`);
    })();
    socket.write('?');assert.deepEqual(await reply,Buffer.from('!'));
   }
  };
  for(const population of [2,4]){
   for(let index=paths.length;index<population;index++){
    const reference='child'+index,{location,waiter}=await spawnHeld(reference,reference);
    paths.push(location.cwd);waits.push(waiter);
    assert.equal(readFileSync(join(location.cwd,reference+'.txt'),'utf8'),reference);
    assert.equal(readFileSync(join(location.cwd,'.volumes/home/private.txt'),'utf8'),reference);
   }
   await live();const sample=await client({kind:'resources',task:parent.task});await live();
   assert.ok(sample.views>population);assert.ok(sample.mounts>0);
   sample.live_sessions=population;samples.push(sample);console.log(JSON.stringify({host,sample,sampled_process:'shared_mount_owner'}));
  }
  assert.equal(new Set(paths.map(path=>resolve(path,'..'))).size,1);
  const [small,large]=samples,added=large.live_sessions-small.live_sessions;
  assert.ok(small.live_sessions>0);assert.equal(added,small.live_sessions);
  for(const [name,limit] of Object.entries({descriptors:1,threads:0,mounts:0}))assert.ok(large[name]-small[name]<=limit*added,JSON.stringify(samples));
  for(const socket of gates)socket.write('.');
  for(let index=0;index<paths.length;index++){
   const observed=await waits[index],reference='child'+index;
   assert.ok(observed.status.settled);assert.equal(observed.reply.kind,'answered');
   const failure=observed.reply.outcome.Failed.message;
   assert.ok(failure.endsWith('diagnostic-tail-'+host));assert.ok(Buffer.byteLength(failure)<4300);
   await client({kind:'fs',task:parent.task,command:'merge agents/'+reference});
   assert.equal(readFileSync(join(workspace,reference+'.txt'),'utf8'),reference);
  }
  bridge.child.stdin.end();assert.equal((await bridge.closed).code,0);
  await stopOwner();stopOwner=undefined;
  console.log(JSON.stringify({host,samples,seconds:(performance.now()-started)/1000,model_calls:0,native_session_standins:true,
   sessions_alive_at_both_samples:true,sampled_process:'shared_mount_owner',shared_project_and_private_views:true}));
 }finally{
  for(const socket of gates)socket.destroy();await gate.close();
  try{if(bridge)await bridge.stop();}finally{if(stopOwner)await stopOwner();rmSync(root,{recursive:true,force:true});}
 }
}catch(error){console.error(`step=shared native scale: ${error.stack}; recovery command: ${recovery}`);process.exitCode=1;}
