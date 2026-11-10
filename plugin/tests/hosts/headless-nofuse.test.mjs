import assert from 'node:assert/strict';
import {chmodSync,copyFileSync,existsSync,mkdirSync,readFileSync,renameSync,rmSync} from 'node:fs';
import {delimiter,isAbsolute,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {fixtureEnvironment,checked,run,start,barrier,ready,socketLine} from './native-fixture.mjs';
import {stagePlugin} from './package.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
import {probeOwner} from './workspace-probe.mjs';

test('installed headless roots share workspace open and capture without FUSE',async()=>{
 assert.equal(process.platform,'linux');
 if(!process.env.E2E_NATIVE_PROBE_OWNER)
  assert.equal(existsSync('/dev/fuse'),false,'supply the probe owner or a host without /dev/fuse');
 const binary=process.env.ACYCLIC_TEST_BINARY,graphcoder=process.env.E2E_GRAPHCODER;
 assert.ok(isAbsolute(binary??'')&&isAbsolute(graphcoder??''),'supply the matching acyclic and graphcoder binaries');
 const root=scratchDirectory('headless-nofuse-'),workspace=join(root,'workspace'),bins=join(root,'bin');
 mkdirSync(workspace);mkdirSync(bins);
 const env=fixtureEnvironment(join(root,'profile'),{PATH:bins+delimiter+process.env.PATH});
 const scope=new Processes();
 let ownerResult,refused,injectedOwner;
 try{
  await checked('git',['init','--quiet'],{cwd:workspace,env});
  const executable=await stagePlugin(fileURLToPath(new URL('../../',import.meta.url)),binary,join(root,'package'));
  for(const name of ['claude','opencode']){
   const path=join(bins,name);copyFileSync(new URL('./headless-native.mjs',import.meta.url),path);chmodSync(path,0o755);
  }
  if(process.env.E2E_NATIVE_PROBE_OWNER)
   injectedOwner=await probeOwner(process.env.E2E_NATIVE_PROBE_OWNER,workspace,join(root,'state'),root,env);
  else await startWorkspaceOwner({scope,binary:graphcoder,workspace,state:join(root,'state'),root,env,
   capture:result=>{ownerResult=result;}});
  for(const host of ['claude-code','opencode']){
   await checked(executable,['install',host,'--plugin-root',join(root,'package')],{cwd:workspace,env});
   const opened=JSON.parse(await checked(executable,['__workspace'],{cwd:workspace,env},
    JSON.stringify({kind:'open',host,session:'no-FUSE workspace client'})));
   assert.equal(opened.cwd,workspace);
   const flags=host==='claude-code'?['--settings','{}','--permission-mode','default']:[];
   const output=await checked(executable,['host',host,'--prompt','no-FUSE root prompt','--',...flags],{cwd:workspace,env});
   const [context,omitted,started]=output.trim().split('\n').map(JSON.parse);
   assert.equal(omitted.type,'acyclic.event_omitted','an oversized native event must report loss and continue');
   assert.equal(context.type,'acyclic.host_context');assert.equal(context.version,1);
   assert.equal(context.cwd,workspace);assert.equal(started.type,'native.started');assert.equal(started.task,context.task);
   assert.equal(readFileSync(join(workspace,'native-root.txt'),'utf8'),context.task);
   const status=await checked('git',['status','--porcelain','--untracked-files=all'],{cwd:workspace,env});
   assert.match(status,/native-root\.txt/,'the native checkout write must remain authored');
   assert.doesNotMatch(status,/\.acyclic-sdk\/|\.volumes\//,'coordination metadata must stay outside native Git inventory');
  }
  // Close the caller's output while a real descendant holds the native pipes.
  // Its socket is the readiness event; no sleep, namespace or PID scan is needed.
  const gate=await barrier(),abort=new AbortController();let failed,socket,descendant;
  try{
   const connection=gate.next(abort.signal);
   failed=start(executable,['host','opencode','--prompt','no-FUSE root prompt','--'],
    {cwd:workspace,env:{...env,FIXTURE_BARRIER:String(gate.port)}});
   socket=await ready(connection,failed.closed,abort,'headless containment regression');
   descendant=Number(await socketLine(socket));assert.ok(descendant>0);
   const disconnected=new Promise(resolve=>socket.once('close',resolve));socket.resume();
   failed.child.stdout.destroy();
   socket.write('write\n');
   const result=await failed.closed;
   assert.notEqual(result.code,0);assert.match(result.stderr,/Broken pipe|broken pipe/);
   await disconnected;
   assert.throws(()=>process.kill(descendant,0),{code:'ESRCH'},'the native descendant must be joined before the host returns');
   assert.equal(readFileSync(join(workspace,'before-output-failure.txt'),'utf8'),'captured');
   // The quiescent checkout remains usable through the shared capture boundary.
   const task=readFileSync(join(workspace,'native-root.txt'),'utf8');
   const diff=JSON.parse(await checked(executable,['__workspace'],{cwd:workspace,env},JSON.stringify({kind:'fs',task,command:'diff'})));
   assert.match(JSON.stringify(diff),/before-output-failure\.txt/);
  }finally{abort.abort();if(failed)await failed.stop();socket?.destroy();await gate.close();}
  // A failed shared admission must diagnose the original path on BOTH ends.
  const mounts=join(workspace,'.volumes/agents'),held=join(root,'held-agents');
  renameSync(mounts,held);mkdirSync(mounts);
  try{
   refused=await run(executable,['host','opencode','--prompt','no-FUSE root prompt','--'],{cwd:workspace,env});
   assert.notEqual(refused.code,0);assert.equal(refused.stdout,'');
   assert.ok(refused.stderr.includes(mounts),refused.stderr);
   assert.match(refused.stderr,/host task-view admission changed/);
  }finally{rmSync(mounts,{recursive:true});renameSync(held,mounts);}
 }finally{
  try{if(injectedOwner)ownerResult=await injectedOwner.close();await scope.close();}finally{rmSync(root,{recursive:true,force:true});}
 }
 assert.ok(ownerResult,'owner completion was not observed');
 assert.equal(ownerResult.code,0,ownerResult.stderr);
 assert.ok(ownerResult.stderr.includes(join(workspace,'.volumes/agents')),ownerResult.stderr);
 assert.match(ownerResult.stderr,/host task-view admission changed/);
});
