import assert from 'node:assert/strict';
import {chmodSync,copyFileSync,existsSync,mkdirSync,readFileSync,renameSync,rmSync} from 'node:fs';
import {delimiter,isAbsolute,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {fixtureEnvironment,checked,run} from './native-fixture.mjs';
import {stagePlugin} from './package.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';

test('installed headless roots share workspace open and capture without FUSE',async()=>{
 assert.equal(process.platform,'linux');
 assert.equal(existsSync('/dev/fuse'),false,'run inside the no-FUSE mount namespace');
 const binary=process.env.ACYCLIC_TEST_BINARY,graphcoder=process.env.E2E_GRAPHCODER;
 assert.ok(isAbsolute(binary??'')&&isAbsolute(graphcoder??''),'supply the matching acyclic and graphcoder binaries');
 const root=scratchDirectory('headless-nofuse-'),workspace=join(root,'workspace'),bins=join(root,'bin');
 mkdirSync(workspace);mkdirSync(bins);
 const env=fixtureEnvironment(join(root,'profile'),{PATH:bins+delimiter+process.env.PATH});
 const scope=new Processes();
 let ownerResult,refused;
 try{
  await checked('git',['init','--quiet'],{cwd:workspace,env});
  const executable=await stagePlugin(fileURLToPath(new URL('../../',import.meta.url)),binary,join(root,'package'));
  for(const name of ['claude','opencode']){
   const path=join(bins,name);copyFileSync(new URL('./headless-native.mjs',import.meta.url),path);chmodSync(path,0o755);
  }
  await startWorkspaceOwner({scope,binary:graphcoder,workspace,state:join(root,'state'),root,env,
   capture:result=>{ownerResult=result;}});
  for(const host of ['claude-code','opencode']){
   await checked(executable,['install',host,'--plugin-root',join(root,'package')],{cwd:workspace,env});
   const opened=JSON.parse(await checked(executable,['__workspace'],{cwd:workspace,env},
    JSON.stringify({kind:'open',host,session:'no-FUSE workspace client'})));
   assert.equal(opened.cwd,workspace);
   const flags=host==='claude-code'?['--settings','{}','--permission-mode','default']:[];
   const output=await checked(executable,['host',host,'--prompt','no-FUSE root prompt','--',...flags],{cwd:workspace,env});
   const [context,started]=output.trim().split('\n').map(JSON.parse);
   assert.equal(context.type,'acyclic.host_context');assert.equal(context.version,1);
   assert.equal(context.cwd,workspace);assert.equal(started.type,'native.started');assert.equal(started.task,context.task);
   assert.equal(readFileSync(join(workspace,'native-root.txt'),'utf8'),context.task);
  }
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
  try{await scope.close();}finally{rmSync(root,{recursive:true,force:true});}
 }
 assert.ok(ownerResult,'owner completion was not observed');
 assert.equal(ownerResult.code,0,ownerResult.stderr);
 assert.ok(ownerResult.stderr.includes(join(workspace,'.volumes/agents')),ownerResult.stderr);
 assert.match(ownerResult.stderr,/host task-view admission changed/);
});
