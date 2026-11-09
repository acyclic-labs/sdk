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

function requireNoFuse(){
 assert.equal(process.platform,'linux');
 assert.equal(existsSync('/dev/fuse'),false,'run workspace-nofuse.sh in a container or private mount namespace');
}

async function roots(binary,headless){
 requireNoFuse();
 const graphcoder=process.env.E2E_GRAPHCODER;
 assert.ok(isAbsolute(binary??'')&&isAbsolute(graphcoder??''),'supply the acyclic client and this source tree\'s graphcoder');
 const root=scratchDirectory('workspace-nofuse-'),workspace=join(root,'workspace'),bins=join(root,'bin');
 mkdirSync(workspace);mkdirSync(bins);
 const env=fixtureEnvironment(join(root,'profile'),{PATH:bins+delimiter+process.env.PATH,FIXTURE_HEADLESS:headless?'1':'0'});
 const scope=new Processes();
 let ownerResult;
 try{
  await checked('git',['init','--quiet'],{cwd:workspace,env});
  const executable=await stagePlugin(fileURLToPath(new URL('../../',import.meta.url)),binary,join(root,'package'));
  for(const name of ['claude','opencode']){
   const path=join(bins,name);copyFileSync(new URL('./workspace-nofuse-native.mjs',import.meta.url),path);chmodSync(path,0o755);
  }
  await startWorkspaceOwner({scope,binary:graphcoder,workspace,state:join(root,'state'),root,env,
   capture:result=>{ownerResult=result;}});
  for(const host of ['claude-code','opencode']){
   await checked(executable,['install',host,'--plugin-root',join(root,'package')],{cwd:workspace,env});
   const opened=JSON.parse(await checked(executable,['__workspace'],{cwd:workspace,env},
    JSON.stringify({kind:'open',host,session:'no-FUSE workspace client'})));
   assert.equal(opened.cwd,workspace);assert.ok(opened.task);
   const flags=host==='claude-code'?['--settings','{}','--permission-mode','default']:[];
   const output=await checked(executable,['host',host,...(headless?['--prompt','no-FUSE root prompt']:[]),'--',...flags],{cwd:workspace,env});
   const events=output.trim().split('\n').map(JSON.parse),started=events.at(-1);
   assert.equal(started.type,'native.started');
   if(headless){
    const context=events[0];assert.equal(context.type,'acyclic.host_context');assert.equal(context.version,1);
    assert.equal(context.cwd,workspace);assert.equal(started.task,context.task);
   }
   assert.equal(readFileSync(join(workspace,'native-root.txt'),'utf8'),started.task);
   const status=await checked('git',['status','--porcelain','--untracked-files=all'],{cwd:workspace,env});
   assert.match(status,/native-root\.txt/,'native checkout writes remain authored');
   assert.doesNotMatch(status,/\.acyclic-sdk\/|\.volumes\//,'coordination metadata stays outside native Git inventory');
  }
  // The passive root still retains the exact admitted directory identity.
  const mounts=join(workspace,'.volumes/agents'),held=join(root,'held-agents');
  renameSync(mounts,held);mkdirSync(mounts);
  try{
   const refused=await run(executable,['__workspace'],{cwd:workspace,env},JSON.stringify({kind:'open',host:'opencode',session:'replaced admission'}));
   assert.notEqual(refused.code,0);assert.equal(refused.stdout,'');
   assert.ok(refused.stderr.includes(mounts),refused.stderr);assert.match(refused.stderr,/host task-view admission changed/);
  }finally{rmSync(mounts,{recursive:true});renameSync(held,mounts);}
 }finally{
  try{await scope.close();}finally{rmSync(root,{recursive:true,force:true});}
 }
 assert.ok(ownerResult,'owner completion was not observed');assert.equal(ownerResult.code,0,ownerResult.stderr);
 assert.match(ownerResult.stderr,/host task-view admission changed/);
}

test('workspace open and native root launch capture the checkout without FUSE',()=>roots(process.env.ACYCLIC_TEST_BINARY,false));

// Headless CLI policy belongs to #3233. Qualify that client against the exact
// owner under test; neither entry point gets a separate coordination path.
test('headless native roots share the no-FUSE workspace open',()=>roots(process.env.ACYCLIC_HEADLESS_TEST_BINARY,true));

test('pre-fix owner refuses the same no-FUSE environment',async()=>{
 requireNoFuse();
 const binary=process.env.E2E_NOFUSE_CONTROL_GRAPHCODER,sha=process.env.E2E_NOFUSE_CONTROL_SHA;
 assert.ok(isAbsolute(binary??'')&&/^[a-f0-9]{40}$/.test(sha??''),'supply an explicitly built pre-fix owner and its source SHA');
 const root=scratchDirectory('workspace-nofuse-control-'),workspace=join(root,'workspace');mkdirSync(workspace);
 const env=fixtureEnvironment(join(root,'profile')),scope=new Processes();let result;
 try{
  await checked('git',['init','--quiet'],{cwd:workspace,env});
  await assert.rejects(startWorkspaceOwner({scope,binary,workspace,state:join(root,'state'),root,env,capture:value=>{result=value;}}));
 }finally{
  try{await scope.close();}finally{rmSync(root,{recursive:true,force:true});}
 }
 assert.ok(result);assert.notEqual(result.code,0);
 // This head's read-only preflight reports the missing device before the
 // actual mount can return ENOENT. Older callers expose the raw OS error.
 assert.match(result.stderr,/\/dev\/fuse is not usable|native mount.*os error 2/s);
 console.log('NOFUSE_NEGATIVE_CONTROL',sha,JSON.stringify({code:result.code,stderr:result.stderr}));
});
