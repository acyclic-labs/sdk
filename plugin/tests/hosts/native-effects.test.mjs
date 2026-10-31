import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {copyFile,mkdir,readFile,rm,unlink,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';

test('native filesystem effects reject parent-only execution and lost resume custody',async t=>{
 const root=hostFixtureDirectory('native-effects-'),workspace=join(root,'workspace');
 const processes=new Processes(),env=environment(join(root,'profile'));
 let step='prepare real filesystem oracle control';
 try {
  await mkdir(workspace);
  const script=fileURLToPath(new URL('./native-effects.mjs',import.meta.url));
  const task=randomUUID();
  const run=async(phase,cwd,identity=task)=>{
   const result=await processes.run(process.execPath,[script,phase,workspace],
    {cwd,env:{...env,ACYCLIC_TASK:identity}});
   t.diagnostic(JSON.stringify({phase,cwd,...result}));return result;
  };
  const receipt=await withOwnedCase({processes,run:async()=>{
   step='reject child work attempted by the parent';
   assert.equal((await run('parent',workspace)).code,0);
   const parentOnly=await run('child',workspace);
   assert.notEqual(parentOnly.code,0);
   assert.match(parentOnly.stderr,/separate view/);
   step='exercise the original effect with an isolated real directory';
   const child=join(workspace,'.volumes/agents/child');
   await mkdir(join(child,'.volumes/scratch'),{recursive:true});
   await copyFile(join(workspace,'parent.txt'),join(child,'parent.txt'));
   const written=await run('child',child);
   assert.equal(written.code,0,written.stderr);
   assert.equal(await readFile(join(child,'child.txt'),'utf8'),'CHILD');
   step='reject resumed work after private state is lost';
   const retained=join(child,'.volumes/scratch/native-retained.txt');
   const bytes=await readFile(retained);await unlink(retained);
   const lost=await run('resumed',child);
   assert.notEqual(lost.code,0);assert.match(lost.stderr,/ENOENT/);
   step='reject a different original child identity';
   await writeFile(retained,bytes);
   const foreign=await run('resumed',child,randomUUID());
   assert.notEqual(foreign.code,0);assert.match(foreign.stderr,/resume changed child identity/);
   step='accept preserved identity and private bytes';
   const resumed=await run('resumed',child);
   assert.equal(resumed.code,0,resumed.stderr);
   assert.equal(await readFile(join(child,'resumed.txt'),'utf8'),'RESUMED');
  }});
  assert.equal(receipt.expired,false);
 } catch(cause) {
  throw new Error('step='+step+': '+cause.message+'; recovery=node --test plugin/tests/hosts/native-effects.test.mjs',{cause});
 } finally {
  await processes.close();await rm(root,{recursive:true});
 }
});
