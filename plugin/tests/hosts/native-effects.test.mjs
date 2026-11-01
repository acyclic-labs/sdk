import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {copyFile,mkdir,readFile,rm,unlink,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {assertNativeResume} from './native-session-proof.mjs';

test('native filesystem effects reject parent-only execution and lost resume custody',async t=>{
 const root=hostFixtureDirectory('native-effects-'),workspace=join(root,'workspace');
 const processes=new Processes(),env=environment(join(root,'profile'));
 let step='prepare real filesystem oracle control';
 try {
  await mkdir(workspace);
  const script=fileURLToPath(new URL('./native-effects.mjs',import.meta.url));
  const task=randomUUID();
  const run=async(phase,cwd,identity=task,binary)=>{
   const result=await processes.run(process.execPath,[script,phase,workspace,'codex'],
    {cwd,env:{...env,ACYCLIC_TASK:identity,E2E_PLUGIN_BINARY:binary}});
   t.diagnostic(JSON.stringify({phase,cwd,...result}));return result;
  };
  const receipt=await withOwnedCase({processes,run:async()=>{
   step='reject child work attempted by the parent';
   assert.equal((await run('parent',workspace)).code,0);
   step='retain the explicit staged executable across separate workspace scopes';
   for(const binary of [undefined,'relative']) {
    const missing=await run('merge',workspace,task,binary);
    assert.notEqual(missing.code,0);
    assert.match(missing.stderr,/staged plugin executable must be an absolute admitted path/);
   }
   // The real Node executable is outside the workspace. Its original module
   // error proves dispatch used the supplied executable, not a derived path.
   // This is a dispatch negative, not a successful filesystem merge claim.
   const dispatched=await run('merge',workspace,task,process.execPath);
   assert.notEqual(dispatched.code,0);
   assert.match(dispatched.stderr,/Cannot find module/);
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
   step='reject a fresh native session with the same task and retained files';
   // Both identifiers come from unchanged real CLI initialization captures.
   // Admit provenance before projecting the original identifier fields.
   const directory=new URL('./fixtures/native/',import.meta.url);
   const provenance=JSON.parse(await readFile(new URL('provenance.json',directory),'utf8'));
   const native=async host=>{
    const bytes=await readFile(new URL(host+'.jsonl',directory));
    assert.equal(createHash('sha256').update(bytes).digest('hex'),
     provenance.find(row=>row.host===host).fixture_sha256);
    const event=JSON.parse(bytes.toString());return {id:event.thread_id??event.session_id};
   };
   const original=await native('codex'),fresh=await native('claude-code');
   const paths=['native-child.json','child.txt','resumed.txt','.volumes/scratch/native-retained.txt'];
   const before=await Promise.all(paths.map(path=>readFile(join(child,path))));
   assert.throws(()=>assertNativeResume(original,fresh),/resume changed native session identity/);
   assert.throws(()=>assertNativeResume(original,{}),/identifier is missing/);
   assertNativeResume(original,original);
   assert.deepEqual(await Promise.all(paths.map(path=>readFile(join(child,path)))),before);
  }});
  assert.equal(receipt.expired,false);
 } catch(cause) {
  throw new Error('step='+step+': '+cause.message+'; recovery=node --test plugin/tests/hosts/native-effects.test.mjs',{cause});
 } finally {
  await processes.close();await rm(root,{recursive:true});
 }
});
