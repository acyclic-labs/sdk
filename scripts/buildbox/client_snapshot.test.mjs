import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import test from 'node:test';
import {consumeClientSnapshot} from './buildbox.mjs';
const here=path.dirname(new URL(import.meta.url).pathname);
const client=path.join(here,'buildbox.mjs');

// One real checkout per case: a commit plus a tracked edit and an untracked file, as a lane worktree has.
function checkout(){
 const root=fs.mkdtempSync(path.join(tmpdir(),'client-snapshot-'));
 const git=(...args)=>{const r=spawnSync('git',args,{cwd:root,encoding:'utf8'});assert.equal(r.status,0,r.stderr);return r.stdout;};
 git('init','-q');git('config','user.email','fixture@example.invalid');git('config','user.name','fixture');
 git('remote','add','origin','https://github.com/acyclic-labs/sdk.git');
 fs.writeFileSync(path.join(root,'tracked.txt'),'committed\n');git('add','.');git('commit','-qm','fixture');
 fs.writeFileSync(path.join(root,'tracked.txt'),'edited\n');fs.writeFileSync(path.join(root,'untracked.txt'),'new\n');
 return root;
}
const verb=(root,...args)=>spawnSync(process.execPath,[client,...args],{cwd:root,encoding:'utf8',env:{...process.env,BUILD_SLOT_VM_HAVE_DIR:root,BUILD_SLOT_CLIENT_CACHE:path.join(root,'.git','client-cache')}});

test('the shared client ships an attested snapshot and refuses a tree changed after identification',async()=>{
 const root=checkout();
 try{
  const file=path.join(root,'.git','snapshot.json');
  for(const [label,change] of [['unchanged',null],['changed',()=>fs.writeFileSync(path.join(root,'untracked.txt'),'changed after identity\n')]]){
   fs.rmSync(file,{force:true});fs.rmSync(file+'.attest',{force:true});
   // bs.ps1's single native process (snapshot, then --attest) for the unchanged tree; separate verbs to change it between.
   const made=change?verb(root,'snapshot','--out',file):verb(root,'snapshot','--out',file,'--attest');assert.equal(made.status,0,made.stderr);
   if(change){change();const attested=verb(root,'snapshot-attest','--snapshot',file);assert.equal(attested.status,0,attested.stderr);}
   if(label==='unchanged'){
    const snapshot=await consumeClientSnapshot(root,file,1000);
    const current=JSON.parse(fs.readFileSync(file+'.attest','utf8'));
    assert.equal(snapshot.id.content,current.content);
    assert.match(snapshot.id.patch.toString(),/untracked\.txt/,'untracked files travel in the snapshot patch');
   }else{
    await assert.rejects(consumeClientSnapshot(root,file,1000),/kind=TREE_IDENTITY step=client-snapshot .*the tree changed after its identity was computed \(.*content/);
   }
  }
  // An attestation that never arrives is a refusal, never an unattested launch.
  fs.rmSync(file+'.attest');
  await assert.rejects(consumeClientSnapshot(root,file,100),/kind=TREE_IDENTITY .*no attestation/);
  // A snapshot that never arrives, or failed natively, is refused; the failure record makes that immediate.
  await assert.rejects(consumeClientSnapshot(root,file+'.absent',100),/kind=TREE_IDENTITY .*no snapshot/);
  fs.writeFileSync(file+'.failed',JSON.stringify({kind:'buildbox-client-snapshot',version:1,error:'git.exe refused'}));
  await assert.rejects(consumeClientSnapshot(root,file+'.failed',60000),/kind=TREE_IDENTITY .*snapshot failed: git\.exe refused/);
  // A snapshot of one checkout cannot serve another.
  await assert.rejects(consumeClientSnapshot('/elsewhere',file,100),/kind=TREE_IDENTITY .*offered for \/elsewhere/);
  // Negative control: the same consumer without the attestation comparison accepts the stale snapshot, whose
  // identity no longer names the tree on disk, and would ship and replay it.
  const mutant=fs.mkdtempSync(path.join(tmpdir(),'client-snapshot-mutant-'));
  try{
   const source=fs.readFileSync(client,'utf8');const guard='a.top !== s.top || a.at < s.at || moved.length';
   assert.equal(source.split(guard).length,2);
   fs.writeFileSync(path.join(mutant,'buildbox.mjs'),source.replace(guard,'false'));
   fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(mutant,'buildbox-receipt.mjs'));
   const made=verb(root,'snapshot','--out',file);assert.equal(made.status,0,made.stderr);
   fs.writeFileSync(path.join(root,'untracked.txt'),'changed again\n');
   assert.equal(verb(root,'snapshot-attest','--snapshot',file).status,0);
   const former=await import(path.join(mutant,'buildbox.mjs'));
   const stale=await former.consumeClientSnapshot(root,file,1000);
   assert.notEqual(stale.id.content,JSON.parse(fs.readFileSync(file+'.attest','utf8')).content);
  }finally{fs.rmSync(mutant,{recursive:true,force:true});}
 }finally{fs.rmSync(root,{recursive:true,force:true});}
});
