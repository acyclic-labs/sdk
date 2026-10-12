import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';

const workflow=readFileSync('.github/workflows/static.yml','utf8');
const command=workflow.match(/^        run: (cargo .+)$/m)?.[1];
assert.equal(command,'cargo update --workspace --locked');
const args=command.split(' ').slice(1);
const env={...process.env,RUSTUP_TOOLCHAIN:'stable'};
const run=(cwd,arguments_)=>spawnSync('cargo',arguments_,{cwd,env,encoding:'utf8'});

for(const mutation of ['unchanged','missing lock','changed version','changed dependency']){
 test(`required CI lock check: ${mutation}`,()=>{
  const root=mkdtempSync(join(tmpdir(),'sdk-lock-contract-'));
  try{
   mkdirSync(join(root,'src'));
   writeFileSync(join(root,'src/lib.rs'),'');
   const manifest='[package]\nname="lock-contract"\nversion="0.1.0"\nedition="2021"\n';
   writeFileSync(join(root,'Cargo.toml'),manifest);
   const generated=run(root,['generate-lockfile','--offline']);
   assert.equal(generated.status,0,generated.stderr);
   const before=readFileSync(join(root,'Cargo.lock'),'utf8');
   if(mutation==='missing lock')rmSync(join(root,'Cargo.lock'));
   if(mutation==='changed version')writeFileSync(join(root,'Cargo.toml'),manifest.replace('0.1.0','0.2.0'));
   if(mutation==='changed dependency'){
    mkdirSync(join(root,'dep/src'),{recursive:true});
    writeFileSync(join(root,'dep/src/lib.rs'),'');
    writeFileSync(join(root,'dep/Cargo.toml'),'[package]\nname="dep"\nversion="0.1.0"\nedition="2021"\n');
    writeFileSync(join(root,'Cargo.toml'),manifest+'[dependencies]\ndep={path="dep"}\n');
   }
   const result=run(root,[...args,'--offline']);
   if(mutation==='unchanged'){
    assert.equal(result.status,0,result.stderr);
    assert.equal(readFileSync(join(root,'Cargo.lock'),'utf8'),before);
   }else{
    assert.notEqual(result.status,0,result.stderr);
    assert.match(result.stderr,/lock file|lockfile/);
   }
  }finally{rmSync(root,{recursive:true,force:true});}
 });
}
