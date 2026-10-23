import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import test from 'node:test';
const here=path.dirname(new URL(import.meta.url).pathname);
test('remote wrappers parse, retain isolation and archive outputs before cleanup',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'job-shell-'));
 try{
  fs.writeFileSync(path.join(cwd,'generator.mjs'),fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport { launchScript };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const probe=String.raw`import {launchScript} from './generator.mjs';
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
for(const argv of [['bash','-c','echo okay'],['cargo','metadata','--format-version','1']]){
const id={commit:'a'.repeat(40),tree:'b'.repeat(40),content:'c'.repeat(64),forced:[],autocrlf:'false',groups:'e30='};
const script=launchScript('/mnt/bb/wt-sdk/fixture.0','sdk/fixture','.',argv,'/mnt/bb/jobs/fixture','','sdk','',id,60,true,{repo:'sdk',argv});
const bodies=[script,...[...script.matchAll(/echo ([A-Za-z0-9+/=]+) \| base64 -d > \$J\.(sh|wrap|admitted)\b/g)].map(m=>Buffer.from(m[1],'base64').toString())];
if(bodies.length!==4)throw Error('missing generated shell');
for(const body of bodies){const r=spawnSync('bash',['-n'],{input:body,encoding:'utf8'});if(r.status!==0)throw Error(r.stderr);}
const inner=bodies[1];
if(!inner.includes('CARGO_TARGET_DIR="$BB_JOB_TARGET"'))throw Error('target escaped job lifetime');
if(!inner.includes('BB_JOB_EXPORT_SPEC='))throw Error('outputs not archived');
if(!bodies[2].includes('rm -rf -- /mnt/bb/jobs/fixture.targets'))throw Error('outer targets not cleaned');
if(script.includes('/mnt/bb/targets/sdk/fixture'))throw Error('persistent target survived');
}
`;
  const file=path.join(cwd,'probe.mjs');fs.writeFileSync(file,probe);
  const result=spawnSync(process.execPath,[file],{encoding:'utf8'});
  assert.equal(result.status,0,result.stdout+result.stderr);
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('executor failure cannot inherit a successful Cargo payload verdict',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'executor-verdict-'));
 try{
  fs.mkdirSync(path.join(cwd,'.codex-fleet'));
  const source=fs.readFileSync(path.join(here,'coordinator/pr-gate.sh'),'utf8');
  const block=source.slice(source.indexOf('# A successful payload'),source.indexOf('# A new strict target'));
  assert.ok(block.includes('executor exited rc='));
  for(const [remote,payload,negative,expected] of [[1,0,false,4],[224,0,false,4],[101,101,false,0],[0,0,false,0],[1,0,true,0]]){
   const log=path.join(cwd,'gate.log');fs.writeFileSync(log,`GATE_DONE rc=${payload}\n`);
   const script=`rc=${remote};n=1;log=${log}\n${negative?'':block}\nexit 0\n`;
   const result=spawnSync('bash',['-c',script],{encoding:'utf8',env:{...process.env,HOME:cwd}});
   assert.equal(result.status,expected,result.stdout+result.stderr);
   if(expected)assert.match(result.stdout,/INFRA.*executor exited rc=/);
  }
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});
