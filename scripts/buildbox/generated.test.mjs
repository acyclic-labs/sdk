import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import test from 'node:test';
const here=path.dirname(new URL(import.meta.url).pathname);

test('gate execution never replays a historical PASS verdict',async()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'gate-no-verdict-replay-'));
 const saved={...process.env};
 try{
  const module=path.join(cwd,'generator.mjs');
  fs.writeFileSync(module,fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport { cacheable };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {cacheable}=await import(module);
  delete process.env.BUILD_SLOT_REMOTE_FETCH;delete process.env.BUILD_SLOT_STRESS;
  process.env.BUILD_SLOT_LABEL='gate-execution-control';process.env.BUILD_SLOT_VERDICT_CACHE='1';
  delete process.env.BUILD_SLOT_GATE_LOG;
  assert.equal(cacheable(['cargo','check']),true,'negative control: generic historical verdict replay is otherwise enabled');
  process.env.BUILD_SLOT_GATE_LOG='actual-gate.log';
  assert.equal(cacheable(['cargo','check']),false);
 }finally{for(const key of Object.keys(process.env))if(!(key in saved))delete process.env[key];Object.assign(process.env,saved);fs.rmSync(cwd,{recursive:true,force:true});}
});

test('exact private inputs preserve source times across fresh checkouts',async()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'source-clock-'));
 try{
  const generator=path.join(cwd,'generator.mjs');
  fs.writeFileSync(generator,fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport { SOURCE_TIMES };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {SOURCE_TIMES}=await import(generator);
  const input=path.join(cwd,'lib.rs'),records=path.join(cwd,'records');
  fs.writeFileSync(input,'exact content');fs.writeFileSync(records,'a'.repeat(40)+' 0\tlib.rs\0');
  function normalize(program=SOURCE_TIMES){
   const result=spawnSync(process.execPath,['-e',program,records,cwd,path.join(cwd,'clock.json'),'c'.repeat(64)],{encoding:'utf8'});
   assert.equal(result.status,0,result.stderr);
   return fs.statSync(input).mtimeMs;
  }
  const first=normalize();
  fs.utimesSync(input,new Date(),new Date());
  assert.notEqual(normalize(''),first,'former fresh checkout invalidates a restored Cargo fingerprint');
  assert.equal(normalize(),first);
  const clock=JSON.parse(fs.readFileSync(path.join(cwd,'clock.json')));
  assert.equal(clock.content_sha256,'c'.repeat(64));assert.equal(clock.cacheable,true);
  assert.equal(clock.files.length,1);assert.equal(clock.files[0][0],'lib.rs');
  fs.writeFileSync(input,'changed content');fs.writeFileSync(records,'b'.repeat(40)+' 0\tlib.rs\0');
  assert.notEqual(normalize(),first);
  fs.symlinkSync('lib.rs',path.join(cwd,'alias'));
  fs.utimesSync(input,new Date(),new Date());
  fs.writeFileSync(records,'e'.repeat(40)+' 0\talias\0'+'b'.repeat(40)+' 0\tlib.rs\0');normalize();
  const aliases=JSON.parse(fs.readFileSync(path.join(cwd,'clock.json'))).files;
  assert.equal(aliases[0][3],String(fs.statSync(input,{bigint:true}).mtimeNs));
  assert.equal(aliases[0][4],String(fs.statSync(input,{bigint:true}).ctimeNs));
  fs.symlinkSync('/etc/hostname',path.join(cwd,'external'));
  fs.writeFileSync(records,'d'.repeat(40)+' 0\texternal\0');normalize();
  assert.equal(JSON.parse(fs.readFileSync(path.join(cwd,'clock.json'))).cacheable,false);
  const source=fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8');
  const privateBlock=source.slice(source.indexOf('if [ ${repo} = sdk ] && [ "$BB_HOST_TARGET" = ${TD} ]; then'),source.indexOf('rm -f "$bbI.recs"\nexport BUILD_SLOT_TREE'));
  assert.match(privateBlock,/BB_SOURCE_TIMES_POLICY=blob-past-private-v1/);
  assert.ok(source.indexOf('remote failed: tree-mismatch')<source.indexOf(privateBlock));
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});
test('remote wrappers parse, retain isolation and archive outputs before cleanup',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'job-shell-'));
 try{
  fs.writeFileSync(path.join(cwd,'generator.mjs'),fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport { launchScript, pinGlibc };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const probe=String.raw`import {launchScript,pinGlibc} from './generator.mjs';
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
import crypto from 'node:crypto';
process.env.BUILD_SLOT_REMOTE_FETCH='/work/target/debug/tool';process.env.BUILD_SLOT_GLIBC='2.35';
for(const gate of [false,true])for(const input of [['bash','-c','echo okay'],['cargo','metadata','--format-version','1'],['cargo','build']]){
const argv=pinGlibc(input);
const id={commit:'a'.repeat(40),tree:'b'.repeat(40),content:'c'.repeat(64),forced:[],autocrlf:'false',groups:'e30='};
const script=launchScript('/mnt/bb/wt-sdk/fixture.0','sdk/fixture','.',argv,'/mnt/bb/jobs/fixture','','sdk','',id,60,true,{repo:'sdk',argv,gate_log:gate?'Q:/lanes/pr-gate/fixture.log':null});
const bodies=[script,...[...script.matchAll(/echo ([A-Za-z0-9+/=]+) \| base64 -d > \$J\.(sh|wrap|admitted)\b/g)].map(m=>Buffer.from(m[1],'base64').toString())];
if(bodies.length!==4)throw Error('missing generated shell');
for(const body of bodies){const r=spawnSync('bash',['-n'],{input:body,encoding:'utf8'});if(r.status!==0)throw Error(r.stderr);}
const inner=bodies[1];
const cold=process.env.BUILD_SLOT_REMOTE_COLD==='1';
const disable='export BB_CACHE_DISABLED=1 BB_TARGET_STORE_DISABLED=1';
if(inner.includes(disable)!==cold || (cold && inner.indexOf(disable)>inner.indexOf('exec python3 /home/var/bin/bb-job.py')))throw Error('cold jobs can restore or publish snapshots');
const first=inner.indexOf('\n');
const requestHash=crypto.createHash('sha256').update(inner.slice(first+1).split('/mnt/bb/jobs/fixture').join('$JOBPREFIX')).digest('hex');
if(inner.slice(0,first)!=='export BB_TARGET_REQUEST_SHA256='+requestHash)throw Error('generated Cargo provenance digest lost or incomplete');
if(!inner.includes('PATH=/home/var/bin/bb-cargo:'))throw Error('nested Cargo bypasses target proxy');
if(input[0]==='cargo' && !inner.includes('/home/var/bin/bb-cargo/cargo'))throw Error('explicit Cargo bypasses target proxy');
if(inner.includes('unset RUSTC RUSTC_WRAPPER')===(gate||input[0]==='cargo'))throw Error('gate dropped compiler cache or generic toolchain was overridden');
if(inner.includes('ulimit -n'))throw Error('job overrides caller nofile limit');
if(!bodies[2].includes('admission_policy.py exec-user var --'))throw Error('scope loses caller limits');
if(!bodies[2].includes('--preserve-env=BB_CALLER_RLIMITS_JSON'))throw Error('sudo drops caller limits');
if(bodies[3].indexOf('capture-limits')>bodies[3].indexOf('node $J.receipt-tool.mjs'))throw Error('Node raises nofile before caller capture');
if(!inner.includes('CARGO_TARGET_DIR="$BB_JOB_TARGET"'))throw Error('target escaped job lifetime');
if(!inner.includes('BB_JOB_EXPORT_SPEC='))throw Error('outputs not archived');
if(input[1]==='build' && !inner.includes('"targetMapping":{"triple":"x86_64-unknown-linux-gnu","profiles":["debug","release"]}'))throw Error('glibc-pinned output mapping lost');
if(!bodies[2].includes('rm -rf -- /mnt/bb/jobs/fixture.targets'))throw Error('outer targets not cleaned');
if(script.includes('/mnt/bb/targets/sdk/fixture'))throw Error('persistent target survived');
}
`;
  const file=path.join(cwd,'probe.mjs');fs.writeFileSync(file,probe);
  for(const cold of ['0','1']){
   const result=spawnSync(process.execPath,[file],{encoding:'utf8',env:{...process.env,BUILD_SLOT_REMOTE_COLD:cold}});
   assert.equal(result.status,0,result.stdout+result.stderr);
  }
  const generator=path.join(cwd,'generator.mjs');
  const original=fs.readFileSync(generator,'utf8');
  fs.writeFileSync(generator,original.replace('cargoJob || Boolean(receipt?.gate_log)','cargoJob'));
  const negative=spawnSync(process.execPath,[file],{encoding:'utf8'});
  assert.notEqual(negative.status,0);assert.match(negative.stderr,/gate dropped compiler cache/);
  fs.writeFileSync(generator,original.replaceAll('BB_CACHE_DISABLED=1 BB_TARGET_STORE_DISABLED=1','BB_COLD_DISABLED_CONTROL=1'));
  const coldNegative=spawnSync(process.execPath,[file],{encoding:'utf8',env:{...process.env,BUILD_SLOT_REMOTE_COLD:'1'}});
  assert.notEqual(coldNegative.status,0);assert.match(coldNegative.stderr,/cold jobs can restore or publish snapshots/);
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('executor failure cannot inherit a successful Cargo payload verdict',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'executor-verdict-'));
 try{
  fs.mkdirSync(path.join(cwd,'.codex-fleet'));
  const source=fs.readFileSync(path.join(here,'coordinator/pr-gate.sh'),'utf8');
  const block=source.slice(source.indexOf('# A successful payload'),source.indexOf('# A new strict target'));
  assert.ok(block.includes('executor exited rc='));
  for(const [remote,payload,negative,expected] of [[1,0,false,4],[224,0,false,4],[224,101,false,4],[1,101,false,4],[-9,101,false,4],[101,101,false,0],[0,0,false,0],[1,0,true,0]]){
   const log=path.join(cwd,'gate.log');fs.writeFileSync(log,`GATE_DONE rc=${payload}\n`);
   const script=`rc=${remote};n=1;log=${log}\n${negative?'':block}\nexit 0\n`;
   const result=spawnSync('bash',['-c',script],{encoding:'utf8',env:{...process.env,HOME:cwd}});
   assert.equal(result.status,expected,result.stdout+result.stderr);
   if(expected)assert.match(result.stdout,/INFRA.*executor exited rc=/);
  }
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('a restarted client cannot reuse a detached jobs source copy',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'source-generation-'));
 try{
  const original=fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  for(const negative of [false,true]){
   const source=negative?original.replace('return `${key}-${path.basename(jobPrefix)}`;','return key;'):original;
   fs.writeFileSync(path.join(cwd,'generator.mjs'),source+'\nexport { sourceCopyKey };\n');
   const probe=`import {sourceCopyKey} from './generator.mjs';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
const key='pr-gate-fixture.0';const old=sourceCopyKey(key,'/mnt/bb/jobs/100-1-a');const next=sourceCopyKey(key,'/mnt/bb/jobs/101-1-b');
fs.mkdirSync(old,{recursive:true});fs.writeFileSync(path.join(old,'source'),'first job');
fs.mkdirSync(next,{recursive:true});fs.writeFileSync(path.join(next,'source'),'second job');
assert.equal(fs.readFileSync(path.join(old,'source'),'utf8'),'first job','local claim reuse reset a detached remote checkout');assert.ok(!old.includes('/'));
assert.equal(old,sourceCopyKey(key,'/mnt/bb/jobs/100-1-a'));
`;
   const result=spawnSync(process.execPath,['--input-type=module','-e',probe],{cwd,encoding:'utf8'});
   assert.equal(result.status===0,!negative,result.stderr);
  }
  assert.match(original,/key = sourceCopyKey\(key, J\)/);
  assert.match(original,/src = `\/mnt\/bb\/src\/\$\{key\}`/);
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('a cut-off executor is INFRA before any target-specific or source verdict',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'executor-cutoff-'));
 try{
  fs.mkdirSync(path.join(cwd,'.codex-fleet'));
  const source=fs.readFileSync(path.join(here,'coordinator/pr-gate.sh'),'utf8');
  const block=source.slice(source.indexOf('# An ended client stream'),source.indexOf('# A successful payload'));
  assert.ok(block.includes('executor/stream ended before payload completion'));
  assert.ok(source.indexOf(block)<source.indexOf('windows_reason='));
  for(const [payload,negative,expected] of [[false,false,4],[true,false,0],[false,true,0]]){
   const log=path.join(cwd,'gate.log');fs.writeFileSync(log,'test source_failure ... FAILED\n'+(payload?'GATE_DONE rc=101\n':''));
   const result=spawnSync('bash',['-c',`rc=1;n=1;log=${log}\n${negative?'':block}\nexit 0\n`],{encoding:'utf8',env:{...process.env,HOME:cwd}});
   assert.equal(result.status,expected,result.stdout+result.stderr);
  }
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('gate capture has one log writer and preserves child status and both streams',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'gate-capture-'));
 try{
  const fixture=path.join(cwd,'streams.py');
  fs.writeFileSync(fixture,`import os,sys\nout=os.open('/proc/self/fd/1',os.O_WRONLY)\nerr=os.open('/proc/self/fd/2',os.O_WRONLY)\nos.write(out,b'CHILD\\n'*100)\nos.write(err,b'DIAGNOSTIC\\n'*100)\nsys.exit(17)\n`);
  for(const name of ['pr-gate.sh','main-gate.sh']){
   const source=fs.readFileSync(path.join(here,'coordinator',name),'utf8');
   const start=source.indexOf('(cd ');const tail=source.slice(start);
   const marker=tail.indexOf('capture_status=');
   // Execute the production pipeline and status handling with only the
   // Windows dispatch command substituted by the independent-fd fixture.
   assert.match(tail.split('\n')[0],/2>&1 \| tee/);
   const closing=/\n[ \t]+fi(?:\r?\n|$)/.exec(tail.slice(marker));assert.ok(closing);
   const capture=tail.slice(0,marker+closing.index+closing[0].length);
   const actual=capture.replace(/^.*\) 2>&1 \|/m,`python3 ${fixture} 2>&1 |`);
   assert.ok(actual.includes('capture_status=("${PIPESTATUS[@]}")'));
   const log=path.join(cwd,name+'.log');
   const result=spawnSync('bash',['-c',`log=${log}\n${actual}\nexit "$rc"`],{encoding:'utf8'});
   assert.equal(result.status,17,result.stdout+result.stderr);
   const text=fs.readFileSync(log,'utf8');assert.equal(text,'CHILD\n'.repeat(100)+'DIAGNOSTIC\n'.repeat(100));
  }
  const direct=path.join(cwd,'direct.log');
  const result=spawnSync('bash',['-c',`python3 ${fixture} >${direct} 2>&1`],{encoding:'utf8'});
  assert.equal(result.status,17);assert.notEqual(fs.readFileSync(direct,'utf8'),'CHILD\n'.repeat(100)+'DIAGNOSTIC\n'.repeat(100));
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('main holds incomplete or mismatched executions without source verdicts or retrying',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'main-executor-'));
 try{
  fs.mkdirSync(path.join(cwd,'.codex-fleet'));fs.mkdirSync(path.join(cwd,'infra'));
  const source=fs.readFileSync(path.join(here,'coordinator/main-gate.sh'),'utf8');
  const block=source.slice(source.indexOf('    completed='),source.indexOf('    mv -f "$log" "$L/wide/$tip.log"'));
  assert.ok(block.includes('last=$tip; sleep 300; continue'));
  for(const [rc,text,infra] of [[0,'GATE_DONE rc=0\n',false],[101,'GATE_DONE rc=101\n',false],[1,'test partial ... ok\n',true],[-9,'GATE_DONE rc=0\n',true],[0,'GATE_DONE rc=0\nGATE_DONE rc=0\n',true],[101,'[buildbox] HOME guard INFRA: missing\nGATE_DONE rc=101\n',true],[224,'BB_DISK_LOW preparation-INFRA: insufficient free space\n',true]]){
   const log=path.join(cwd,'running.log');fs.writeFileSync(log,text);
   const result=spawnSync('bash',['-c',`log=${log};L=${cwd};tip=abcdef123;rc=${rc};sleep(){ :; };for attempt in 1;do\n${block}\necho SOURCE_VERDICT;done`],{encoding:'utf8',env:{...process.env,HOME:cwd}});
   assert.equal(result.status,0,result.stderr);assert.equal(result.stdout.includes('SOURCE_VERDICT'),!infra);
   assert.equal(fs.existsSync(log),!infra);
   if(infra)assert.match(fs.readFileSync(path.join(cwd,'.codex-fleet/alerts.log'),'utf8'),/MAIN_WIDE INFRA/);
  }
  const log=path.join(cwd,'negative.log');fs.writeFileSync(log,'partial\n');
  const unguarded=block.replace(/if \[ "\$completed"[^\n]+; then/,'if false; then');assert.notEqual(unguarded,block);
  const negative=spawnSync('bash',['-c',`log=${log};L=${cwd};tip=abcdef123;rc=1;sleep(){ :; };for attempt in 1;do\n${unguarded}\necho SOURCE_VERDICT;done`],{encoding:'utf8',env:{...process.env,HOME:cwd}});
  assert.match(negative.stdout,/SOURCE_VERDICT/); // former path reports a partial run as source evidence
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('outer cleanup failure is INFRA and the former status handling is a negative control',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'outer-cleanup-status-'));
 try{
  const helper=path.join(cwd,'refuse.py');fs.writeFileSync(helper,'import sys\nprint("forced outer cleanup refusal",file=sys.stderr)\nsys.exit(1)\n');
  const source=fs.readFileSync(path.join(here,'buildbox-receipt.mjs'),'utf8').replaceAll('/home/var/bin/bb-job.py',helper);
  for(const [negative,expected] of [[false,224],[true,0]]){
   const module=path.join(cwd,`receipt-${negative}.mjs`);fs.writeFileSync(module,negative?source.replaceAll("rc = 224; category = 'infra';",''):source);
   const meta=path.join(cwd,`meta-${negative}.json`);const receipt=path.join(cwd,`done-${negative}.json`);
   const stat=fs.readFileSync(`/proc/${process.pid}/stat`,'utf8').split(') ').pop().split(' ');
   const identity=fs.readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim()+':'+stat[19];
   fs.writeFileSync(meta,JSON.stringify({owner:{pid:process.pid,identity,pid_namespace:fs.readlinkSync('/proc/self/ns/pid')},side:'bb2',job_prefix:'/mnt/bb/jobs/cleanup-control',receipt_file:receipt,argv:[],worktree:cwd,admission_start:new Date().toISOString()}));
   const result=spawnSync(process.execPath,[module,'finish',meta,'0','command'],{encoding:'utf8'});
   assert.equal(result.status,expected,result.stderr);
   const terminal=JSON.parse(fs.readFileSync(receipt));assert.equal(terminal.exit_status,expected);
   assert.equal(terminal.termination,negative?'command':'infra');assert.match(terminal.outer_cleanup_error,/forced outer cleanup refusal/);
  }
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});
test('isolation and artifact-dispatch INFRA cannot become a source verdict',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'isolation-verdict-'));
 try{
  fs.mkdirSync(path.join(cwd,'.codex-fleet'));
  const source=fs.readFileSync(path.join(here,'coordinator/pr-gate.sh'),'utf8');
  const block=source.slice(source.indexOf('# Executor isolation failures'),source.indexOf('# A successful payload'));
  for(const diagnostic of ['[buildbox] HOME guard INFRA: unsupported','error: GATE_RUNNER_INFRA missing manifest','[buildbox] cleanup INFRA: refused','[buildbox] export INFRA: refused']){
   const log=path.join(cwd,'gate.log');fs.writeFileSync(log,diagnostic+'\nGATE_DONE rc=101\n');
   const result=spawnSync('bash',['-c',`log=${log};n=1\n${block}\nexit 0\n`],{encoding:'utf8',env:{...process.env,HOME:cwd}});
   assert.equal(result.status,4,result.stdout+result.stderr);assert.match(result.stdout,/GATE INFRA/);
  }
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});
