import assert from 'node:assert/strict';
import {spawn,spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import test from 'node:test';
import {gitPrepInvocation,maintenanceResult} from './buildbox.mjs';
const here=path.dirname(new URL(import.meta.url).pathname);

test('source preparation streams arbitrary launch and binary payload bytes through one bounded command',()=>{
 const root=fs.mkdtempSync(path.join(tmpdir(),'streamed-launch-'));
 const quote=value=>"'"+value.replaceAll("'","'\\''")+"'";
 try{
  const prep=path.join(root,'prepare.mjs');
  fs.writeFileSync(prep,`import fs from 'node:fs';
const [root,...args]=process.argv.slice(2);
fs.appendFileSync(root+'/preparations','prepared\\n');
fs.writeFileSync(root+'/payload',fs.readFileSync(0));
fs.writeFileSync(root+'/arguments',JSON.stringify(args));
if(process.env.PREP_FAIL)process.exit(Number(process.env.PREP_FAIL));
console.log('prepared');`);
  const payload=Buffer.alloc(256*1024);
  for(let i=0;i<payload.length;i++)payload[i]=i%256;
  for(const repo of ['sdk','cloud']){
   const directory=path.join(root,repo+' fixture');fs.mkdirSync(directory);
   const job=path.join(directory,'job'),worktree=path.join(directory,'source');
   const effect=path.join(directory,'effect');
   const launch='#'+ 'x'.repeat(1024*1024)+'\n'+
    `printf '%s' 'exact action' >> ${quote(effect)}\ntouch ${quote(job+'.wpid')}\necho launched\n`;
   // sdk maintenance rides the same connection: a fixture maintainer that warns and fails proves its status and
   // stderr come back framed on every exit path, launched or refused.
   const maintainer=path.join(directory,'maintain.sh');
   fs.writeFileSync(maintainer,"#!/bin/bash\necho >&2 'BB_WT_PRUNE_FAILED fixture'\nexit 3\n",{mode:0o755});
   const invocation=gitPrepInvocation(repo,worktree,'a'.repeat(40),job,launch,payload,quote(maintainer));
   const maintained=(stdout)=>{
    const result=maintenanceResult(stdout);
    if(repo!=='sdk'){assert.equal(result,null);return;}
    assert.equal(result.status,3);assert.equal(result.stderr.toString(),'BB_WT_PRUNE_FAILED fixture\n');
   };
   assert.equal(invocation.command,'bash -s');
   assert.equal(invocation.input.toString().split('/usr/local/bin/bb-git-prep').length,2);
   const input=invocation.input.toString()
    .replaceAll('/mnt/bb/locks/wt-sdk',path.join(root,'locks'))
    .replace('/usr/local/bin/bb-git-prep',`${quote(process.execPath)} ${quote(prep)} ${quote(directory)}`);
   const run=extra=>spawnSync('bash',['-s'],{input,env:{...process.env,...extra},encoding:'utf8'});
   const current=run({});
   assert.equal(current.status,0,current.stderr);
   assert.match(current.stdout,repo==='sdk'?/prepared\nlaunched\nBB_MAINTAIN 3 \S+\n$/:/prepared\nlaunched\n$/);maintained(current.stdout);
   assert.deepEqual(fs.readFileSync(path.join(directory,'payload')),payload);
   assert.deepEqual(JSON.parse(fs.readFileSync(path.join(directory,'arguments'))),
    [worktree,'a'.repeat(40),...(repo==='sdk'?['sdk']:[])]);
   assert.equal(fs.readFileSync(effect,'utf8'),'exact action');
   assert.equal(run({}).status,0);
   assert.equal(fs.readFileSync(path.join(directory,'preparations'),'utf8'),'prepared\n');
   assert.equal(fs.readFileSync(effect,'utf8'),'exact action','acknowledged ownership cannot launch twice');
   fs.unlinkSync(job+'.wpid');fs.unlinkSync(effect);
   const refused=run({PREP_FAIL:'73'});
   assert.equal(refused.status,73,refused.stderr);maintained(refused.stdout);
   assert.equal(fs.existsSync(effect),false,'failed preparation must block launch');
   // Restore the former production transport: its full generated launch is
   // one SSH exec argument. The actual kernel refuses it before any effect.
   const former=`echo ${Buffer.from(launch).toString('base64')} | base64 -d | bash`;
   const negative=spawnSync('bash',['-c',former],{input:payload});
   assert.equal(negative.error?.code,'E2BIG');
   assert.equal(fs.existsSync(effect),false);
  }
 }finally{fs.rmSync(root,{recursive:true,force:true});}
});


test('new admission never traverses unrelated receipt records',()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'receipt-admission-'));
 try{
  const directory=path.join(here,'fixtures/startup-readiness');
  const former=fs.readFileSync(path.join(directory,'former-begin-receipt.mjs.txt'),'utf8');
  const origin=JSON.parse(fs.readFileSync(path.join(directory,'former-begin-receipt.origin.json')));
  assert.equal(spawnSync('sha256sum',[path.join(directory,'former-begin-receipt.mjs.txt')],{encoding:'utf8'}).stdout.split(' ')[0],origin.fixture_sha256);
  const source=fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8');
  for(const negative of [false,true]){
   const root=path.join(cwd,negative?'former':'current');fs.mkdirSync(root);
   const file=path.join(root,'buildbox.mjs');
   let selected=source.replace("const LOGDIR = '/home/var/log/buildbox';",'const LOGDIR = '+JSON.stringify(root)+';');
   if(negative){
    const first=selected.indexOf('export function beginReceipt(argv) {');
    const last=selected.indexOf('export function endReceipt(',first);
    selected=selected.slice(0,first)+former+selected.slice(last);
    selected=selected.replace('import { arm, atomic, finish }','import { arm, atomic, finish, recover }');
   }
   fs.writeFileSync(file,selected);
   fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(root,'buildbox-receipt.mjs'));
   const result=spawnSync(process.execPath,['--input-type=module','-e',`
    import fs from 'node:fs';
    const original=fs.readdirSync;
    fs.readdirSync=function(dir,...args){
      if(dir===${JSON.stringify(root)}) throw new Error('unrelated receipt storage unavailable');
      return original.call(this,dir,...args);
    };
    const m=await import(${JSON.stringify(file)});
    const ctx=m.beginReceipt(['true']);
    m.endReceipt(ctx,0);
    console.log('OWN_RECEIPT_COMPLETED');
   `],{cwd:root,encoding:'utf8'});
   if(negative){assert.notEqual(result.status,0);assert.match(result.stderr,/unrelated receipt storage unavailable/);}
   else {assert.equal(result.status,0,result.stderr);assert.match(result.stdout,/OWN_RECEIPT_COMPLETED/);}
  }
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});
test('launch acknowledgement follows observable wrapper readiness, including slow recovery and early death',async()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'wrapper-readiness-'));
 try{
  fs.writeFileSync(path.join(cwd,'generator.mjs'),fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport {launchScript};\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {launchScript}=await import(path.join(cwd,'generator.mjs'));
  const id={commit:'a'.repeat(40),tree:'b'.repeat(40),content:'c'.repeat(64),forced:[],autocrlf:'false',groups:'e30='};
  const script=launchScript('/mnt/bb/wt-sdk/fixture.0','sdk/fixture','.', ['true'],'/mnt/bb/jobs/fixture','','sdk','',id,60,true,{});
  const encoded=script.match(/echo ([A-Za-z0-9+/=]+) \| base64 -d > \$J\.wrap\b/);
  const wrapper=Buffer.from(encoded[1],'base64').toString();
  assert.doesNotMatch(wrapper,/receipt-tool\.mjs recover /u,'remote admission only arms its own receipt');
  const start=wrapper.indexOf('echo $$ > $J.wpid');
  const close=wrapper.indexOf('exec 3>&-',start);
  assert.ok(start>=0&&close>start);
  const acknowledgement=wrapper.slice(start,close+'exec 3>&-'.length);
  assert.doesNotMatch(acknowledgement,/bb-admit|receipt-meta/u,'fixture executes only the generated acknowledgement');
  assert.ok(start>wrapper.indexOf('node $J.receipt-tool.mjs arm'),'receipt custody precedes acknowledgement');
  const current=script.slice(script.lastIndexOf('ready=$(setsid'));
  const directory=path.join(here,'fixtures/startup-readiness');
  const origin=JSON.parse(fs.readFileSync(path.join(directory,'origin.json')));
  const former=fs.readFileSync(path.join(directory,'former-launch.sh.txt'),'utf8');
  const {createHash}=await import('node:crypto');
  assert.equal(createHash('sha256').update(former).digest('hex'),origin.sha256);
  function waitPath(file){
   return new Promise((resolve,reject)=>{
    const watcher=fs.watch(cwd,()=>{if(fs.existsSync(file)){watcher.close();resolve();}});
    watcher.on('error',reject);
    if(fs.existsSync(file)){watcher.close();resolve();}
   });
  }
  function launch(prefix,body){
   const child=spawn('bash',['-c',`set -eu\nJ='${prefix}'\n${body}`],{stdio:['ignore','pipe','pipe']});
   let stdout='',stderr='';child.stdout.on('data',b=>stdout+=b);child.stderr.on('data',b=>stderr+=b);
   const completed=new Promise((resolve,reject)=>child.on('error',reject).on('close',status=>resolve({status,stdout,stderr})));
   return {child,completed};
  }
  for(const negative of [true,false]){
   const prefix=path.join(cwd,negative?'former':'current');
   assert.equal(spawnSync('mkfifo',[prefix+'.release']).status,0);
   // An OS FIFO is a deterministic barrier for the wrapper's pre-readiness
   // recovery, not fabricated provider output or a raised production budget.
   fs.writeFileSync(prefix+'.wrap',`J='${prefix}'\ntouch "$J.blocked"\nread -r release < "$J.release"\n${negative?'echo $$ > $J.wpid':acknowledgement}\ntouch "$J.ran"\n`);
   const blocked=waitPath(prefix+'.blocked'),ran=waitPath(prefix+'.ran');
   const job=launch(prefix,negative?former:current);
   await blocked;
   if(negative){
    const result=await job.completed;
    assert.equal(result.status,0,result.stderr);
    assert.equal(result.stdout.trim(),'launched');
    assert.equal(fs.existsSync(prefix+'.wpid'),false,'actual former launch reports readiness before the wrapper is observable');
    assert.equal(fs.existsSync(prefix+'.ran'),false);
   }else{
    assert.equal(job.child.exitCode,null,'acknowledgement remains blocked by actual wrapper readiness');
   }
   fs.writeFileSync(prefix+'.release','release\n');
   await ran;
   if(!negative){
    const result=await job.completed;
    assert.equal(result.status,0,result.stderr);
    assert.equal(result.stdout.trim(),'launched');
    assert.ok(fs.existsSync(prefix+'.wpid'));
   }
  }
  const prefix=path.join(cwd,'early-death');
  fs.writeFileSync(prefix+'.wrap','exit 73\n');
  const refused=await launch(prefix,current).completed;
  assert.equal(refused.status,224,refused.stdout+refused.stderr);
  assert.equal(refused.stdout,'');
  assert.match(refused.stderr,/kind=LAUNCH_INFRA step=wrapper-readiness owner=gate-speed recovery=build-slot --remote/u);
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});
// Test callers name their tenant explicitly, exactly as production callers
// must at cut-over; the launcher has no repository/name inference fallback.
process.env.BUILD_SLOT_TENANT='graphcoder';

test('both services require an explicit tenant and generated phases inherit one kernel lease',async()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'cpu-lease-dispatch-'));
 const saved=process.env.BUILD_SLOT_TENANT;
 try{
  const file=path.join(cwd,'generator.mjs');
  const source=fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8');
  fs.writeFileSync(file,source+'\nexport {tenantOf};\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {tenantOf}=await import(file);
  for(const tenant of ['graphcoder','cloud']){
   process.env.BUILD_SLOT_TENANT=tenant;
   assert.equal(tenantOf('unrelated-label','/other/worktree','sdk',['printf','a b']),tenant);
  }
  delete process.env.BUILD_SLOT_TENANT;
  assert.throws(()=>tenantOf('gate-speed','/sdk','sdk',['printf','a b']),/CPU_LEASE.*tenant-label.*recovery=.*printf/u);
  // Actual former inference/default shape admits an unlabelled SDK caller.
  assert.equal((()=>process.env.BUILD_SLOT_TENANT||'graphcoder')(),'graphcoder');
  assert.ok(source.includes('cpu_leases.py work'));
  assert.doesNotMatch(source,/systemd-run --scope -q --slice=bbjobs\.slice/u);
 }finally{
  process.env.BUILD_SLOT_TENANT=saved;fs.rmSync(cwd,{recursive:true,force:true});
 }
});

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
  fs.writeFileSync(generator,fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport { SOURCE_TIMES, IDENT_ADD, IDENT_SKIP };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {SOURCE_TIMES,IDENT_ADD,IDENT_SKIP}=await import(generator);
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
  assert.deepEqual(clock.namespace,{version:1,pathspec:IDENT_ADD,skip:IDENT_SKIP},'publication must use the actual upload inventory rules');
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
test('watched source directories retain content-derived times across fresh checkouts',async()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'source-directory-clock-'));
 try{
  const generator=path.join(cwd,'generator.mjs');
  const source=fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8');
  fs.writeFileSync(generator,source+'\nexport { SOURCE_TIMES };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {SOURCE_TIMES}=await import(generator);
  const root=path.join(cwd,'src'),proto=path.join(root,'proto'),records=path.join(cwd,'records');
  fs.mkdirSync(proto,{recursive:true});fs.writeFileSync(path.join(proto,'schema.proto'),'schema');
  fs.writeFileSync(records,'a'.repeat(40)+' 0\tproto/schema.proto\0');
  function normalize(program=SOURCE_TIMES){
   const result=spawnSync(process.execPath,['-e',program,records,root,path.join(cwd,'clock.json'),'c'.repeat(64)],{encoding:'utf8'});
   assert.equal(result.status,0,result.stderr);return fs.statSync(proto).mtimeMs;
  }
  const first=normalize();
  fs.rmSync(proto,{recursive:true});fs.mkdirSync(proto);fs.writeFileSync(path.join(proto,'schema.proto'),'schema');
  const fresh=fs.statSync(proto).mtimeMs;assert.notEqual(fresh,first);
  const start=SOURCE_TIMES.indexOf('for(const [name,hash] of directories){');
  const end=SOURCE_TIMES.indexOf('// Normalize every target',start);
  assert.ok(start>=0&&end>start);
  const filesOnly=SOURCE_TIMES.slice(0,start)+SOURCE_TIMES.slice(end);
  assert.equal(normalize(filesOnly),fresh,'old files-only normalization leaves watched directories fresh');
  assert.equal(normalize(),first);
  fs.writeFileSync(records,'b'.repeat(40)+' 0\tproto/schema.proto\0');
  assert.notEqual(normalize(),first,'changed directory inputs require a different content time');
  const prepare=source.indexOf("${cargoJob ? '' : NPM_ENSURE}");
  const clock=source.indexOf('# Shared target slot');
  const verify=source.indexOf('# Tree identity (2026-10-16 treeid)');
  assert.ok(prepare>=0&&verify>prepare&&clock>verify,'dependency preparation must precede identity verification and source normalization');
 }finally{fs.rmSync(cwd,{recursive:true,force:true});}
});

test('dependency preparation cannot change source under the original uploaded identity',async()=>{
 const cwd=fs.mkdtempSync(path.join(tmpdir(),'preparation-identity-'));
 try{
  const module=path.join(cwd,'generator.mjs');
  fs.writeFileSync(module,fs.readFileSync(path.join(here,'buildbox.mjs'),'utf8')+'\nexport { launchScript, NPM_ENSURE };\n');
  fs.copyFileSync(path.join(here,'buildbox-receipt.mjs'),path.join(cwd,'buildbox-receipt.mjs'));
  const {launchScript,NPM_ENSURE}=await import(module);
  const root=path.join(cwd,'input');fs.mkdirSync(root);
  function git(...args){const r=spawnSync('git',args,{cwd:root,encoding:'utf8'});assert.equal(r.status,0,r.stderr);return r.stdout;}
  git('init','-q');fs.writeFileSync(path.join(root,'source.rs'),'uploaded');git('add','source.rs');
  const records=git('ls-files','-s','-z').replace(/^[0-7]* /,'');
  const crypto=await import('node:crypto');
  const id={commit:'none',tree:'none',content:crypto.createHash('sha256').update(records).digest('hex'),forced:[],autocrlf:'false',groups:'e30='};
  const wrapper=launchScript('/mnt/bb/wt-sdk/fixture.0','sdk/fixture','.', ['bash','fixture.sh'],'/mnt/bb/jobs/fixture','','sdk','',id,60,true,{});
  const encoded=wrapper.match(/echo ([A-Za-z0-9+/=]+) \| base64 -d > \$J\.sh\b/);assert.ok(encoded);
  const inner=Buffer.from(encoded[1],'base64').toString();
  const start=inner.indexOf(NPM_ENSURE),verify=inner.indexOf('# Tree identity (2026-10-16 treeid)'),end=inner.indexOf('# Shared target slot');
  assert.ok(start>=0&&verify>start&&end>verify);
  const segment=inner.slice(start,end),prepare="printf changed > source.rs\n";
  for(const negative of [false,true]){
   fs.writeFileSync(path.join(root,'source.rs'),'uploaded');
   let body=negative?segment.replace(NPM_ENSURE,'')+NPM_ENSURE:segment;
   body=body.replace(NPM_ENSURE,prepare).replaceAll('/work/src',root).replaceAll('/tmp/bbid.',cwd+'/bbid.');
   const result=spawnSync('bash',['-c',body],{cwd:root,encoding:'utf8'});
   assert.equal(result.status,negative?0:224,result.stdout+result.stderr);
   assert.equal(fs.readFileSync(path.join(root,'source.rs'),'utf8'),'changed');
   if(!negative)assert.match(result.stderr,/tree-mismatch/);
  }
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
function captureBeforeNode(body){
const capture=body.indexOf('capture-limits'),node=body.indexOf('node $J.receipt-tool.mjs');
if(capture<0||node<0||capture>node)throw Error('Node raises nofile before caller capture');
}
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
if(/(?:unset RUSTC|export RUSTC=|export RUSTC_WRAPPER=)/u.test(inner))throw Error('transport replaced the canonical admitted compiler policy');
if(inner.includes('ulimit -n'))throw Error('job overrides caller nofile limit');
if(!bodies[2].includes('admission_policy.py exec-user var --'))throw Error('scope loses caller limits');
if(!bodies[2].includes('--preserve-env=BB_CALLER_RLIMITS_JSON'))throw Error('sudo drops caller limits');
captureBeforeNode(bodies[3]);
for(const broken of [bodies[3].replaceAll('capture-limits','missing-capture'),bodies[3].replaceAll('node $J.receipt-tool.mjs','missing-node')]){
let rejected=false;try{captureBeforeNode(broken);}catch{rejected=true;}if(!rejected)throw Error('missing capture/Node negative control passed');
}
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
  fs.writeFileSync(generator,original.replace('${testRunner()}','${testRunner()}unset RUSTC RUSTC_WRAPPER'));
  const negative=spawnSync(process.execPath,[file],{encoding:'utf8'});
  assert.notEqual(negative.status,0);assert.match(negative.stderr,/transport replaced the canonical admitted compiler policy/);
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
  const block=source.slice(source.indexOf('# A successful payload'),source.indexOf('# Validate an owned preflight'));
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
  assert.ok(source.indexOf(block)<source.indexOf('phase_reason='));
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
