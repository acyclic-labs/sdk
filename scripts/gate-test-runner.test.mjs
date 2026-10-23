import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {chmodSync,mkdtempSync,mkdirSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
const source=readFileSync(new URL('./gate-test-runner.sh',import.meta.url),'utf8');
test('basename collisions retain libtest args; only linked fuzz drivers get bounded smoke args',()=>{
 const cwd=mkdtempSync(join(tmpdir(),'gate-fuzz-owner-'));
 try{
  mkdirSync(join(cwd,'bin'));
  const nm=join(cwd,'bin/nm');
  writeFileSync(nm,`#!/bin/bash
[ "$NM_FAIL" != 1 ] || exit 1
case "$*" in *-fuzz) echo '000001 t LLVMFuzzerTestOneInput';; *) echo '000002 T main';; esac
`);chmodSync(nm,0o755);
  const bins=join(cwd,'bins');writeFileSync(bins,'filesystem_access\n');
  for(const suffix of ['ordinary','fuzz']){
   const binary=join(cwd,'filesystem_access-'+suffix);
   writeFileSync(binary,`#!/bin/bash\nprintf '%s\\n' "$@"\n`);chmodSync(binary,0o755);
  }
  for(const [kind,broken,negative,status] of [['ordinary',false,false,0],['fuzz',false,false,0],['ordinary',true,false,2],['ordinary',false,true,0]]){
   const runner=join(cwd,'runner');
   const old=source.replace(/        if ! symbols=[\s\S]*?        fi\n        break;;/, '        echo "GATE_FUZZ_SMOKE bin=$bin runs=1 seed=1"\n        set -- -runs=1 -seed=1 -max_total_time=10\n        break;;');
   assert.notEqual(old,source);
   writeFileSync(runner,negative?old:source);
   const r=spawnSync('bash',[runner,join(cwd,'filesystem_access-'+kind),'--skip','known-flake'],{encoding:'utf8',env:{...process.env,PATH:join(cwd,'bin')+':'+process.env.PATH,GATE_FUZZ_TEST_BIN_FILE:bins,NM_FAIL:broken?'1':'0'}});
   assert.equal(r.status,status,r.stdout+r.stderr);
   if(broken){assert.match(r.stderr,/GATE_RUNNER_INFRA/);continue;}
   if(kind==='ordinary'&&!negative){assert.equal(r.stdout,'--skip\nknown-flake\n');assert.doesNotMatch(r.stdout,/GATE_FUZZ/);}
   else{assert.match(r.stdout,/-runs=1\n-seed=1\n-max_total_time=10/);assert.doesNotMatch(r.stdout,/known-flake/);}
  }
 }finally{rmSync(cwd,{recursive:true,force:true});}
});
test('live main bootstrap uses the reviewed runner exactly',()=>{
 const wrapper=readFileSync(new URL('./buildbox/coordinator/gate-checks.sh',import.meta.url),'utf8');
 const marker="cat > .gate-test-runner.sh <<'GATE_FUZZ_BOOTSTRAP'\n";
 assert.equal(wrapper.split(marker)[1].split('GATE_FUZZ_BOOTSTRAP\n')[0],source);
 const pr=readFileSync(new URL('./buildbox/coordinator/pr-gate.sh',import.meta.url),'utf8');
 assert.match(pr,/cp Q:\/lanes\/gate-test-runner.sh "\$W\/\.gate-test-runner.sh"/);
 assert.match(pr,/GATE_TEST_RUNNER_SCRIPT=\.gate-test-runner.sh/);
});

test('main installs corrected helper before dispatching an older reviewed runner',()=>{
 const cwd=mkdtempSync(join(tmpdir(),'main-runner-bootstrap-'));
 try{
  mkdirSync(join(cwd,'scripts'));
  writeFileSync(join(cwd,'scripts/check-browser-targets.sh'),'');
  writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),'');
  writeFileSync(join(cwd,'scripts/gate-test-runner.sh'),'old name-only runner');
  writeFileSync(join(cwd,'scripts/main-gate-checks.sh'),'# Main baseline completion v2\ntest "$GATE_TEST_RUNNER_SCRIPT" = .gate-test-runner.sh || exit 71\ncmp .gate-test-runner.sh expected || exit 72\necho corrected-helper-selected\n');
  writeFileSync(join(cwd,'expected'),source);
  const complete=readFileSync(new URL('./buildbox/coordinator/gate-checks.sh',import.meta.url),'utf8');
  // Exercise main dispatch only; auxiliary Cargo phases belong to other tests.
  const boundary=complete.lastIndexOf('gate_main_existing "$@" | sed');
  assert.ok(boundary>0);
  const wrapper=complete.slice(0,boundary)+'gate_main_existing "$@"\n';
  const a=wrapper.indexOf("cat > .gate-test-runner.sh <<'GATE_FUZZ_BOOTSTRAP'\n");
  const b=wrapper.indexOf('export GATE_TEST_RUNNER_SCRIPT=.gate-test-runner.sh\n',a)+'export GATE_TEST_RUNNER_SCRIPT=.gate-test-runner.sh\n'.length;
  const block=wrapper.slice(a,b);
  for(const negative of [false,true]){
   let script=wrapper;
   if(negative){script=script.slice(0,a)+script.slice(b);const at=script.indexOf("cat > .gate-feature-matrix.mjs");script=script.slice(0,at)+block+script.slice(at);}
   const path=join(cwd,'wrapper');writeFileSync(path,script);
   const result=spawnSync('bash',[path],{cwd,encoding:'utf8',env:{...process.env,GATE_TEST_RUNNER_SCRIPT:''}});
   assert.equal(result.status,negative?71:0,result.stdout+result.stderr);
   if(!negative)assert.match(result.stdout,/corrected-helper-selected/);
  }
 }finally{rmSync(cwd,{recursive:true,force:true});}
});
