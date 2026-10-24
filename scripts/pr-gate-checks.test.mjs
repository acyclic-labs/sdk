import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const scripts=dirname(fileURLToPath(import.meta.url));
test('affected PR checks every target and docs, including test=false fuzz binaries', {skip:process.platform==='win32'},()=>{
  const cwd=mkdtempSync(join(tmpdir(),'pr-gate-targets-'));
  try{
    mkdirSync(join(cwd,'bin'));mkdirSync(join(cwd,'scripts'));
    writeFileSync(join(cwd,'scripts/gate-test-runner.sh'),readFileSync(join(scripts,'gate-test-runner.sh')));
    writeFileSync(join(cwd,'scripts/gate-windows-features.sh'),'#!/bin/bash\necho "windows-features $*" >> calls\nexit "${WINDOWS_FEATURE_RC:-0}"\n');
    const cargo=join(cwd,'bin/cargo');
    writeFileSync(cargo,`#!/bin/bash
echo "$*" >> calls
echo "$1|$CARGO_TARGET_DIR" >> targets
case "$1" in
metadata) [ "$BROKEN_METADATA" != 1 ] || { echo 'error: metadata control' >&2; exit 101; }; echo '{"workspace_members":["fuzz"],"packages":[{"id":"fuzz","name":"fuzz","metadata":{"cargo-fuzz":true},"targets":[{"name":"recorded_invocation","kind":["bin"]}]}]}' ;;
clean|clippy) exit 0 ;;
test)
 if [[ " $* " == *" --all-targets "* ]] && [ "$BROKEN_FUZZ" = 1 ]; then
  echo 'error[E0425]: cannot find type Capabilities in module acyclic_harness' >&2
  echo 'error: could not compile acyclic-harness-fuzz (bin recorded_invocation test)' >&2
  exit 101
 fi
 if [[ " $* " != *" --no-run "* ]]; then echo 'test result: ok. 1 passed; 0 failed'; fi;;
*) exit 9;;
esac
`);chmodSync(cargo,0o755);
    writeFileSync(join(cwd,'.gate-scope-mode'),'affected\n');
    writeFileSync(join(cwd,'.gate-tests-packages'),'acyclic-harness-fuzz\nreverse-consumer\n');
    writeFileSync(join(cwd,'.gate-clippy-packages'),'acyclic-harness-fuzz\nreverse-consumer\n');
    const source=readFileSync(join(scripts,'pr-gate-checks.sh'),'utf8');
    for(const [broken,negative,expected,windows=0] of [[true,false,101],[true,true,0],[false,false,0],[false,false,101,101]]){
      const runner=join(cwd,'runner.sh');writeFileSync(runner,negative?source.replace('run_tests targets --all-targets','run_tests targets'):source);
      writeFileSync(join(cwd,'calls'),'');
      writeFileSync(join(cwd,'targets'),'');
      const result=spawnSync('bash',[runner,'--skip','known-flake'],{cwd,encoding:'utf8',env:{...process.env,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),BROKEN_FUZZ:broken?'1':'0',WINDOWS_FEATURE_RC:String(windows)}});
      assert.equal(result.status,expected,result.stdout+result.stderr);
      assert.equal(readFileSync(join(cwd,'calls'),'utf8').split('\n').filter(line=>line==='windows-features affected').length,1);
      const calls=readFileSync(join(cwd,'calls'),'utf8').split('\n').filter(c=>c.startsWith('test '));
      assert.equal(calls.length,broken&&!negative?2:3);assert.ok(calls.every(c=>c.includes('-p acyclic-harness-fuzz -p reverse-consumer')));
      assert.ok(!calls.some(c=>c.includes('--workspace')),'affected scope remains narrowed');
      if(!negative)assert.match(calls[0],/--all-targets/);
      assert.match(calls.at(-1),/--doc/);assert.ok(calls.filter(c=>!c.includes('--no-run')).every(c=>c.includes('--skip known-flake')));
      assert.match(result.stdout,new RegExp('GATE_DONE rc='+expected));
      if(broken&&!negative)assert.match(result.stdout,/error\[E0425\]: cannot find type Capabilities/);
      assert.match(result.stdout,new RegExp('GATE_TEST_PHASE phase=targets rc='+(broken&&!negative?101:0)+' seconds=\\d+'));
      assert.match(result.stdout,/GATE_TEST_PHASE phase=docs rc=0 seconds=\d+/);
      const targets=readFileSync(join(cwd,'targets'),'utf8').trim().split('\n').map(line=>line.split('|'));
      const testTarget=targets.find(([command])=>command==='test')[1];
      const clippyTarget=targets.find(([command])=>command==='clippy')[1];
      assert.equal(dirname(testTarget),dirname(clippyTarget),'independent phases must share only a parent lease');
      assert.notEqual(testTarget,clippyTarget);
      if(!expected)assert.equal(readFileSync(join(cwd,'.gate-fuzz-executables'),'utf8'),'');
    }
    const oldLayout=source.replace('test_target="$target_root/.gate-pr-tests"','test_target="$target_root"').replace('clippy_target="$target_root/.gate-pr-clippy"','clippy_target="$test_target/gate-clippy"');
    writeFileSync(join(cwd,'runner.sh'),oldLayout);writeFileSync(join(cwd,'targets'),'');
    const old=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...process.env,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),BROKEN_FUZZ:'0'}});
    assert.equal(old.status,0,old.stdout+old.stderr);
    const oldTargets=readFileSync(join(cwd,'targets'),'utf8').trim().split('\n').map(line=>line.split('|'));
    assert.throws(()=>assert.equal(dirname(oldTargets.find(([command])=>command==='test')[1]),dirname(oldTargets.find(([command])=>command==='clippy')[1])),assert.AssertionError);
    // A preparation error must not launch unobserved background Cargo jobs.
    writeFileSync(join(cwd,'calls'),'');
    const failed=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...process.env,PATH:join(cwd,'bin')+':'+process.env.PATH,BROKEN_METADATA:'1'}});
    assert.equal(failed.status,2);assert.match(failed.stdout+failed.stderr,/metadata control/);
    assert.ok(readFileSync(join(cwd,'calls'),'utf8').split('\n').filter(Boolean).every(c=>c.startsWith('metadata ')));
    // A failed consumer-codegen phase must reach the PR's final verdict even
    // when every test, document and clippy invocation succeeds.
    writeFileSync(join(cwd,'scripts/check-feature-matrix.mjs'),'');
    writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),
      '#!/bin/bash\necho "FAILED cargo build --package reverse-consumer"\nexit 1\n');
    const propagation='[ "$rc" != 0 ] || rc=$(cat .gate-features.rc)';
    assert.ok(source.includes(propagation));
    for(const negative of [false,true]){
      writeFileSync(join(cwd,'runner.sh'),negative?source.replace(propagation,':'):source);
      const result=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{
        ...process.env,PATH:join(cwd,'bin')+':'+process.env.PATH,
        CARGO_TARGET_DIR:join(cwd,'target'),GATE_FEATURE_TARGET_DIR:join(cwd,'features'),
        BROKEN_FUZZ:'0',WINDOWS_FEATURE_RC:'0',BROKEN_METADATA:'0',
      }});
      assert.equal(result.status,negative?0:1,result.stdout+result.stderr);
      assert.match(result.stdout,/GATE_FEATURES rc=1/);
      assert.match(result.stdout,new RegExp('GATE_DONE rc='+(negative?0:1)));
    }
  }finally{rmSync(cwd,{recursive:true,force:true});}
});
