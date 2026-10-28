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
  // Dispatch controls own their cache roots, regardless of the enclosing gate.
  // Never contend with a real gate's feature lock or inherit its worker count.
  const fixtureEnv={...process.env, SDK_GATE_CLIPPY: join(cwd,"scripts/external-gate/gate-clippy.py"), SDK_WINDOWS_GATE: join(cwd,"scripts/external-gate/gate-checks.sh"),CARGO_TARGET_DIR:join(cwd,'target'),
    GATE_FEATURE_TARGET_DIR:join(cwd,'features'),GATE_FEATURE_WORKERS:'1'};
  try{
    mkdirSync(join(cwd,'bin'));mkdirSync(join(cwd,'scripts'));
    mkdirSync(join(cwd,'scripts/external-gate'),{recursive:true});
    writeFileSync(join(cwd,'scripts/external-gate/gate-checks.sh'),'#!/bin/bash\n[ "$1" = --windows-runtime-only ] || exit 9\necho "GATE_WINDOWS_RUNTIME_CLIPPY rc=0"\n');
    writeFileSync(join(cwd,'scripts/gate-rust-format.sh'),readFileSync(join(scripts,'gate-rust-format.sh')));
    writeFileSync(join(cwd,'bin/bun'),'#!/bin/bash\necho "format $*" >> calls\nexit "${FORMAT_RC:-0}"\n');
    chmodSync(join(cwd,'bin/bun'),0o755);
    writeFileSync(join(cwd,'scripts/gate-test-runner.sh'),readFileSync(join(scripts,'gate-test-runner.sh')));
    writeFileSync(join(cwd,'scripts/gate-windows-features.sh'),'#!/bin/bash\necho "windows-features $*" >> calls\nexit "${WINDOWS_FEATURE_RC:-0}"\n');
    writeFileSync(join(cwd,'scripts/check-harness-models.sh'),'#!/bin/bash\necho "models $*" >> calls\nexit "${MODELS_RC:-0}"\n');
    writeFileSync(join(cwd,'scripts/external-gate/gate-clippy.py'),readFileSync(process.env.SDK_GATE_CLIPPY || '/home/var/bin/gate-clippy.py'));
    const cargo=join(cwd,'bin/cargo');
    writeFileSync(cargo,`#!/bin/bash
echo "$*" >> calls
echo "$1|$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER|$GATE_FUZZ_EXECUTABLES" >> phase-env
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
      const result=spawnSync('bash',[runner,'--skip','known-flake'],{cwd,encoding:'utf8',env:{...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),BROKEN_FUZZ:broken?'1':'0',WINDOWS_FEATURE_RC:String(windows)}});
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
    const old=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),BROKEN_FUZZ:'0'}});
    assert.equal(old.status,0,old.stdout+old.stderr);
    const oldTargets=readFileSync(join(cwd,'targets'),'utf8').trim().split('\n').map(line=>line.split('|'));
    assert.throws(()=>assert.equal(dirname(oldTargets.find(([command])=>command==='test')[1]),dirname(oldTargets.find(([command])=>command==='clippy')[1])),assert.AssertionError);
    // A preparation error must not launch unobserved background Cargo jobs.
    writeFileSync(join(cwd,'calls'),'');
    const failed=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,BROKEN_METADATA:'1'}});
    assert.equal(failed.status,2);assert.match(failed.stdout+failed.stderr,/metadata control/);
    const preparationCalls=readFileSync(join(cwd,'calls'),'utf8').split('\n').filter(Boolean);
    assert.equal(preparationCalls.shift(),'format scripts/qualify-rustfmt.mjs');
    assert.ok(preparationCalls.every(c=>c.startsWith('metadata ')));
    // A failed consumer-codegen phase must reach the PR's final verdict even
    // when every test, document and clippy invocation succeeds.
    writeFileSync(join(cwd,'scripts/check-feature-matrix.mjs'),'');
    writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),
      '#!/bin/bash\necho "FAILED cargo build --package reverse-consumer"\nexit 1\n');
    const propagation='[ "$rc" != 0 ] || rc=$(cat "$gate_output/.gate-features.rc")';
    assert.ok(source.includes(propagation));
    for(const negative of [false,true]){
      writeFileSync(join(cwd,'runner.sh'),negative?source.replace(propagation,':'):source);
      const result=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{
        ...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,
        CARGO_TARGET_DIR:join(cwd,'target'),GATE_FEATURE_TARGET_DIR:join(cwd,'features'),
        BROKEN_FUZZ:'0',WINDOWS_FEATURE_RC:'0',BROKEN_METADATA:'0',
      }});
      assert.equal(result.status,negative?0:1,result.stdout+result.stderr);
      assert.match(result.stdout,/GATE_FEATURES rc=1/);
      assert.match(result.stdout,new RegExp('GATE_DONE rc='+(negative?0:1)));
    }
    const formatHook='bash scripts/gate-rust-format.sh || exit $?';
    assert.ok(source.includes(formatHook));
    writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),'#!/bin/bash\nexit 0\n');
    for(const mode of ['affected','full'])for(const negative of [false,true]){
      writeFileSync(join(cwd,'.gate-scope-mode'),mode+'\n');
      writeFileSync(join(cwd,'runner.sh'),negative?source.replace(formatHook,':'):source);
      writeFileSync(join(cwd,'calls'),'');
      const result=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{
        ...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,
        CARGO_TARGET_DIR:join(cwd,'target'),GATE_FEATURE_TARGET_DIR:join(cwd,'features'),
        FORMAT_RC:'101',BROKEN_FUZZ:'0',WINDOWS_FEATURE_RC:'0',BROKEN_METADATA:'0',
      }});
      assert.equal(result.status,negative?0:101,result.stdout+result.stderr);
      if(!negative){
        const expected='format scripts/qualify-rustfmt.mjs';
        assert.deepEqual(readFileSync(join(cwd,'calls'),'utf8').trim().split('\n'),[expected]);
        assert.match(result.stdout,/GATE_RUSTFMT rc=101/);
      }
    }
    // Exercise the real helper too: private jobs keep generated metadata,
    // artifacts, diagnostics and statuses outside the uploaded source tree.
    writeFileSync(join(cwd,'scripts/gate-target-cache.py'),readFileSync(join(scripts,'gate-target-cache.py')));
    writeFileSync(join(cwd,'.gate-scope-mode'),'affected\n');
    writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),readFileSync(join(scripts,'gate-feature-matrix.sh')));
    writeFileSync(join(cwd,'scripts/gate-feature-payload.sh'),readFileSync(join(scripts,'gate-feature-payload.sh')));
    writeFileSync(join(cwd,'scripts/check-consumer-builds.mjs'),'process.exit(0);\n');
    writeFileSync(join(cwd,'runner.sh'),source);
    for(const file of ['.gate-pr-metadata.json','.gate-fuzz-executables','.gate-fuzz-executables.artifacts','.gate-clippy.out','.gate-clippy.rc','.gate-tests.out','.gate-harness-loom.out'])rmSync(join(cwd,file),{force:true});
    const privateRun=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),BROKEN_FUZZ:'0'}});
    assert.equal(privateRun.status,0,privateRun.stdout+privateRun.stderr);
    assert.throws(()=>readFileSync(join(cwd,'.gate-pr-metadata.json')),/ENOENT/);
    const output=/^GATE_OUTPUT_DIRECTORY (.+)$/mu.exec(privateRun.stdout);
    assert.ok(output,'private invocation retains its actual output identity');
    assert.match(readFileSync(join(output[1],'.gate-features.out'),'utf8'),
      new RegExp('GATE_FEATURES_CACHE target='+join(cwd,'features')+' workers=1 '));
    assert.ok(JSON.parse(readFileSync(join(output[1],'.gate-pr-metadata.json'),'utf8')).packages.length);
    assert.throws(()=>readFileSync(join(cwd,'.gate-harness-loom.out')),/ENOENT/);
    assert.equal(readFileSync(join(output[1],'.gate-harness-loom.out'),'utf8'),'');
    writeFileSync(join(cwd,'scripts/check-feature-matrix.mjs'),"console.log('FAILED cargo check --manifest-path fixture/Cargo.toml');console.error('error[E0000]: retained feature diagnostic');console.log('feature matrix: 0/1 sets ok');process.exitCode=101;\n");
    for(const negative of [false,true]){
      const retaining='if [ "$(cat "$gate_output/.gate-features.rc")" != 0 ]; then cat "$gate_output/.gate-features.out"; fi';
      writeFileSync(join(cwd,'runner.sh'),negative?source.replace(retaining,''):source);
      const failedFeatures=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),BROKEN_FUZZ:'0'}});
      assert.equal(failedFeatures.status,101,failedFeatures.stdout+failedFeatures.stderr);
      assert.match(failedFeatures.stdout,/GATE_FEATURES rc=101/);
      if(negative)assert.doesNotMatch(failedFeatures.stdout,/error\[E0000\]: retained feature diagnostic/);
      else assert.match(failedFeatures.stdout,/error\[E0000\]: retained feature diagnostic/);
    }
    // Cache/output custody was exercised above. The remaining controls test
    // model dispatch and compiler environments; do not repeat cache setup.
    rmSync(join(cwd,'scripts/gate-target-cache.py'));
    writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),'#!/bin/bash\nexit 0\n');
    // All ordinary tests can pass while a cfg-only Loom composition fails.
    // Exercise the actual final PR verdict, then delete its model phase as the
    // negative control: the unchanged broken composition must become green.
    writeFileSync(join(cwd,'.gate-scope-mode'),'affected\n');
    writeFileSync(join(cwd,'.gate-tests-packages'),'acyclic-harness-core\n');
    const modelPhase=/loom_started=\$SECONDS[\s\S]*?\[ "\$rc" != 0 \] \|\| rc=\$loom_rc\n/;
    assert.match(source,modelPhase);
    for(const negative of [false,true]){
      writeFileSync(join(cwd,'runner.sh'),negative?source.replace(modelPhase,''):source);
      writeFileSync(join(cwd,'calls'),'');
      const result=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{
        ...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,
        CARGO_TARGET_DIR:join(cwd,'target'),GATE_FEATURE_TARGET_DIR:join(cwd,'features'),
        FORMAT_RC:'0',BROKEN_FUZZ:'0',WINDOWS_FEATURE_RC:'0',BROKEN_METADATA:'0',MODELS_RC:'101',
      }});
      assert.equal(result.status,negative?0:101,result.stdout+result.stderr);
      const modelCalls=readFileSync(join(cwd,'calls'),'utf8').split('\n').filter(c=>c.startsWith('models '));
      assert.deepEqual(modelCalls,negative?[]:['models --packages-file .gate-tests-packages']);
      assert.match(result.stdout,new RegExp('GATE_DONE rc='+(negative?0:101)));
    }
    const exports='export GATE_FUZZ_EXECUTABLES="$gate_fuzz_executables" GATE_BASE_TEST_RUNNER="$gate_base_test_runner"\nexport CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$test_runner"\n';
    assert.ok(source.includes(exports));
    writeFileSync(join(cwd,'scripts/gate-feature-matrix.sh'),'#!/bin/bash\necho "features|$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER|$GATE_FUZZ_EXECUTABLES" >> phase-env\nexit 0\n');
    for(const negative of [false,true]){
      writeFileSync(join(cwd,'runner.sh'),negative?source.replace(exports,'').replace('( CARGO_TARGET_DIR="$clippy_target"',exports+'( CARGO_TARGET_DIR="$clippy_target"'):source);
      writeFileSync(join(cwd,'phase-env'),'');
      const result=spawnSync('bash',[join(cwd,'runner.sh')],{cwd,encoding:'utf8',env:{...fixtureEnv,PATH:join(cwd,'bin')+':'+process.env.PATH,CARGO_TARGET_DIR:join(cwd,'target'),GATE_FEATURE_TARGET_DIR:join(cwd,'features'),CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:'',GATE_FUZZ_EXECUTABLES:'',FORMAT_RC:'0',BROKEN_FUZZ:'0',WINDOWS_FEATURE_RC:'0',BROKEN_METADATA:'0'}});
      assert.equal(result.status,0,result.stdout+result.stderr);
      const phases=readFileSync(join(cwd,'phase-env'),'utf8').trim().split('\n').map(line=>line.split('|'));
      const compilerInputs=()=>{for(const phase of ['clippy','features'])assert.deepEqual(phases.find(([name])=>name===phase),[phase,'','']);};
      if(negative)assert.throws(compilerInputs,assert.AssertionError);else compilerInputs();
      for(const [,runner,manifest] of phases.filter(([name])=>name==='test')){assert.ok(runner.endsWith('gate-test-runner.sh'));assert.ok(manifest.endsWith('.gate-fuzz-executables'));}
    }
  }finally{rmSync(cwd,{recursive:true,force:true});}
});

test('main and affected PRs use the same complete model qualifier', () => {
  const cwd = mkdtempSync(join(tmpdir(), 'gate-model-scope-'));
  try {
    mkdirSync(join(cwd, 'bin'));
    const runner = join(cwd, 'models.sh');
    writeFileSync(runner, readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'check-harness-models.sh')));
    const node = join(cwd, 'bin/node');
    writeFileSync(node, '#!/bin/bash\necho "$*" >> calls\nexit "${MODEL_RC:-0}"\n');
    chmodSync(node, 0o755);
    const core = 'scripts/qualify-mcp-loom.mjs --child-views --all-features';
    const facade = ['--harness-models', '--query-model'].map(flag => `scripts/qualify-mcp-loom.mjs ${flag} --all-features`);
    const env = {...process.env, SDK_GATE_CLIPPY: join(cwd,"scripts/external-gate/gate-clippy.py"), SDK_WINDOWS_GATE: join(cwd,"scripts/external-gate/gate-checks.sh"), PATH: join(cwd, 'bin') + ':' + process.env.PATH, MODEL_RC: '0'};
    for (const [packages, expected] of [[null, [core, ...facade]], [['acyclic-harness-core'], [core]], [['acyclic-harness'], facade], [['acyclic-harness-core', 'acyclic-harness'], [core, ...facade]], [['acyclic-fs'], []]]) {
      writeFileSync(join(cwd, 'calls'), '');
      if (packages !== null) writeFileSync(join(cwd, 'packages'), packages.join('\r\n'));
      const result = spawnSync('bash', [runner, ...(packages === null ? [] : ['--packages-file', 'packages'])], {cwd, env, encoding: 'utf8'});
      assert.equal(result.status, 0, result.stdout + result.stderr);
      assert.deepEqual(readFileSync(join(cwd, 'calls'), 'utf8').trim().split('\n').filter(Boolean), expected);
    }
    const missing = spawnSync('bash', [runner, '--packages-file', 'absent'], {cwd, env, encoding: 'utf8'});
    assert.equal(missing.status, 2);
    writeFileSync(join(cwd, 'packages'), 'acyclic-harness-core\n');
    const broken = spawnSync('bash', [runner, '--packages-file', 'packages'], {cwd, env: {...env, MODEL_RC: '101'}, encoding: 'utf8'});
    assert.equal(broken.status, 101, broken.stdout + broken.stderr);
  } finally {
    rmSync(cwd, {recursive: true, force: true});
  }
});
