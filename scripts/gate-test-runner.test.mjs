import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {chmodSync,mkdtempSync,mkdirSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
const source=readFileSync(new URL('./gate-test-runner.sh',import.meta.url),'utf8');
test('Cargo artifact package, kind and executable identity distinguish same-name tests',()=>{
 const cwd=mkdtempSync(join(tmpdir(),'gate-fuzz-owner-'));
 try{
  for(const dir of ['ordinary/src','ordinary/tests','fuzz'])mkdirSync(join(cwd,dir),{recursive:true});
  writeFileSync(join(cwd,'Cargo.toml'),'[workspace]\nresolver="2"\nmembers=["ordinary","fuzz"]\n');
  writeFileSync(join(cwd,'ordinary/Cargo.toml'),'[package]\nname="ordinary"\nversion="0.0.0"\nedition="2021"\n');
  writeFileSync(join(cwd,'ordinary/src/lib.rs'),'');
  writeFileSync(join(cwd,'ordinary/tests/filesystem_access.rs'),'#[test] fn ordinary_test() {}');
  writeFileSync(join(cwd,'fuzz/Cargo.toml'),'[package]\nname="fuzz"\nversion="0.0.0"\nedition="2021"\n[package.metadata]\ncargo-fuzz=true\n[[bin]]\nname="filesystem_access"\npath="driver.rs"\ntest=false\nharness=false\n');
  writeFileSync(join(cwd,'fuzz/driver.rs'),'fn main() { assert_eq!(std::env::args().skip(1).collect::<Vec<_>>(), vec!["-runs=1", "-seed=1"]); println!("fuzz-driver-ok"); }');
  const runner=join(cwd,'runner');writeFileSync(runner,source);chmodSync(runner,0o755);
  const env={...process.env,CARGO_TARGET_DIR:join(cwd,'target'),GATE_FUZZ_EXECUTABLES:join(cwd,'executables')};
  delete env.CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER;
  const lock=spawnSync('cargo',['generate-lockfile','--offline'],{cwd,env,encoding:'utf8'});assert.equal(lock.status,0,lock.stderr);
  const metadata=spawnSync('cargo',['metadata','--offline','--no-deps','--format-version','1'],{cwd,env,encoding:'utf8'});
  assert.equal(metadata.status,0,metadata.stderr);writeFileSync(join(cwd,'metadata'),metadata.stdout);
  const build=spawnSync('bash',[runner,'--build',join(cwd,'metadata'),env.GATE_FUZZ_EXECUTABLES,'--','cargo','test','--locked','--workspace','--all-targets'],{cwd,env,encoding:'utf8'});
  assert.equal(build.status,0,build.stdout+build.stderr);
  const artifacts=readFileSync(env.GATE_FUZZ_EXECUTABLES+'.artifacts','utf8').trim().split('\n').map(JSON.parse);
  const bins=artifacts.filter(a=>a.reason==='compiler-artifact'&&a.executable&&a.target.name==='filesystem_access');
  assert.equal(bins.length,2);
  const fuzz=bins.find(a=>a.target.kind[0]==='bin').executable;
  const ordinary=bins.find(a=>a.target.kind[0]==='test').executable;
  assert.equal(readFileSync(env.GATE_FUZZ_EXECUTABLES,'utf8'),fuzz+'\n');
  for(const [path,negative,expected] of [[ordinary,false,0],[fuzz,false,0],[ordinary,true,101]]){
   writeFileSync(runner,negative?source.replace('if [ "$match" = 0 ]; then','if [[ ${program##*/} == filesystem_access-* ]]; then'):source);
   const result=spawnSync('bash',[runner,path,'--skip','does-not-exist'],{cwd,env,encoding:'utf8'});
   // libtest rejects -runs with a nonzero status (the exact code varies by Rust).
   if(negative){assert.notEqual(result.status,0);assert.match(result.stderr,/Unrecognized option: 'r'/);}
   else{assert.equal(result.status,expected,result.stdout+result.stderr);assert.match(result.stdout,path===ordinary?/ordinary_test.*ok/:/fuzz-driver-ok/);}
  }
  writeFileSync(runner,source);
  const missing=spawnSync('bash',[runner,ordinary],{cwd,env:{...env,GATE_FUZZ_EXECUTABLES:join(cwd,'missing')},encoding:'utf8'});
  assert.equal(missing.status,2);assert.match(missing.stderr,/missing completed compiler-artifact/);
  // A bin from another package and a test target in the fuzz package are
  // deliberately forged same-name events. Neither may enter the manifest.
  const forged=bins.map(a=>({...a,package_id:a.target.kind[0]==='bin'?bins.find(b=>b.target.kind[0]==='test').package_id:bins.find(b=>b.target.kind[0]==='bin').package_id}));
  writeFileSync(join(cwd,'forged'),forged.map(a=>JSON.stringify(a)+'\n').join(''));
  const collect=spawnSync('bash',[runner,'--collect',join(cwd,'metadata'),join(cwd,'forged'),env.GATE_FUZZ_EXECUTABLES],{cwd,env,encoding:'utf8'});
  assert.equal(collect.status,0,collect.stderr);assert.equal(readFileSync(env.GATE_FUZZ_EXECUTABLES,'utf8'),'');
  writeFileSync(join(cwd,'forged'),'not Cargo JSON\n');
  const invalid=spawnSync('bash',[runner,'--collect',join(cwd,'metadata'),join(cwd,'forged'),env.GATE_FUZZ_EXECUTABLES],{cwd,env,encoding:'utf8'});
  assert.equal(invalid.status,2);assert.match(invalid.stderr,/error: GATE_RUNNER_INFRA compiler-artifact discovery:/);
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
  writeFileSync(join(cwd,'scripts/main-gate-checks.sh'),'# Main baseline completion v2\n# Fuzz executable identity v2\ntest "$GATE_TEST_RUNNER_SCRIPT" = .gate-test-runner.sh || exit 71\ncmp .gate-test-runner.sh expected || exit 72\necho corrected-helper-selected\n');
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
