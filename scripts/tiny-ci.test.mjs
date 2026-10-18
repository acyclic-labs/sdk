const STATIC='static.yml';
const SCHEDULED=[];
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import {runInNewContext} from 'node:vm';
import YAML from './vendor/yaml.cjs';
const workflows=fs.readdirSync('.github/workflows').filter(p=>/\.ya?ml$/.test(p));
const read=name=>fs.readFileSync('.github/workflows/'+name,'utf8');
const triggers=text=>text.match(/^on:\n([\s\S]*?)(?=^[^ #\s])/m)?.[1]??'';
test('Linux host qualification uses the smallest registered runner and retains every scenario',()=>{
 const workflow=YAML.parse(read('agent-host-qualification.yml'));
 const job=workflow.jobs.qualify;
 assert.deepEqual(job.strategy.matrix.platform,[
  {name:'linux-fuse',runner:'blacksmith-2vcpu-ubuntu-2404',cache_size:'448M'},
  {name:'macos-nfs',runner:'blacksmith-6vcpu-macos-15',cache_size:'384M'},
  {name:'windows-projfs',runner:'blacksmith-4vcpu-windows-2025',cache_size:'576M'},
 ]);
 const registered=YAML.parse(fs.readFileSync('.github/actionlint.yaml','utf8'))['self-hosted-runner'].labels;
 for(const platform of job.strategy.matrix.platform)assert.ok(registered.includes(platform.runner));
 assert.equal(job['runs-on'],'${{ matrix.platform.runner }}');
 const scenarios=[
  ['Build host scenario',undefined,'cargo test -p acyclic-plugin --test e2e --no-run --locked'],
  ['Run Pi installed diagnostics and native context qualification',undefined,
   'suffix="" if [[ "$RUNNER_OS" == Windows ]]; then suffix=".exe"; fi export ACYCLIC_BINARY="$CARGO_TARGET_DIR/debug/acyclic$suffix" cd plugin/tests/hosts bun run test:pi-context bun run test:pi-context-mail bun run test:pi-bootstrap bun run test:pi-session bun run test:pi-installation bun run test:pi-service bun run test:pi-native-recursive bun run test:pi-context-fresh bun run test:pi-context-pages bun run test:pi-builtin-paths'],
  ['Run codex scenario',undefined,'cargo test -p acyclic-plugin --test e2e actual_codex_binary_executes_the_scripted_scenario -- --ignored --exact --nocapture'],
  ['Run claude scenario','success() || failure()','cargo test -p acyclic-plugin --test e2e actual_claude_binary_executes_the_scripted_scenario -- --ignored --exact --nocapture'],
 ];
 /** @param {ReturnType<typeof YAML.parse>} document */
 const verify=document=>{
  const qualification=document.jobs.qualify;
  assert.equal(qualification.if,undefined);
  const locate=qualification.steps.find(step=>step.name==='Locate build outputs')??assert.fail('missing plugin target setup');
  assert.ok(locate.run.includes('CARGO_TARGET_DIR=$RUNNER_TEMP/agent-host-target'));
  for(const environment of [document.env,qualification.env,...qualification.steps.map(step=>step.env)])
   assert.equal(environment?.CARGO_BUILD_JOBS,undefined,'Cargo follows the runner CPU count');
  for(const [name,condition,command] of scenarios){
   const step=qualification.steps.find(candidate=>candidate.name===name)??assert.fail('missing '+name);
   assert.equal(step.if,condition,name+': condition');
   assert.equal(step.run.replace(/\\\r?\n/g,' ').replace(/\s+/g,' ').trim(),command,name+': command');
  }
 };
 verify(workflow);
 for(const scope of ['workflow','job','step']){
  const altered=structuredClone(workflow);
  const destination=scope==='workflow'?altered:scope==='job'?altered.jobs.qualify:altered.jobs.qualify.steps.find(step=>step.name==='Build host scenario');
  destination.env={...destination.env,CARGO_BUILD_JOBS:'16'};
  assert.throws(()=>verify(altered),/Cargo follows the runner CPU count/);
 }
 for(const [name] of scenarios)for(const change of ['condition','command']){
  const altered=structuredClone(workflow);
  const step=altered.jobs.qualify.steps.find(candidate=>candidate.name===name);
  if(change==='condition')step.if=false;else step.run='true';
  assert.throws(()=>verify(altered),new RegExp(name+': '+change));
 }
});
test('PRs and main retain the static lock check, and tags/releases cannot start heavy work',()=>{
 const automatic=workflows.filter(name=>/^  (?:push|pull_request):/m.test(triggers(read(name))));
 assert.deepEqual(automatic,[STATIC]);
 const workflow=read(STATIC);
 assert.deepEqual(Object.keys(YAML.parse(workflow).jobs),['static','aarch64']);
 const job=YAML.parse(workflow).jobs.static;
 assert.equal(job['timeout-minutes'],1);
 assert.equal(job['runs-on'],'ubuntu-24.04');
 assert.match(workflow,/cancel-in-progress: true/);
 assert.doesNotMatch(triggers(workflow),/paths(?:-ignore)?:|tags:/);
 for(const name of workflows)assert.doesNotMatch(triggers(read(name)),/^  (?:release|workflow_run):/m,name);
});
test('only expiry cleanup is scheduled',()=>{
 assert.deepEqual(workflows.filter(name=>/^  schedule:/m.test(triggers(read(name)))),SCHEDULED);
});

test('every cache save is main-only, with no per-run keys or automatic branch saves',()=>{
 const mainOnly=(condition,label)=>{
  const expression=condition.replace(/^\$\{\{\s*|\s*\}\}$/g,'')
   .replace(/steps\.([\w-]+)\.outputs\.([\w-]+)/g,(_,step,output)=>`steps[${JSON.stringify(step)}].outputs[${JSON.stringify(output)}]`)
   .replace(/steps\.([\w-]+)\.outcome\b/g,(_,step)=>`steps[${JSON.stringify(step)}].outcome`);
  for(const ref of ['refs/heads/topic','refs/pull/451/merge','refs/heads/main'])for(const event of ['workflow_dispatch','push','pull_request','pull_request_target']){
   if(ref==='refs/heads/main'&&!event.startsWith('pull_request'))continue;
   for(const hit of ['true','false'])for(const save of ['true','false'])for(const key of ['', 'key']){
    const steps=new Proxy({}, {get:()=>({outcome:'success',outputs:new Proxy({}, {get:(_,name)=>
     name==='save'?save:name==='cache-hit'||name==='downloads-hit'?hit:key})})});
    assert.equal(runInNewContext(expression,{github:{ref,event_name:event},steps,cancelled:()=>false},{timeout:1000}),false,label+': off-main or PR save');
   }
  }
 };
 const files=workflows.map(name=>'.github/workflows/'+name).concat([
  '.github/actions/qualification-lane/action.yml',
  '.github/actions/rust-build-cache/action.yml',
 ]);
 for(const file of files){
  const document=YAML.parse(fs.readFileSync(file,'utf8'));
  const steps=document.runs?.steps??Object.values(document.jobs??{}).flatMap(job=>job.steps??[]);
  for(const step of steps){
   assert.doesNotMatch(step.uses??'',/^actions\/cache@/,file+': automatic post-save');
   assert.doesNotMatch(step.with?.key??'',/run_id|run_attempt/,file+': per-run snapshot');
   if(step.uses?.startsWith('actions/cache/save@'))
    mainOnly(step.if,file+': '+step.name);
   if(step.uses?.startsWith('Swatinem/rust-cache@'))
    mainOnly(step.with['save-if'],file);
  }
 }
 const lane=YAML.parse(fs.readFileSync('.github/actions/qualification-lane/action.yml','utf8'));
 const hosts=YAML.parse(read('agent-host-qualification.yml'));
 for(const steps of [lane.runs.steps,hosts.jobs.qualify.steps]){
  for(const step of steps.filter(step=>step.uses?.startsWith('actions/cache/')))
   assert.doesNotMatch(step.with.path,/sccache/,'off-critical compiler snapshot');
 }
 for(const condition of [
  "github.ref == 'refs/heads/main' && steps.tools.outputs.cache-hit != 'true' || true",
  "${{ github.ref == 'refs/heads/main' && github.event_name != 'pull_request' || true }}",
 ])assert.throws(()=>mainOnly(condition,'mutation'),/off-main or PR save/);
});

test('main qualification saves completed downloads and tools after failure, but not cancellation or incomplete restore',()=>{
 const action=YAML.parse(fs.readFileSync('.github/actions/qualification-lane/action.yml','utf8'));
 const saves=action.runs.steps.filter(step=>step.uses?.startsWith('actions/cache/save@'));
 assert.equal(saves.length,3);
 for(const step of saves){
  assert.ok(step.if.includes('!cancelled()'),step.name);
  const expression=step.if.replace(/^\$\{\{\s*|\s*\}\}$/g,'')
   .replace(/steps\.([\w-]+)\.outputs\.([\w-]+)/g,(_,id,output)=>`steps[${JSON.stringify(id)}].outputs[${JSON.stringify(output)}]`)
   .replace(/steps\.([\w-]+)\.outcome\b/g,(_,id)=>`steps[${JSON.stringify(id)}].outcome`);
  for(const cancelled of [false,true])for(const key of ['', 'key'])for(const hit of ['true','false'])for(const outcome of ['success','failure','skipped']){
   const steps=new Proxy({}, {get:()=>({outcome,outputs:{'cache-primary-key':key,'cache-hit':hit}})});
   const saved=runInNewContext(expression,{github:{ref:'refs/heads/main',event_name:'workflow_dispatch'},steps,cancelled:()=>cancelled,success:()=>false},{timeout:1000});
   assert.equal(saved,!cancelled&&key!==''&&hit!=='true'&&(step.name!=='Save Windows ARM64 compiler'||outcome==='success'),step.name);
  }
 }
 assert.equal(action.runs.steps.find(step=>step.name==='Install Windows ARM64 compiler').id,'llvm-install');
});

test('all publishers use the one qualified source and explicit artifacts',()=>{
 const release=read('release.yml');
 assert.match(release,/workflow_dispatch:/);
 assert.match(release,/test "\$GITHUB_REF" = refs\/heads\/main/);
 assert.match(release,/uses: \.\/\.github\/workflows\/release-acyclic\.yml/);
 assert.match(release,/qualification_run_attempt: \$\{\{ needs\.qualify\.outputs\.attempt \}\}/);
 assert.match(release,/path: \$\{\{ runner\.temp \}\}\/qualified-plugin-release/);
 assert.match(release,/RELEASE_ASSET_DIR: \$\{\{ runner\.temp \}\}\/qualified-plugin-release/);
 for(const name of ['publish-npm.yml','publish-crate.yml']){
  const publisher=read(name);
  assert.match(triggers(publisher),/workflow_call:/);
  assert.doesNotMatch(triggers(publisher),/workflow_dispatch:/);
  assert.match(publisher,/ref: \$\{\{ inputs\.source_sha \}\}/);
  assert.doesNotMatch(publisher,/actions\/workflows\/[^\n]*\/runs/);
  assert.match(publisher,/id-token: write/);
 }
 const qualification=YAML.parse(read('qualification.yml'));
 for(const name of ['qualification','agent-host-qualification']){
  const workflow=YAML.parse(read(name+'.yml'));
  assert.deepEqual(workflow.on.workflow_call.inputs.source_ref,{...(name==='qualification'?{description:'Exact commit SHA to qualify'}:{}),type:'string',required:true});
  const caller=YAML.parse(read('release-acyclic.yml')).jobs[name==='qualification'?'sdk-qualification':name];
  assert.equal(caller.with.source_ref,'${{ inputs.source_sha }}');
  for(const job of Object.values(workflow.jobs))for(const step of job.steps??[]){
   if(step.uses?.startsWith('actions/checkout@'))assert.equal(step.with.ref,'${{ inputs.source_ref }}');
  }
 }
 assert.equal(qualification.on.workflow_call.inputs.force,undefined);
 assert.deepEqual(Object.keys(qualification.jobs),['plan','qualification','gate']);
 assert.deepEqual(qualification.jobs.plan.steps.filter(step=>step.uses?.startsWith('actions/cache/restore@')).map(step=>step.name),['Restore preflight tools']);
 assert.equal(qualification.jobs.plan.steps.filter(step=>step.name==='Select lanes to execute').length,1);
 assert.equal((read('release-acyclic.yml').match(/uses: \.\/\.github\/workflows\/qualification\.yml/g)??[]).length,1);
});

test('the required job checks Cargo lock consistency and committed lane artifacts without builds or installs',()=>{
 const job=YAML.parse(read(STATIC)).jobs.static;
 assert.equal(job.name,'SDK Static');
 assert.equal(job.env.RUSTUP_TOOLCHAIN,'stable');
 assert.deepEqual(job.steps.filter(step=>step.run).map(step=>step.run),[
  'if [[ -n "$ARTIFACT_BASE" && ! "$ARTIFACT_BASE" =~ ^0+$ ]]; then\n'
   +'  git fetch --no-tags --depth=1 origin "$ARTIFACT_BASE"\n'
   +'fi\n'
   +'node scripts/check-lane-artifacts.mjs "$ARTIFACT_BASE" "$ARTIFACT_HEAD"\n',
  'node --test scripts/lane-artifact-policy.test.mjs scripts/check-lane-artifacts.test.mjs',
  'node --test scripts/local-cloud.test.mjs',
  'node --test scripts/fuzz-oracle-policy.test.mjs\nnode scripts/check-fuzz-oracles.mjs\n',
  'cargo update --workspace --locked',
 ]);
 assert.equal(job.steps.filter(step=>step.uses).length,1);
 assert.doesNotMatch(JSON.stringify(job),/setup-node|static-ci|fmt|clippy|cargo (?:build|test|check)|(?:npm|bun|yarn|pnpm) (?:install|add)|cargo install/);
});
test('README approval guard runs on pull requests without manual dispatch',()=>{
 const guard=triggers(read('readme-guard.yml'));
 assert.match(guard,/^  pull_request_target:/m);
 assert.doesNotMatch(guard,/^  workflow_dispatch:/m);
});

test('heavy workflows are reachable only through the release caller',()=>{
 for(const name of workflows.filter(name=>![STATIC,'release.yml','readme-guard.yml'].includes(name))){
  assert.match(triggers(read(name)),/workflow_call:/,name);
  assert.doesNotMatch(triggers(read(name)),/workflow_dispatch:/,name);
 }
 assert.match(fs.readFileSync('scripts/qualify-ci.sh','utf8'),/policy\)\s+node scripts\/static-ci\.mjs/);
});

test('all artifact uploads expire after three days, or thirty for release runs; coverage expires after one',()=>{
 const sources=workflows.map(name=>['.github/workflows/'+name,read(name)]);
 for(const directory of fs.readdirSync('.github/actions')){
  const path='.github/actions/'+directory+'/action.yml';if(fs.existsSync(path))sources.push([path,fs.readFileSync(path,'utf8')]);
 }
 let uploads=0;
 for(const [path,source] of sources){
  const document=YAML.parse(source);
  const steps=[...(document.runs?.steps??[]),...Object.values(document.jobs??{}).flatMap(job=>job.steps??[])];
  for(const step of steps.filter(step=>step.uses?.startsWith('actions/upload-artifact@'))){
   uploads++;
   const expression=step.with?.['retention-days'];
   const conditionalCoverage=path==='.github/actions/qualification-lane/action.yml'&&step.with.name.includes('inputs.artifact');
   assert.equal(expression,conditionalCoverage
    ? "${{ contains(inputs.artifact, 'coverage') && 1 || contains(github.workflow, 'Release') && 30 || 3 }}"
    : "${{ contains(github.workflow, 'Release') && 30 || 3 }}",path+': '+step.with.name);
   const js=expression.slice(3,-2).replaceAll('github.workflow','workflow').replaceAll('inputs.artifact','artifact');
   for(const workflow of ['SDK Qualification','SDK Release'])for(const artifact of ['packages-linux','coverage']){
    const days=runInNewContext(js,{workflow,artifact,contains:(value,needle)=>value.toLowerCase().includes(needle.toLowerCase())});
    assert.equal(days,conditionalCoverage&&artifact==='coverage'?1:workflow==='SDK Release'?30:3,path);
   }
  }
 }
 assert.ok(uploads>0);
});

test('publishers consume only this coordinated run and explicitly named artifacts',()=>{
 const release=YAML.parse(read('release.yml'));
 for(const job of ['npm','cargo'])assert.equal(release.jobs[job].with.qualification_run_id,'${{ needs.source.outputs.run }}');
 assert.equal(release.jobs.source.outputs.run,'${{ steps.source.outputs.run }}');
 assert.match(fs.readFileSync('scripts/release-source.mjs','utf8'),/run=\$\{process\.env\.GITHUB_RUN_ID\}/);
 for(const name of ['publish-npm.yml','publish-crate.yml']){
  const source=read(name);assert.match(source,/test "\$RUN" = "\$GITHUB_RUN_ID"/);
  assert.doesNotMatch(source,/actions\/workflows\/[^\n]*\/runs|gh run list/);
  for(const match of source.matchAll(/gh run download ([^\n]+)/g))assert.match(match[1],/^"\$run_id" --name /);
  const workflow=YAML.parse(source);
  for(const job of Object.values(workflow.jobs))for(const step of job.steps??[]){
   if(step.uses?.startsWith('actions/download-artifact@'))assert.equal(step.with?.['run-id'],undefined,'downloads default to the current run');
  }
 }
});


test('native builds and mount checks overlap qualification and still gate publication',()=>{
 const release=YAML.parse(read('release.yml'));
 const workflow=YAML.parse(read('release-acyclic.yml'));
 const {build,assemble}=workflow.jobs;
 assert.equal(build.needs,undefined);
 assert.equal(workflow.jobs['sdk-qualification'].needs,undefined);
 assert.equal(workflow.jobs['agent-host-qualification'].needs,undefined);
 assert.equal(workflow.jobs['native-mount'],undefined);
 assert.deepEqual(assemble.needs,['build','sdk-qualification','agent-host-qualification','installer-recovery']);
 const installer=workflow.jobs['installer-recovery'];
 assert.equal(installer.needs,undefined);
 assert.equal(installer.if,undefined);
 assert.equal(installer['runs-on'],'blacksmith-4vcpu-ubuntu-2404');
 assert.equal(installer.permissions.contents,'read');
 assert.equal(installer.steps[0].with.ref,'${{ inputs.source_sha }}');
 assert.equal(installer.steps[1].with['node-version'],'24.15.0');
 assert.equal(installer.steps[2].run,'node --test plugin/scripts/install.test.mjs');
 assert.equal(Object.values(workflow.jobs).flatMap(job=>job.steps??[]).filter(step=>step.run==='node --test plugin/scripts/install.test.mjs').length,1);
 assert.equal(assemble.if,undefined);
 const matrix=build.strategy.matrix.include;
 assert.deepEqual(matrix.map(row=>row.target).sort(),['darwin-arm64','darwin-x64','linux-arm64-gnu','linux-arm64-musl','linux-x64-gnu','linux-x64-musl','win32-arm64','win32-x64']);
 const windowsArm=matrix.find(row=>row.target==='win32-arm64')??assert.fail('missing Windows ARM64 build');
 assert.equal(windowsArm.runner,'blacksmith-4vcpu-windows-2025');
 assert.equal(build.env.SCCACHE_IDLE_TIMEOUT,'0');
 assert.equal(windowsArm.rust_target,'aarch64-pc-windows-msvc');
 assert.equal(windowsArm.executable,'acyclic.exe');
 assert.ok(fs.readFileSync('.github/actionlint.yaml','utf8').includes(windowsArm.runner));
 assert.deepEqual(matrix.filter(row=>row.backend).map(row=>[row.target,row.backend,row.qualifier]),[
  ['linux-x64-gnu','linux-fuse','native-mount-qualify'],
  ['darwin-arm64','macos-nfs','native-mount-qualify'],
  ['win32-x64','windows-projfs','native-mount-qualify.exe'],
 ]);
 for(const [target,runner] of new Map([
  ['linux-x64-gnu','blacksmith-16vcpu-ubuntu-2404'],
  ['linux-arm64-gnu','blacksmith-16vcpu-ubuntu-2404-arm'],
  ['linux-arm64-musl','blacksmith-16vcpu-ubuntu-2404-arm'],
  ['win32-x64','blacksmith-32vcpu-windows-2025'],
 ])){
  const row=matrix.find(row=>row.target===target)??assert.fail(target);
  assert.equal(row.runner,runner);
  assert.ok(fs.readFileSync('.github/actionlint.yaml','utf8').includes(row.runner));
 }
 const steps=build.steps;
 const linux=steps.find(step=>step.name==='Qualify Linux or macOS native mount');
 const windows=steps.find(step=>step.name==='Qualify Windows native mount');
 assert.equal(linux.if,"matrix.backend && runner.os != 'Windows'");
 assert.equal(windows.if,"matrix.backend == 'windows-projfs'");
 assert.equal(linux.run,"bash scripts/qualify-native-mount.sh '${{ matrix.backend }}'");
 assert.equal(windows.run,".\\scripts\\qualify-native-mount.ps1 -Backend '${{ matrix.backend }}'");
 for(const step of [linux,windows]){
  assert.equal(step['timeout-minutes'],20);
  assert.equal(step.env.ACYCLIC_RELEASE_EXECUTABLE,'${{ runner.temp }}/release/${{ matrix.target }}/${{ matrix.executable }}');
  assert.equal(step.env.ACYCLIC_QUALIFIER_EXECUTABLE,'${{ runner.temp }}/release/${{ matrix.target }}/${{ matrix.qualifier }}');
 }
 const compile=steps.find(step=>step.name==='Build the unified executable');
 assert.ok(compile.run.includes('--target-dir "$RUNNER_TEMP/target-qualifier"'));
 const stage=steps.find(step=>step.name==='Stage normalized executable');
 assert.ok(stage.run.includes('"$RUNNER_TEMP/target-qualifier/'));
 assert.ok(stage.run.includes('mkdir -p "$RUNNER_TEMP/release/'));
 const product=steps.find(step=>step.with?.name==='acyclic-${{ matrix.target }}');
 assert.equal(product.with.path,'${{ runner.temp }}/release');
 const retained=steps.find(step=>step.name==='Retain native-mount qualification');
 assert.equal(retained.if,'matrix.backend');
 assert.equal(retained.with.name,'native-mount-${{ matrix.backend }}');
 assert.equal(retained.with['if-no-files-found'],'error');
 assert.ok(steps.indexOf(linux)>steps.findIndex(step=>step.name==='Stage normalized executable'));
 assert.ok(steps.indexOf(retained)>steps.indexOf(windows));
 for(const name of ['npm','cargo','publish']){
  assert.ok(release.jobs[name].needs.includes('qualify'));
  assert.equal(release.jobs[name].if,'inputs.publish');
 }
});
