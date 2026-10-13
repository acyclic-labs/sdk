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
test('PRs and main have one static job, and tags/releases cannot start heavy work',()=>{
 const automatic=workflows.filter(name=>/^  (?:push|pull_request):/m.test(triggers(read(name))));
 assert.deepEqual(automatic,[STATIC]);
 const workflow=read(STATIC);
 assert.equal((workflow.split('jobs:')[1].match(/^  [\w-]+:/gm)??[]).length,1);
 assert.match(workflow,/timeout-minutes: 1/);
 assert.match(workflow,/cancel-in-progress: true/);
 assert.doesNotMatch(triggers(workflow),/paths(?:-ignore)?:|tags:/);
 for(const name of workflows)assert.doesNotMatch(triggers(read(name)),/^  (?:release|workflow_run):/m,name);
});
test('only expiry cleanup is scheduled',()=>{
 assert.deepEqual(workflows.filter(name=>/^  schedule:/m.test(triggers(read(name)))),SCHEDULED);
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
 assert.match(read('release-acyclic.yml'),/force: true/);
 assert.equal((read('release-acyclic.yml').match(/uses: \.\/\.github\/workflows\/qualification\.yml/g)??[]).length,1);
});

test('the required job only checks Cargo lock consistency',()=>{
 const workflow=read(STATIC);
 assert.match(workflow,/name: SDK Static/);
 assert.match(workflow,/RUSTUP_TOOLCHAIN: stable/);
 assert.deepEqual([...workflow.matchAll(/^        run: (.+)$/gm)].map(match=>match[1]),['cargo update --workspace --locked']);
 assert.equal((workflow.match(/^\s+- uses:/gm)??[]).length,1);
 assert.doesNotMatch(workflow,/setup-node|static-ci|fmt|clippy|cargo (?:build|test|check)/);
});
test('heavy workflows are reachable only through the release caller',()=>{
 for(const name of workflows.filter(name=>![STATIC,'release.yml'].includes(name))){
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
 assert.deepEqual(assemble.needs,['build','sdk-qualification','agent-host-qualification']);
 assert.equal(assemble.if,undefined);
 const matrix=build.strategy.matrix.include;
 assert.deepEqual(matrix.map(row=>row.target).sort(),['darwin-arm64','darwin-x64','linux-arm64-gnu','linux-arm64-musl','linux-x64-gnu','linux-x64-musl','win32-arm64','win32-x64']);
 assert.deepEqual(matrix.filter(row=>row.backend).map(row=>[row.target,row.backend,row.qualifier]),[
  ['linux-x64-gnu','linux-fuse','native-mount-qualify'],
  ['darwin-arm64','macos-nfs','native-mount-qualify'],
  ['win32-x64','windows-projfs','native-mount-qualify.exe'],
 ]);
 for(const target of ['linux-arm64-gnu','linux-arm64-musl','win32-x64']){
  const row=matrix.find(row=>row.target===target)??assert.fail(target);
  assert.match(row.runner,/blacksmith-16vcpu-/);
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
