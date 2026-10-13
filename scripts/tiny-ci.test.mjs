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
