const STATIC='static.yml';
const SCHEDULED=[];
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
const workflows=fs.readdirSync('.github/workflows').filter(p=>/\.ya?ml$/.test(p));
const read=name=>fs.readFileSync('.github/workflows/'+name,'utf8');
const triggers=text=>text.match(/^on:\n([\s\S]*?)(?=^[^ #\s])/m)?.[1]??'';
test('PRs and main have one static job, and tags/releases cannot start heavy work',()=>{
 const automatic=workflows.filter(name=>/^  (?:push|pull_request):/m.test(triggers(read(name))));
 assert.deepEqual(automatic,[STATIC]);
 const workflow=read(STATIC);
 assert.equal((workflow.split('jobs:')[1].match(/^  [\w-]+:/gm)??[]).length,1);
 assert.match(workflow,/timeout-minutes: 5/);
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
