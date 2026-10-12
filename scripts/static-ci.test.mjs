import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import test from 'node:test';

function fixture(body){
 const root=mkdtempSync(join(tmpdir(),'sdk-release-static-'));
 const git=(...args)=>{
  const result=spawnSync('git',args,{cwd:root,encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
  return result.stdout.trim();
 };
 try{
  mkdirSync(join(root,'scripts/vendor'),{recursive:true});
  for(const name of ['static-ci.mjs','static-policy.mjs','vendor/yaml.cjs'])copyFileSync(resolve('scripts',name),join(root,'scripts',name));
  git('init','-q');git('config','tag.gpgSign','false');git('config','commit.gpgSign','false');git('config','user.name','Contract');git('config','user.email','contract@example.invalid');
  const commit=message=>{git('add','.');git('commit','--allow-empty','-qm',message);};
  commit('baseline');
  body({root,git,commit,check:()=>spawnSync(process.execPath,['scripts/static-ci.mjs'],{cwd:root,encoding:'utf8'})});
 }finally{rmSync(root,{recursive:true,force:true});}
}

test('release whitespace catches an earlier commit and cannot be hidden by a tag at HEAD',()=>fixture(({root,git,commit,check})=>{
 git('tag','acyclic-v0.1.0');
 writeFileSync(join(root,'earlier.txt'),'trailing space \n');commit('earlier error');
 writeFileSync(join(root,'later.txt'),'clean\n');commit('clean final commit');
 git('tag','acyclic-v0.2.0');
 const result=check();
 assert.notEqual(result.status,0);
 assert.match(result.stdout,/earlier\.txt.*trailing whitespace/);
}));

test('a release checks its full range without rechecking inherited whitespace',()=>fixture(({root,git,commit,check})=>{
 writeFileSync(join(root,'inherited.txt'),'old space \n');commit('prior release');git('tag','acyclic-v0.1.0');
 writeFileSync(join(root,'current.txt'),'clean\n');commit('new release');
 const result=check();assert.equal(result.status,0,result.stdout+result.stderr);
}));

test('the first release checks the tracked tree when no prior release exists',()=>fixture(({root,commit,check})=>{
 writeFileSync(join(root,'earlier.txt'),'trailing space \n');commit('earlier error');commit('clean final commit');
 const result=check();assert.notEqual(result.status,0);assert.match(result.stdout,/earlier\.txt.*trailing whitespace/);
}));

test('release policy fetches complete history and docs describe only release diagnostics',()=>{
 const workflow=readFileSync('.github/workflows/qualification.yml','utf8');
 assert.match(workflow,/fetch-depth: \$\{\{ matrix\.lane == 'policy' && '0' \|\| '1' \}\}/);
 assert.match(workflow,/fetch-tags: \$\{\{ matrix\.lane == 'policy' \}\}/);
 const docs=readFileSync('docs/ci-releases.md','utf8');
 assert.match(docs,/publish=false/);assert.match(docs,/no independent manual triggers/);
 assert.doesNotMatch(docs,/Manual diagnostic qualification remains available|native-mount-qualification\.yml|five-minute job/);
});
