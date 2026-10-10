import assert from 'node:assert/strict';
import test from 'node:test';
import {checkReleaseVersion,checkReleaseSource,checkNpmVersions} from './release-source.mjs';
test('dispatch source stays frozen when main advances',()=>{
 const candidate='a'.repeat(40);
 checkReleaseSource(candidate,candidate);
 assert.throws(()=>checkReleaseSource('b'.repeat(40),candidate),/dispatch candidate/);
});
test('npm and plugin manifests must all agree with the coordinated version',()=>{
 const packages=[{source:'typescript',directory:'sdk',name:'@acyclic-labs/sdk'},{source:'plugin',directory:'plugin',name:'@acyclic-labs/plugin'}];
 const manifests={'typescript/packages/sdk/package.json':{name:packages[0].name,version:'0.2.0',private:false},'plugin/package.json':{name:packages[1].name,version:'0.2.0',private:false}};
 checkNpmVersions('0.2.0',packages,file=>manifests[file]);
 for(const field of ['version','name','private']){
  const invalid=structuredClone(manifests);
  invalid['plugin/package.json'][field]=field==='private'?true:'conflict';
  assert.throws(()=>checkNpmVersions('0.2.0',packages,file=>invalid[file]),/plugin\/package.json/);
 }
 for(const invalid of ['',undefined,'v0.2.0','01.2.0','0.2'])assert.throws(()=>checkReleaseVersion(invalid));
 assert.equal(checkReleaseVersion('0.2.0'),'0.2.0');
});
