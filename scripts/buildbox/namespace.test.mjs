import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {finish, processIdentity, processOwner, processState, recover, watch} from './buildbox-receipt.mjs';

test('receipt recovery cannot treat a foreign PID namespace as a dead owner', async () => {
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'receipt-namespace-'));
 try {
  const live=processOwner();
  const child=spawnSync(process.execPath,['--input-type=module','-e',
   `import {processOwner} from ${JSON.stringify(new URL('./buildbox-receipt.mjs',import.meta.url).href)};console.log(JSON.stringify(processOwner()));`],{encoding:'utf8'});
  assert.equal(child.status,0,child.stderr);
  const dead=JSON.parse(child.stdout);
  assert.equal(processState(live),'live');
  assert.equal(processState(dead),'dead');
  assert.equal(processState({...live,identity:live.identity+'-reused'}),'dead');
  let oldShapeOrphans=0;
  for(const actual of [live,dead]) for(const identity of [actual.identity,null,'different-birth']) {
   for(const namespace of [undefined,live.pid_namespace+'-foreign']) {
    const owner={...actual,identity,pid_namespace:namespace};
    const expected=namespace && identity ? 'foreign':'unverifiable';
    assert.equal(processState(owner),expected);
    const meta=path.join(root,`${oldShapeOrphans}-${actual.pid}-${String(identity)}.receipt-meta.v2.json`);
    const receipt=meta+'.receipt.json';
    fs.writeFileSync(meta,JSON.stringify({owner,receipt_file:receipt,argv:[],admission_start:new Date().toISOString()}));
    // Both global recovery and a directly invoked watcher must lack deletion
    // authority. A missing process in a private /proc is not an exit receipt.
    recover(root);
    assert.equal(fs.existsSync(receipt),false);
    if (identity === null) assert.throws(()=>finish(meta,137,'interrupted'),/receipt metadata INFRA/);
    else assert.equal(finish(meta,137,'interrupted'),null);
    await watch(meta);
    assert.equal(fs.existsSync(receipt),false);
    // The former production condition misclassified these owners as orphaned.
    if(processIdentity(owner.pid)!==owner.identity) oldShapeOrphans++;
   }
  }
  assert.ok(oldShapeOrphans>0,'negative control never reached destructive recovery');
 } finally {fs.rmSync(root,{recursive:true,force:true});}
});
