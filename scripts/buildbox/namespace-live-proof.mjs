// Run inside the shipping build box's existing private-proc job, paired with
// namespace-live-host.py outside it. FIFOs establish a live host witness.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {finish, processIdentity, processState, recover, watch} from './buildbox-receipt.mjs';

const root=process.argv[2];
assert.equal(path.dirname(root),'/mnt/bb/jobs');
assert.ok(path.basename(root).startsWith('approvals-namespace-live-'));
const parent=JSON.parse(fs.readFileSync(process.env.BUILD_SLOT_RECEIPT_META));
fs.writeFileSync(path.join(root,'owner'),JSON.stringify(parent.owner));
const witness=JSON.parse(fs.readFileSync(path.join(root,'witness')));
assert.deepEqual(witness.owner,parent.owner);
assert.equal(witness.live_in_host_namespace,true);
const started=performance.now();
try {
 assert.equal(processState(parent.owner),'foreign');
 assert.notEqual(processIdentity(parent.owner.pid),parent.owner.identity,
  'negative control requires the real live host owner to be invisible or aliased');
 const meta=path.join(root,'hidden.receipt-meta.v2.json');
 const receipt=path.join(root,'terminal.json');
 fs.writeFileSync(meta,JSON.stringify({owner:parent.owner,receipt_file:receipt,argv:[],side:'proof',admission_start:new Date().toISOString()}));
 recover(root);
 assert.equal(fs.existsSync(receipt),false);
 assert.equal(finish(meta,137,'interrupted'),null);
 await watch(meta);
 assert.equal(fs.existsSync(receipt),false);
 // Remove the production namespace fence, retaining its actual former raw-PID
 // recovery condition. The hidden live owner's receipt is then made terminal.
 const source=fs.readFileSync(new URL('./buildbox-receipt.mjs',import.meta.url),'utf8');
 const broken=source.replace('return namespace !== null && owner?.pid_namespace === namespace;', 'return Boolean(owner);');
 assert.notEqual(broken,source);
 const module=path.join(root,'broken.mjs');fs.writeFileSync(module,broken);
 const old=await import(pathToFileURL(module));
 old.recover(root);
 assert.equal(JSON.parse(fs.readFileSync(receipt)).exit_status,137);
 const seconds=(performance.now()-started)/1000;
 assert.ok(seconds<10,`namespace proof exceeded suite budget: ${seconds}`);
 console.log('REAL_PRIVATE_PROC_NAMESPACE_PROOF',JSON.stringify({seconds,owner:parent.owner,
  actual_namespace:fs.readlinkSync('/proc/self/ns/pid'),negative_control:'premature terminal receipt'}));
} finally {fs.rmSync(root,{recursive:true,force:true});}
