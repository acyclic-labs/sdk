import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import test from 'node:test';
const here=path.dirname(new URL(import.meta.url).pathname);
const original=fs.readFileSync(path.join(here,'buildbox-receipt.mjs'),'utf8');
function fixture(){
 const root=fs.mkdtempSync('/mnt/bb/tmp/receipt-pidns-');
 const module=path.join(root,'receipt.mjs');
 fs.writeFileSync(module,original.replaceAll('/mnt/bb/jobs/',root+'/'));
 return {root,module};
}
function run(module,code,unshare=false){
 const args=[process.execPath,'--input-type=module','-e',`import * as r from ${JSON.stringify(module)};${code}`];
 const result=spawnSync(unshare?'sudo':args.shift(),unshare?['-n','--preserve-env','unshare','--pid','--fork','--kill-child','--mount','--mount-proc','setpriv','--reuid=var','--regid=var','--init-groups',...args]:args,{encoding:'utf8'});
 assert.equal(result.status,0,result.stderr+result.stdout);return result;
}
test('private proc cannot recover or finish a live host owner; old recovery is the negative control',()=>{
 const {root,module}=fixture();
 try{
  for(const negative of [false,true]){
   const mod=negative?path.join(root,'unguarded.mjs'):module;
   if(negative)fs.writeFileSync(mod,fs.readFileSync(module,'utf8').replace('return namespace !== null && owner?.pid_namespace === namespace;','return Boolean(owner);'));
   const file=path.join(root,`${negative}.receipt-meta.v2.json`),receipt=file+'.terminal';
   const hostNamespace=fs.readlinkSync('/proc/self/ns/pid');
   const stat=fs.readFileSync(`/proc/${process.pid}/stat`,'utf8').split(') ').pop().split(' ');
   fs.writeFileSync(file,JSON.stringify({owner:{pid:process.pid,identity:fs.readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim()+':'+stat[19],pid_namespace:hostNamespace},receipt_file:receipt,argv:[],admission_start:new Date().toISOString()}));
   run(mod,`r.recover(${JSON.stringify(root)});r.finish(${JSON.stringify(file)},137,'interrupted');`,true);
   assert.equal(fs.existsSync(receipt),negative,'foreign /proc absence is not proof of death');
  }
 }finally{fs.rmSync(root,{recursive:true,force:true});}
});
test('killed namespace descendants get receipts only after their registered outer owner ends',()=>{
 const {root,module}=fixture();
 try{
  const parent=path.join(root,'parent.json'),terminal=parent+'.terminal';
  const child=path.join(root,'child.receipt-meta.v2.json'),childTerminal=child+'.terminal';
  fs.writeFileSync(parent,JSON.stringify({receipt_file:terminal}));
  run(module,`const ns=r.pidNamespace();const registration=${JSON.stringify(root)}+'/'+ns.replaceAll(/[^0-9]/g,'')+'.pid-namespace.json';r.atomic(registration,{namespace:ns,init_identity:r.processIdentity(1),parent_receipt_meta:${JSON.stringify(parent)},registration_file:registration,epoch:'fixture-epoch'});r.arm(${JSON.stringify(child)},{receipt_file:${JSON.stringify(childTerminal)},argv:[],admission_start:new Date().toISOString()});`,true);
  run(module,`r.recover(${JSON.stringify(root)});`);
  assert.equal(fs.existsSync(childTerminal),false,'invisible PIDs remain held while their enclosing owner is active');
  fs.writeFileSync(terminal,JSON.stringify({receipt_file:terminal,namespace_custody:'unproven'}));
  run(module,`r.recover(${JSON.stringify(root)});`);assert.equal(fs.existsSync(childTerminal),false);
  // Exercise the terminal callback too: it visits only registered descendants,
  // so a killed nested job need not wait for another unrelated job to start.
  fs.rmSync(terminal);
  const scope=JSON.parse(fs.readFileSync(child)).namespace_scope;
  fs.writeFileSync(parent+'.pid-namespaces',JSON.stringify(scope.registration_file)+'\n');
  fs.writeFileSync(parent,JSON.stringify({receipt_file:terminal,namespace_custody:'pidns-v1',owner:{pid:process.pid,pid_namespace:fs.readlinkSync('/proc/self/ns/pid')},argv:[],admission_start:new Date().toISOString()}));
  run(module,`r.finish(${JSON.stringify(parent)},0,'command');`);
  assert.equal(fs.existsSync(childTerminal),true,JSON.stringify(fs.readdirSync(root).map(name=>[name,fs.readFileSync(path.join(root,name),'utf8')])));
  const receipt=JSON.parse(fs.readFileSync(childTerminal));
  assert.equal(receipt.exit_status,137);assert.equal(receipt.termination,'interrupted');
 }finally{fs.rmSync(root,{recursive:true,force:true});}
});
