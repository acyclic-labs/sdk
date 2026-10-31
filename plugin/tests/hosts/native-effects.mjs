// Native shells execute this same filesystem effect on every platform. It
// supplies no host loop, tool response, process event or authentication input.
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {openSync,closeSync} from 'node:fs';
import {access,readFile,realpath,writeFile} from 'node:fs/promises';
import {dirname,isAbsolute,join,relative,sep} from 'node:path';
import {assertNativeResume,nativeSessionIdentity} from './native-session-proof.mjs';

const [phase,workspace,host,...extra]=process.argv.slice(2);
let step='admit native filesystem effect';
try {
 assert.ok(['parent','child','merge','resumed','finish'].includes(phase)&&isAbsolute(workspace??'')
  &&['codex','claude-code','pi'].includes(host)&&!extra.length);
 const root=await realpath(workspace),cwd=await realpath(process.cwd());
 const text=path=>readFile(path,'utf8');
 const absent=async path=>{
  try {await access(path);} catch(error) {if(error.code==='ENOENT')return;throw error;}
  assert.fail('child file became visible before its merge: '+path);
 };
 const parent=()=>assert.equal(cwd,root,'native parent cwd changed');
 const child=()=>{
  const path=relative(join(root,'.volumes/agents'),cwd);
  assert.ok(path&&path!=='..'&&!path.startsWith('..'+sep)&&!isAbsolute(path),'native child did not run in its separate view');
  assert.ok(process.env.ACYCLIC_TASK,'native child has no original task identity');
 };
 const proof=()=>text('native-child.json').then(JSON.parse);
 step='execute native '+phase+' filesystem effect';
 if(phase==='parent') {
  parent();await absent('child.txt');await absent('resumed.txt');
  await writeFile('parent.txt','PARENT');
 } else if(phase==='child') {
  child();assert.equal(await text('parent.txt'),'PARENT','fork lost parent bytes');
  await writeFile('child.txt','CHILD');
  await writeFile('native-child.json',JSON.stringify({task:process.env.ACYCLIC_TASK,cwd}));
  await writeFile('.volumes/scratch/native-retained.txt','RETAINED');
 } else if(phase==='resumed') {
  child();assert.equal((await proof()).task,process.env.ACYCLIC_TASK,'resume changed child identity');
  assert.equal(await text('.volumes/scratch/native-retained.txt'),'RETAINED','resume lost private child state');
  assert.equal(await text('child.txt'),'CHILD');
  await writeFile('resumed.txt','RESUMED');
 } else {
  parent();
  if(phase==='merge')await absent('child.txt');
  await absent('resumed.txt');
  const binary=join(dirname(root),'package/bin',process.platform==='win32'?'acyclic.exe':'acyclic');
  const args=['fs','merge','agents/native-child'];
  try {execFileSync(binary,args,{stdio:['ignore','inherit','inherit']});}
  catch(cause) {throw new Error('native merge failed; recovery argv: '+JSON.stringify([binary,...args])+'; '+cause.message,{cause});}
  assert.equal(await text('parent.txt'),'PARENT');
  assert.equal(await text('child.txt'),'CHILD');
  const original=await proof();
  assert.notEqual(original.task,process.env.ACYCLIC_TASK,'parent substituted its own task for the child');
  assert.notEqual(original.cwd,cwd,'parent substituted its own view for the child');
  // Public wait has settled the native turn. The existing owner query reads
  // its original native identifier; no second event reader is introduced.
  const initial=join(root,'.volumes/scratch/native-initial-session.json');
  const current=phase==='merge'?initial:join(root,'.volumes/scratch/native-resumed-session.json');
  const descriptor=openSync(current,'w');
  try {
   execFileSync(binary,['__workspace'],{input:JSON.stringify({kind:'hook',host,event:'NativeSession',
    input:{task:original.task}}),stdio:['pipe',descriptor,'inherit']});
  } finally {closeSync(descriptor);}
  const session=JSON.parse(await text(current));
  nativeSessionIdentity(session);
  if(phase==='finish')assertNativeResume(JSON.parse(await text(initial)),session);
  if(phase==='finish')assert.equal(await text('resumed.txt'),'RESUMED');
  else await absent('resumed.txt');
 }
 console.log('NATIVE_EFFECT_'+phase.toUpperCase());
} catch(cause) {
 console.error('step='+step+': '+cause.message+'; recovery argv: '+JSON.stringify([process.execPath,...process.argv.slice(1)]));
 process.exitCode=1;
}
