import assert from 'node:assert/strict';
import {test} from 'node:test';
import {rmSync} from 'node:fs';
import {join,relative,isAbsolute} from 'node:path';
import {connect} from 'node:net';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {fixtureEnvironment,checked,start,barrier,ready,socketLine} from './native-fixture.mjs';

test('shared native fixture retains its owner and isolates actual child temporary paths',async()=>{
 const root=scratchDirectory('plugins-fixture-environment-');
 const original=process.env.PROFILE_BRIDGE_SENTINEL;
 process.env.PROFILE_BRIDGE_SENTINEL='must-never-be-returned';
 try{
  const variables={PATH:process.env.PATH},env=fixtureEnvironment(root,variables);
  assert.deepEqual(variables,{PATH:process.env.PATH});
  assert.throws(()=>fixtureEnvironment(root,{HOME:join(root,'escape')}),/step=admit native fixture variables/);
  const source="const os=require('node:os');console.log(JSON.stringify({paths:[os.tmpdir(),process.env.XDG_RUNTIME_DIR],owner:process.env.E2E_PROCESS_HOST,unrelated:process.env.PROFILE_BRIDGE_SENTINEL}))";
  const capture=async env=>JSON.parse(await checked(process.execPath,['-e',source],{env}));
  const inside=path=>{if(!path)return false;const part=relative(root,path);return !isAbsolute(part)&&part!=='..'&&!part.startsWith('..'+(process.platform==='win32'?'\\':'/'));};
  const actual=await capture(env);
  assert.equal(actual.owner,process.env.E2E_PROCESS_HOST);assert.equal(actual.unrelated,undefined);
  assert.ok(actual.paths.every(inside),JSON.stringify(actual));
  const previous={...env};for(const key of ['TMPDIR','TMP','TEMP','XDG_RUNTIME_DIR'])delete previous[key];
  assert.ok(!(await capture(previous)).paths.every(inside),'previous shared temporary paths must fail the same oracle');
 }finally{
  if(original===undefined)delete process.env.PROFILE_BRIDGE_SENTINEL;else process.env.PROFILE_BRIDGE_SENTINEL=original;
  rmSync(root,{recursive:true,force:true});
 }
});

test('native cancellation descendant survives parent input closure',async()=>{
 const root=scratchDirectory('plugins-fixture-descendant-');
 const env=fixtureEnvironment(root,{FIXTURE_SUPPORT:new URL('./native-fixture.mjs',import.meta.url).href});
 const child=start(process.execPath,[fileURLToPath(new URL('./native-session-child.mjs',import.meta.url)),'--descendant'],{cwd:root,env});
 const events=new Spool(child.child.stdout,child.closed,{key:event=>event.port?['ready']:[],unique:true});
 let socket;
 try{
  const {port}=await events.keyed('ready');
  child.child.stdin.end();
  socket=connect({host:'127.0.0.1',port});await once(socket,'connect');
  assert.equal(await socketLine(socket),'alive');
 }finally{socket?.destroy();await child.stop();rmSync(root,{recursive:true,force:true});}
});

test('native readiness follows the original connection or child settlement',async()=>{
 const root=scratchDirectory('plugins-fixture-readiness-'),gate=await barrier();
 const env=fixtureEnvironment(root,{FIXTURE_BARRIER:String(gate.port)});
 try{
  for(const mode of ['ready','exit-before-connect','close-without-identity']){
   const abort=new AbortController(),connection=gate.next(abort.signal);connection.catch(()=>{});
   const source=mode==='exit-before-connect'?'process.exitCode=23;':mode==='ready'
    ?`const {hold}=await import(${JSON.stringify(new URL('./native-fixture.mjs',import.meta.url).href)});await hold('original-child',true);`
    :"const {connect}=await import('node:net');const socket=connect({host:'127.0.0.1',port:Number(process.env.FIXTURE_BARRIER)});socket.on('connect',()=>socket.end());";
   const child=start(process.execPath,['--input-type=module','-e',source],{env});child.collect();
   let socket;
   try{
    const identity=(async()=>{socket=await ready(connection,child.closed,abort,'node --test plugin/tests/hosts/native-fixture.test.mjs');return socketLine(socket);})();
    if(mode==='ready'){assert.equal(await identity,'original-child');socket.write('.');}
    else await assert.rejects(identity,/child settled before readiness|producer closed/);
    assert.equal((await child.closed).code,mode==='exit-before-connect'?23:0);
   }finally{abort.abort();socket?.destroy();await child.stop();}
  }
 }finally{await gate.close();rmSync(root,{recursive:true,force:true});}
});
