import assert from 'node:assert/strict';
import {randomBytes,timingSafeEqual} from 'node:crypto';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {rm} from 'node:fs/promises';
import {join} from 'node:path';
import {test} from 'node:test';
import {createModels} from '@earendil-works/pi-ai/models';
import {createEventBus} from '@earendil-works/pi-coding-agent';
import {nativeHost,nativeProvider} from './native-host.mjs';
import register from './pi-provider.mjs';
import {fuzz} from './native-fixture.fuzz.mjs';
import {assertNativeCompletion,launchNativeHost} from './native-model.mjs';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';

// The native SDK produces the request; the real local server checks its issued
// credential and returns an empty HTTP refusal, never authored provider wire.
test('pinned Pi uses the deterministic model capability without exposing it to commands',async t=>{
 const credential=randomBytes(32).toString('hex');
 const requests=[];let requestFailure;
 const server=createServer(async(request,response)=>{
  try{
   const supplied=Buffer.from(request.headers.authorization??''),expected=Buffer.from('Bearer '+credential);
   if(supplied.length!==expected.length||!timingSafeEqual(supplied,expected)){
    response.writeHead(401).end();return;
   }
   const chunks=[];for await(const chunk of request)chunks.push(chunk);
   requests.push({url:request.url,bytes:Buffer.concat(chunks).toString('utf8')});
   response.writeHead(400).end();
  }catch(error){
   requestFailure=error;response.destroy(error);
  }
 });
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const url=`http://127.0.0.1:${server.address().port}/native-installed-pi-recursive/v1`;
 const saved={url:process.env.ACYCLIC_E2E_MODEL_URL,credential:process.env.ACYCLIC_E2E_MODEL_CAPABILITY,
  proxy:process.env.ACYCLIC_NATIVE_PROXY};
 const root=hostFixtureDirectory('native-authenticated-'),scope=new Processes();
 try{
  delete process.env.ACYCLIC_NATIVE_PROXY;
  process.env.ACYCLIC_E2E_MODEL_URL=url;
  process.env.ACYCLIC_E2E_MODEL_CAPABILITY=credential;
  const native=await nativeHost('pi',process.env);
  assert.equal(nativeProvider(native,undefined,{url,nativeCapability:credential}),'authenticated-responses');
  for(const changed of [{url:'http://example.com/native-case/v1',credential},
   {url:'http://127.0.0.1:12345/installed-pi-recursive/v1',credential},{url:'malformed',credential},
   {url,credential:'dummy'},{url,credential:{}}]){
   fuzz(Buffer.from(JSON.stringify(changed)));
   assert.throws(()=>nativeProvider(native,undefined,{url:changed.url,
    nativeCapability:changed.credential}),/step=admit/);
  }
  const events=createEventBus();let selected;
  await register({events,registerProvider(provider){assert.equal(selected,undefined);selected=provider;}});
  assert.equal(process.env.ACYCLIC_E2E_MODEL_CAPABILITY,undefined,'guest commands cannot inherit the capability');
  const bridge={...process.env};events.emit('acyclic:coordination-environment',bridge);
  assert.ok(bridge.ACYCLIC_E2E_MODEL_CAPABILITY===credential,'the original bridge retains child authentication');
  assert.equal(process.env.ACYCLIC_E2E_MODEL_CAPABILITY,undefined);
  const models=createModels();models.setProvider(selected);
  const model=models.getModel('fixture','tiny');
  const result=await models.completeSimple(model,{messages:[{role:'user',content:'NATIVE_PLUGIN_MODEL',timestamp:0}]},{maxRetries:0});
  if(requestFailure)throw requestFailure;
  assert.equal(result.stopReason,'error','retain the original native HTTP refusal');
  assert.equal(requests.length,1,'one authenticated native SDK request, without replay');
  assert.equal(requests[0].url,'/native-installed-pi-recursive/v1/responses');
  assert.equal(JSON.parse(requests[0].bytes).model,'tiny');
  assert.ok(!requests[0].bytes.includes(credential),'the credential must not enter model content');
  t.diagnostic(JSON.stringify({native_request:requests[0]}));
  const env=environment(join(root,'profile'),{...process.env,ACYCLIC_E2E_MODEL_CAPABILITY:credential});
  assert.ok(env.ACYCLIC_E2E_MODEL_CAPABILITY===undefined,'general setup commands cannot inherit model credentials');
  const launch=await launchNativeHost('pi',{root,url,env,prompt:'NATIVE_PLUGIN_MODEL'});
  assert.ok(!JSON.stringify(launch).includes(credential),'the credential must not enter native argv/policy');
  const completed=await scope.runOwned(native.program,[...native.args,...launch.args],
   {cwd:root,env:{...env,ACYCLIC_E2E_MODEL_CAPABILITY:credential}});
  if(requestFailure)throw requestFailure;
  assert.equal(completed.code,0,'retain the original native zero-exit convention for a model refusal');
  assert.equal(completed.signal,null);
  assert.throws(()=>assertNativeCompletion('pi',completed,'NATIVE_PLUGIN_MODEL_DONE'),error=>
   error.code==='NATIVE_HOST_MODEL_FAILED'&&error.cause.stopReason==='error'&&!!error.cause.errorMessage);
  assert.equal(requests.length,2,'one request each through the original SDK and CLI, without replay');
  assert.equal(requests[1].url,requests[0].url);
  assert.equal(JSON.parse(requests[1].bytes).model,'tiny');
  assert.ok(!requests[1].bytes.includes(credential),'native CLI content must not contain its credential');
  assert.ok(!completed.stdout.includes(credential)&&!completed.stderr.includes(credential),
   'native session and diagnostic output must not contain its credential');
 }finally{
  for(const [key,value] of [['ACYCLIC_E2E_MODEL_URL',saved.url],['ACYCLIC_E2E_MODEL_CAPABILITY',saved.credential],
   ['ACYCLIC_NATIVE_PROXY',saved.proxy]])if(value===undefined)delete process.env[key];else process.env[key]=value;
  try{await scope.close();}
  finally{
   await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
   await rm(root,{recursive:true,force:true});
  }
 }
});
