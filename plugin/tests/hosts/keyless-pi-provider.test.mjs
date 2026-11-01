import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {mkdir,readdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {createModels} from '@earendil-works/pi-ai/models';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';
import {nativeHost} from './native-host.mjs';
import {assertNativeCompletion,installedHost,launchNativeHost} from './native-model.mjs';
import register from './keyless-pi-provider.mjs';

// Exercise the actual lockfile-pinned native provider/auth/Responses path.
// This is an unsupported-admission control, never an installed-host pass.
test('pinned Pi retains native keyless refusal without a credential or HTTP request',async t=>{
 const server=createServer((_request,response)=>{requests++;response.writeHead(500).end();});
 let requests=0;
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const previous=process.env.ACYCLIC_E2E_MODEL_URL;
 process.env.ACYCLIC_E2E_MODEL_URL=`http://127.0.0.1:${server.address().port}/fixture/v1`;
 const root=hostFixtureDirectory('native-keyless-'),scope=new Processes();
 try {
  let provider;
  register({registerProvider(value){assert.equal(provider,undefined);provider=value;}});
  assert.deepEqual((await provider.auth.apiKey.resolve({})).auth,{});
  const models=createModels();models.setProvider(provider);
  const model=models.getModel('fixture','tiny');
  const auth=await models.getAuth(model);
  assert.equal(auth.auth.apiKey,undefined);
  assert.equal(auth.auth.headers?.Authorization,undefined);
  assert.throws(()=>provider.streamSimple(model,{messages:[]},auth.auth),error=>
   error.code==='UNSUPPORTED_NATIVE_KEYLESS_PROVIDER'&&error.cause.message==='No API key for provider: fixture');
  const result=await models.completeSimple(model,{messages:[{role:'user',content:'keyless admission',timestamp:0}]});
  assert.equal(result.stopReason,'error');
  assert.match(result.errorMessage,/step=admit Pi keyless Responses provider: No API key for provider: fixture/);
  const workspace=join(root,'workspace');await mkdir(workspace);
  const env=environment(join(root,'profile')),native=await nativeHost('pi',env);
  const before=await readdir(root),effects=[];
  await assert.rejects(installedHost({host:'pi',processes:scope,
   fixture:{env:{...env,E2E_PLUGIN_BINARY:native.entry},workspace,url:process.env.ACYCLIC_E2E_MODEL_URL},
   tools:[],capture:record=>effects.push(record),execute:()=>effects.push('execute')}),error=>
   error.code==='UNSUPPORTED_NATIVE_KEYLESS_PROVIDER'&&error.message.includes(`${native.package.name}@${native.package.version}`));
  assert.deepEqual(await readdir(root),before,'unsupported provider must not stage a package, state, approval file or Git root');
  assert.deepEqual(effects,[],'unsupported provider must not install, start an owner or execute a native session');
  const launch=await launchNativeHost('pi',{url:process.env.ACYCLIC_E2E_MODEL_URL,env,prompt:'keyless admission'});
  const completed=await scope.runOwned(native.program,[...native.args,...launch.args],{cwd:root,env});
  t.diagnostic(JSON.stringify({native_entry:native.entry,...completed}));
  assert.equal(completed.code,0,'retain the pinned CLI zero-exit convention');
  assert.equal(completed.signal,null);
  assert.throws(()=>assertNativeCompletion('pi',completed,'NATIVE_PLUGIN_MODEL_DONE'),error=>
   error.code==='NATIVE_HOST_MODEL_FAILED'&&error.cause.stopReason==='error'
    &&error.message.startsWith('step=admit Pi keyless Responses provider: No API key for provider: fixture'));
  assert.equal(requests,0,'no credential/fetch shim may silently admit a different native request');
 } finally {
  try {await scope.close();}
  finally {
   await rm(root,{recursive:true,force:true});
   if(previous===undefined)delete process.env.ACYCLIC_E2E_MODEL_URL;
   else process.env.ACYCLIC_E2E_MODEL_URL=previous;
   await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
  }
 }
});
