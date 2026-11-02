import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {mkdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {test} from 'node:test';
import {createModels} from '@earendil-works/pi-ai/models';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';
import {nativeHost} from './native-host.mjs';
import {assertNativeCompletion,launchNativeHost} from './native-model.mjs';
import register from './pi-provider.mjs';

// A real HTTP refusal exercises the pinned native serializers and credential
// interfaces. It supplies no provider response fixture and makes no model call.
test('pinned Pi protocols authenticate exact models and preserve native HTTP refusal',async t=>{
 const capability=randomBytes(32).toString('hex'),requests=[];
 const server=createServer(async(request,response)=>{
  const chunks=[];for await(const chunk of request)chunks.push(chunk);
  requests.push({path:request.url,headers:request.headers,body:Buffer.concat(chunks).toString('utf8')});
  response.writeHead(400).end();
 });
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const proxy=`http://127.0.0.1:${server.address().port}/${capability}`;
 const previous=process.env.ACYCLIC_NATIVE_PROXY;
 const root=hostFixtureDirectory('native-proxy-'),scope=new Processes();
 try {
  const cells=[['openai','gpt-6-luna','/openai/responses','authorization','Bearer '+capability],
   ['anthropic','claude-haiku-5-5','/anthropic/v1/messages','x-api-key',capability],
   ['gemini','gemini-3.8-flash','/gemini/models/gemini-3.8-flash:streamGenerateContent?alt=sse','x-goog-api-key',capability]];
  for(const [provider,model,path,header,credential] of cells) {
   const live={provider,model,proxy};process.env.ACYCLIC_NATIVE_PROXY=JSON.stringify(live);
   let selected;
   await register({registerProvider(value){assert.equal(selected,undefined);selected=value;}});
   assert.equal(process.env.ACYCLIC_NATIVE_PROXY,undefined,'the native shell must not inherit provider bootstrap credentials');
   const models=createModels();models.setProvider(selected);
   const admitted=models.getModel(provider,model);
   assert.ok(admitted,'the model must come from the pinned native catalogue');
   const count=requests.length;
   const result=await models.completeSimple(admitted,{messages:[{role:'user',content:'native admission',timestamp:0}]},{maxRetries:0});
   assert.equal(result.stopReason,'error','HTTP refusal must remain a native failure');
   assert.equal(requests.length,count+1,'one original request, without replay');
   const captured=requests.at(-1);
   assert.equal(captured.path,path);assert.equal(captured.headers[header],credential);
   const body=JSON.parse(captured.body);
   if(provider!=='gemini')assert.equal(body.model,model);
   assert.ok(!captured.body.includes(capability),'capability must stay out of model input');
   t.diagnostic(JSON.stringify({provider,model,path,native_stop:result.stopReason,requests:1}));
  }
  // The actual extension loader must await the same async provider factory.
  const live={provider:'openai',model:'gpt-6-luna',proxy};
  const workspace=join(root,'workspace');await mkdir(workspace);
  const env=environment(join(root,'profile'),{}),native=await nativeHost('pi',env);
  const launch=await launchNativeHost('pi',{root,workspace,env,live,prompt:'native admission',installed:false});
  const count=requests.length;
  const completed=await scope.runOwned(native.program,[...native.args,...launch.args],{cwd:workspace,env});
  assert.equal(requests.length,count+1,'the actual CLI must use the selected native provider once');
  assert.equal(requests.at(-1).headers.authorization,'Bearer '+capability);
  assert.throws(()=>assertNativeCompletion('pi',completed,'NATIVE_PLUGIN_MODEL_DONE'),
   error=>error.code==='NATIVE_HOST_MODEL_FAILED');
  const codexEnv=environment(join(root,'codex-profile'));
  const codex=await nativeHost('codex',codexEnv);
  const initialized=await scope.runOwned(codexEnv.E2E_GIT_BINARY??'git',['init','--quiet'],{cwd:workspace,env:codexEnv});
  assert.equal(initialized.code,0,initialized.stderr);
  const codexLaunch=await launchNativeHost('codex',{root,workspace,env:codexEnv,live,prompt:'native admission',installed:false});
  const codexCount=requests.length;
  const refused=await scope.runOwned(codex.program,[...codex.args,...codexLaunch.args],{cwd:workspace,env:codexEnv});
  assert.equal(requests.length,codexCount+1,'the actual Codex CLI must send one admitted request');
  assert.equal(requests.at(-1).path,'/openai/responses');
  assert.equal(requests.at(-1).headers.authorization,'Bearer '+capability);
  assert.equal(JSON.parse(requests.at(-1).body).model,live.model);
  assert.throws(()=>assertNativeCompletion('codex',refused,'NATIVE_PLUGIN_MODEL_DONE'));
 } finally {
  try {await scope.close();}
  finally {
   await rm(root,{recursive:true,force:true});
   if(previous===undefined)delete process.env.ACYCLIC_NATIVE_PROXY;else process.env.ACYCLIC_NATIVE_PROXY=previous;
   await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
  }
 }
});
