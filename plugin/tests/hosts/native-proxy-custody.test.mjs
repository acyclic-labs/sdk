import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {once} from 'node:events';
import {mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import {join} from 'node:path';
import {test} from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';
import {nativeHost} from './native-host.mjs';
import {launchNativeHost} from './native-model.mjs';

// The actual native request receives a local HTTP refusal, never provider data.
// Its startup command records credential presence only, never the credential.
test('native Claude authenticates its proxy while withholding the credential from child commands',
 {skip:process.platform!=='win32'},async t=>{
 const root=hostFixtureDirectory('native-proxy-custody-'),workspace=join(root,'workspace');
 const scope=new Processes(),report=join(root,'observed.json'),observer=join(root,'observe.mjs');
 const requests=[],capability=randomBytes(32).toString('hex');
 const server=createServer(async(request,response)=>{
  const chunks=[];for await(const chunk of request)chunks.push(chunk);
  requests.push({method:request.method,path:request.url,credential:request.headers['x-api-key'],body:Buffer.concat(chunks).toString('utf8')});
  response.writeHead(400).end();
 });
 try {
  server.listen(0,'127.0.0.1');await once(server,'listening');
  await mkdir(workspace);
  await writeFile(observer,`import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(report)},JSON.stringify({api_key_present:Object.hasOwn(process.env,'ANTHROPIC_API_KEY')}));process.stdin.resume();\n`);
  const env=environment(join(root,'profile'));
  const native=await nativeHost('claude-code',env);
  const live={provider:'anthropic',model:'claude-haiku-5-5',
   proxy:`http://127.0.0.1:${server.address().port}/${capability}`};
  const launch=await launchNativeHost('claude-code',{root,workspace,env,live,
   tools:[],nativePermissions:[],installed:false,prompt:'native credential custody'});
  const settings=join(root,'claude-policy.json'),policy=JSON.parse(await readFile(settings,'utf8'));
  policy.hooks={SessionStart:[{hooks:[{type:'command',command:'"'+process.execPath+'" "'+observer+'"'}]}]};
  await writeFile(settings,JSON.stringify(policy));
  const result=await scope.runOwned(native.program,[...native.args,...launch.args],{cwd:workspace,env});
  t.diagnostic(JSON.stringify({requests:requests.map(request=>({method:request.method,path:request.path,body_bytes:Buffer.byteLength(request.body)}))}));
  assert.equal(requests.length,2,'the native CLI sends its preconnect and one admitted model request');
  const preconnect=requests.find(request=>request.method==='HEAD'),request=requests.find(request=>request.method==='POST');
  assert.equal(preconnect?.path,'/anthropic/api/hello');
  assert.ok(preconnect.credential===undefined&&!preconnect.body,'native preconnect must not carry a credential or model input');
  assert.equal(request?.path,'/anthropic/v1/messages?beta=true');
  assert.ok(request.credential===capability,'the native API must authenticate with the issued capability');
  assert.equal(JSON.parse(request.body).model,live.model);
  assert.ok(!request.body.includes(capability),'the capability must stay out of model input');
  assert.notEqual(result.code,0,'the native HTTP refusal must not become success');
  assert.equal(JSON.parse(await readFile(report,'utf8')).api_key_present,false,
   'native child commands must not inherit the proxy credential');
 } finally {
  try {await scope.close();}finally {
   await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
   await rm(root,{recursive:true,force:true});
  }
 }
});
