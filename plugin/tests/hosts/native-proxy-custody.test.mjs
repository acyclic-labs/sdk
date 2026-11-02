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
 const scope=new Processes(),report=join(root,'observed.json'),bridgeReport=join(root,'bridge.json'),observer=join(root,'observe.mjs');
 const requests=[],capability=randomBytes(32).toString('hex');
 const server=createServer(async(request,response)=>{
  const chunks=[];for await(const chunk of request)chunks.push(chunk);
  requests.push({method:request.method,path:request.url,credential:request.headers['x-api-key'],body:Buffer.concat(chunks).toString('utf8')});
  response.writeHead(400).end();
 });
 try {
  server.listen(0,'127.0.0.1');await once(server,'listening');
  await mkdir(workspace);
  const env=environment(join(root,'profile'));
  const native=await nativeHost('claude-code',env);
  const live={provider:'anthropic',model:'claude-haiku-5-5',
   proxy:`http://127.0.0.1:${server.address().port}/${capability}`};
  const launch=await launchNativeHost('claude-code',{root,workspace,env,live,
   tools:[],nativePermissions:[],installed:false,prompt:'native credential custody'});
  await writeFile(observer,`import fs from 'node:fs';
const observed={api_key_present:Object.hasOwn(process.env,'ANTHROPIC_API_KEY'),base_url:process.env.ANTHROPIC_BASE_URL};
const save=()=>fs.writeFileSync(process.argv[2]??${JSON.stringify(report)},JSON.stringify(observed));
save();if(!process.argv[2])process.stdin.resume();\n`);
  const settings=join(root,'claude-policy.json'),policy=JSON.parse(await readFile(settings,'utf8'));
  policy.hooks={SessionStart:[{hooks:[{type:'command',command:'"'+process.execPath+'" "'+observer+'"'}]}]};
  await writeFile(settings,JSON.stringify(policy));
  // A real native MCP child exits after observation, without inventing MCP
  // replies. The host must distinguish its trusted bridge from guest commands.
  const coordinator=JSON.parse(await readFile(new URL('../../claude/.claude-plugin/plugin.json',import.meta.url),'utf8')).mcpServers['acyclic-sessions'];
  const mcp=JSON.stringify({mcpServers:{custody:{...coordinator,command:process.execPath,args:[observer,bridgeReport]}}});
  const result=await scope.runOwned(native.program,[...native.args,...launch.args,'--mcp-config',mcp],{cwd:workspace,env});
  t.diagnostic(JSON.stringify({requests:requests.map(request=>({method:request.method,path:request.path,body_bytes:Buffer.byteLength(request.body)}))}));
  const bridge=JSON.parse(await readFile(bridgeReport,'utf8'));
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
  assert.equal(bridge.api_key_present,true,
   'the trusted native MCP bridge must retain provider authentication for managed children');
  assert.equal(bridge.base_url,new URL(live.proxy).origin+'/anthropic');
 } finally {
  try {await scope.close();}finally {
   await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
   await rm(root,{recursive:true,force:true});
  }
 }
});

test('native Codex forwards provider custody only to its declared coordination bridge',async t=>{
 const root=hostFixtureDirectory('native-codex-custody-'),workspace=join(root,'workspace');
 const scope=new Processes(),report=join(root,'bridge.json'),observer=join(root,'observe.mjs');
 const capability=randomBytes(32).toString('hex');
 try {
  await mkdir(workspace);
  await writeFile(observer,`import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(report)},JSON.stringify({token_present:Object.hasOwn(process.env,'ACYCLIC_NATIVE_PROXY_TOKEN')}));\n`);
  const env=environment(join(root,'profile')),native=await nativeHost('codex',env);
  const initialized=await scope.runOwned(env.E2E_GIT_BINARY??'git',['init','--quiet'],{cwd:workspace,env});
  assert.equal(initialized.code,0,initialized.stderr);
  const launch=await launchNativeHost('codex',{root,workspace,env,installed:false,
   live:{provider:'openai',model:'gpt-6-luna',proxy:'http://127.0.0.1:1/'+capability},prompt:'native custody'});
  const coordinator=JSON.parse(await readFile(new URL('../../.mcp.json',import.meta.url),'utf8')).mcpServers['acyclic-sessions'];
  // Use the packaged forwarding declaration in the native MCP launcher. The
  // observer exits without a reply, so required-server admission must fail
  // before model work. It does not invent a successful MCP exchange.
  const config={command:process.execPath,args:[observer],env_vars:coordinator.env_vars,required:coordinator.required};
  const flags=Object.entries(config).flatMap(([key,value])=>['--config','mcp_servers.custody.'+key+'='+JSON.stringify(value)]);
  const result=await scope.runOwned(native.program,[...native.args,...flags,...launch.args],{cwd:workspace,env});
  t.diagnostic(result.stderr.replaceAll(capability,'<capability>'));
  assert.notEqual(result.code,0,'required bridge exit must refuse before model work');
  assert.match(result.stderr,/required MCP servers failed to initialize/,
   'the native required-server refusal must precede model execution');
  assert.equal(JSON.parse(await readFile(report,'utf8')).token_present,true,
   'the trusted native MCP bridge must retain provider authentication for managed children');
 } finally {
  try {await scope.close();}finally {await rm(root,{recursive:true,force:true});}
 }
});
