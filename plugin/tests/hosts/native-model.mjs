// Installed real native hosts against the shipping mock or an explicit native
// login profile. The acceptance caller owns the process scope, provider,
// deadline and request oracle; this module only drives one installed host.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile,realpath,stat,writeFile} from 'node:fs/promises';
import {dirname,delimiter,isAbsolute,join,relative,sep,toNamespacedPath} from 'node:path';
import {fileURLToPath} from 'node:url';
import {stagePlugin} from './package.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
import {nativeHost,nativeProvider} from './native-host.mjs';

// Host process success also requires its own successful model-turn terminal.
export function assertNativeCompletion(host,result,answer,selection) {
 assert.equal(result.code,0,result.stderr);
 let terminal,text,completed=false,initialized=false;
 for(const line of result.stdout.split('\n')) {
  if(!line)continue;
  const event=JSON.parse(line);
  if(host==='codex') {
   if(event.type==='turn.started') {text=undefined;completed=false;}
   if(event.type==='turn.failed'||event.type==='error') {
    const error=new Error('native Codex turn failed',{cause:event});error.code='NATIVE_HOST_MODEL_FAILED';throw error;
   }
   if(event.type==='item.completed'&&event.item.type==='agent_message')text=event.item.text;
   if(event.type==='turn.completed')completed=true;
  } else if(host==='claude-code') {
   if(selection&&event.type==='system'&&event.subtype==='init') {
    assert.equal(initialized,false,'duplicate native Claude initialization');initialized=true;
    assert.equal(event.model,selection.model,'native Claude model differs from approved selection');
    const count=selection.installed?1:0;
    assert.ok(Array.isArray(event.plugins),'native Claude plugin set differs from paired mode');
    assert.ok(Array.isArray(event.mcp_servers),'native Claude MCP connections differ from paired mode');
    assert.equal(event.plugins.length,count,'native Claude plugin set differs from paired mode');
    assert.equal(event.mcp_servers.length,count,'native Claude MCP connections differ from paired mode');
    if(selection.installed) {
     assert.equal(event.plugins[0].name,'acyclic','native Claude plugin set differs from paired mode');
     assert.equal(event.mcp_servers[0].name,'plugin:acyclic:acyclic-sessions','native Claude MCP connections differ from paired mode');
     assert.equal(event.mcp_servers[0].status,'connected','native Claude MCP connections differ from paired mode');
    }
   }
   if(event.type==='result')terminal=event;
  } else if(host==='pi') {
   const message=event.type==='message_end'?event.message:undefined;
   if(message?.role!=='assistant')continue;
   terminal=message;
   if(message.stopReason==='error') {
    const error=new Error(message.errorMessage,{cause:message});
    error.code='NATIVE_HOST_MODEL_FAILED';
    throw error;
   }
  } else {
   throw new Error('step=validate native terminal; recovery=select codex, claude-code or pi');
  }
 }
 if(host==='codex')assert.equal(completed,true,'native Codex terminal missing');
 else if(host==='claude-code') {
  if(selection)assert.equal(initialized,true,'native Claude initialization missing');
  assert.ok(terminal,'native Claude terminal missing');
  assert.equal(terminal.subtype,'success','native Claude turn failed');
  assert.equal(terminal.is_error,false,'native Claude turn failed');
  text=terminal.result;
 } else {
  assert.equal(terminal?.stopReason,'stop','native Pi terminal missing');
  text=terminal.content.filter(part=>part.type==='text').map(part=>part.text).join('');
 }
 assert.ok(typeof text==='string'&&text.includes(answer),'native answer missing');
}


// Native policy arguments the root runs under and every managed child inherits
// (ACYCLIC_HOST_ARGS), plus each host's own keyless provider selection.
// Sandbox and approval policy stay unchanged. Codex asks the plugin's
// permission hook, which the owner's approval policy answers; Claude Code
// approves the same coordination tools by its native allow list.
export const nativeModel=(host,live)=>live?.model??(host==='claude-code'?'claude-haiku-5-5':'tiny');

export async function launchNativeHost(host,{url,root,packageRoot,workspace,env,tools,nativePermissions,prompt,live,extension,installed=true,input=false}) {
 const model=nativeModel(host,live);
 if(host==='codex') {
  const provider=live?['--config','model_provider="openai"','--model',model]
   :['--config','model_provider="mock"','--config','model_providers.mock.name="Local deterministic fixture"',
   '--config',`model_providers.mock.base_url="${url}"`,'--config','model_providers.mock.wire_api="responses"',
   '--config','model_providers.mock.requires_openai_auth=false','--model',model];
  const policy=[...provider,
   '--ask-for-approval','never','--sandbox','workspace-write','--add-dir',workspace];
  return {policy,args:[...policy,'exec','--json',input?'-':prompt]};
 }
 if(host==='claude-code') {
  // Foundry's Messages endpoint with its documented gateway mode: no key,
  // token or account header reaches the localhost mock.
  if(!live)Object.assign(env,{CLAUDE_CODE_USE_FOUNDRY:'1',CLAUDE_CODE_SKIP_FOUNDRY_AUTH:'1',
   ANTHROPIC_FOUNDRY_BASE_URL:url.replace(/\/v1$/,''),CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'});
  const settings=join(root,'claude-policy.json');
  await writeFile(settings,JSON.stringify({permissions:{allow:[...tools.map(name=>'mcp__plugin_acyclic_acyclic-sessions__'+name),...nativePermissions]}}));
  const policy=['--model',model,'--settings',settings,'--permission-mode','default',
   ...(installed?['--plugin-dir',join(packageRoot,'claude')]:[]),'--add-dir',workspace];
  return {policy,args:['-p',...(input?[]:[prompt]),'--output-format','stream-json','--verbose',...policy]};
 }
 if(!live)env.ACYCLIC_E2E_MODEL_URL=url;
 const policy=live?['--no-extensions',...(installed?['--extension',extension]:[]),
  '--provider',live.provider,'--model',model,'--session-dir',join(root,'native-sessions')]
  :['--extension',fileURLToPath(new URL('./keyless-pi-provider.mjs',import.meta.url)),'--provider','fixture','--model',model];
 return {policy,args:['--mode','json','--print','--no-skills','--no-prompt-templates','--no-themes',...policy,...(input?[]:[prompt])]};
}

// A campaign keeps one immutable package source across its pair/workflow
// scopes. Admission still precedes the first stage, and failed staging stays
// failed; later scopes neither copy the package nor change its registration.
export function nativePackage(binary,packageRoot) {
 let staged;
 return ()=>staged??=stagePlugin(fileURLToPath(new URL('../../',import.meta.url)),binary,packageRoot)
  .then(executable=>({packageRoot,executable}));
}

// One installed host session under the caller's process scope: stage the
// production package, install through the shipping CLI or the host's native
// plugin directory, open the root through the workspace API, then run the
// actual host once against the caller's admitted provider.
export async function installedHost({host,processes,fixture,capture,tools,nativePermissions=[],prompt,answer,execute,packageStage}) {
 const native=await nativeHost(host,fixture.env);
 nativeProvider(native,fixture.live);
 const binary=fixture.env.E2E_PLUGIN_BINARY;
 assert.ok(binary&&isAbsolute(binary),'step=admit E2E_PLUGIN_BINARY; recovery=set E2E_PLUGIN_BINARY to its matching prebuilt absolute executable');
 const {workspace}=fixture,root=dirname(workspace),env={...fixture.env};
 if(fixture.live) {
  // The caller provisions an exclusive authenticated profile outside guest
  // files. Pass its path to the original host; never read or copy credentials.
  try {
   const profile=await realpath(fixture.live.profile),within=relative(await realpath(root),profile);
   assert.ok(within==='..'||within.startsWith('..'+sep)||isAbsolute(within),'native login profile must be outside the fixture root');
   assert.ok((await stat(profile)).isDirectory(),'native login profile must be an existing directory');
   env[{codex:'CODEX_HOME','claude-code':'CLAUDE_CONFIG_DIR',pi:'PI_CODING_AGENT_DIR'}[host]]=profile;
  } catch(cause) {
   throw Object.assign(new Error('step=admit live native login profile: '+cause.message+
    '; recovery=provision an existing exclusive native login directory outside the fixture root',{cause}),
   {code:'NATIVE_LOGIN_PROFILE_REFUSED'});
  }
 } else {
  let endpoint;
  try {endpoint=new URL(fixture.url);}
  catch(cause) {throw new Error('step=admit mock endpoint; recovery=run through scripts/graphcoder-e2e/run.mjs',{cause});}
  assert.ok(endpoint.protocol==='http:'&&endpoint.hostname==='127.0.0.1',
   'step=admit mock endpoint; recovery=supply the original loopback fixture.url');
 }
 env.PATH=[dirname(native.entry),native.path,env.PATH].join(delimiter);
 let step='stage native package';
 const run=async(program,args,input)=>{
  // The long-lived workspace owner holds the one mount lease. Commands retain
  // process-tree custody without attempting another lease over its live views.
  // Owned external commands receive empty stdin; the one workspace-API request
  // with a body runs inside the same scope through its ordinary process path.
  const record={program,args,...await (input===undefined?processes.runOwned(program,args,{cwd:workspace,env})
   :processes.run(program,args,{cwd:workspace,env},input))};
  capture(record);return record;
 };
 try {
  const {packageRoot,executable}=await (packageStage??nativePackage(binary,join(root,'package')))();
  env.E2E_PLUGIN_BINARY=executable;
  step='install through shipping plugin';
  if(host==='codex') {
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot]);
   assert.equal(installation.code,0,installation.stderr);
    const listed=await run(native.program,[...native.args,'plugin','list','--json']);assert.equal(listed.code,0,listed.stderr);
   const installed=JSON.parse(listed.stdout).installed;
   assert.ok(installed.some(row=>row.pluginId==='acyclic@acyclic'&&row.enabled),'native plugin manager lacks acyclic@acyclic');
   if(fixture.live)assert.deepEqual(installed.filter(row=>row.enabled).map(row=>row.pluginId),
    ['acyclic@acyclic'],'step=admit native plugins; recovery=use an exclusive native login profile with no unrelated enabled plugins');
  } else if(host==='pi') {
   const extension=fixture.live?join(root,'acyclic.ts'):join(env.PI_CODING_AGENT_DIR,'extensions/acyclic.ts');
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot,'--settings',extension]);
   assert.equal(installation.code,0,installation.stderr);
   assert.ok((await readFile(extension,'utf8')).includes(JSON.stringify(toNamespacedPath(await realpath(executable)))),'native extension does not bind admitted shipping binary');
  }
  step='start shipping workspace owner';
  // The harness approval chain the native permission hook asks: the case's
  // coordination tools only; exhausting it refuses every other one.
  const approvals=join(root,'approval-policy.json');
  await writeFile(approvals,JSON.stringify({approvers:[{kind:'rules',principal:'acceptance',
   rules:tools.map(tool=>({tool,verdict:'allow'})),fallback:'ask'}]}));
  await startWorkspaceOwner({scope:processes,binary:fixture.binary,workspace,state:join(root,'state'),env,root,
   args:['--approval-policy',approvals],capture:record=>capture({role:'owner',...record})});
  step='initialize workspace Git';
  const initialized=await run(env.E2E_GIT_BINARY??'git',['init','--quiet']);
  assert.equal(initialized.code,0,initialized.stderr);
  step='open native root through the workspace API';
  const opened=await run(executable,['__workspace'],JSON.stringify({kind:'open',host,session:randomUUID()}));
  assert.equal(opened.code,0,opened.stderr);
  const configuration={url:fixture.url,live:fixture.live,root,packageRoot,workspace,env,tools,nativePermissions,prompt,
   extension:fixture.live&&host==='pi'?join(root,'acyclic.ts'):undefined};
  const {policy,args}=await launchNativeHost(host,configuration);
  Object.assign(env,{ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:JSON.parse(opened.stdout).task,ACYCLIC_HOST_ARGS:JSON.stringify(policy)});
  step='complete installed native session';
  if(execute)return await execute({native,configuration,run});
  const completed=await run(native.program,[...native.args,...args]);
  assertNativeCompletion(host,completed,answer,fixture.live&&host==='claude-code'
   ?{model:fixture.live.model,installed:true}:undefined);
 } catch(error) {
  throw new Error(`step=${host} ${step}: ${error.message}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`,{cause:error});
 }
}
