// Installed real native hosts against the shipping mock or an explicit native
// login profile. The acceptance caller owns the process scope, provider,
// deadline and request oracle; this module only drives one installed host.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile,realpath,stat,writeFile} from 'node:fs/promises';
import {dirname,delimiter,isAbsolute,join,relative,sep,toNamespacedPath} from 'node:path';
import {fileURLToPath} from 'node:url';
import {toolOutcome} from '../../../scripts/graphcoder-e2e/assertions.mjs';
import {stagePlugin} from './package.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
import {codexSandboxConfig,nativeHost,nativeProvider,nativeProxy} from './native-host.mjs';

const completionRecovery='node --test plugin/tests/hosts/native-completion.test.mjs';
function nativeFailure(host,code,message,cause) {
 throw Object.assign(new Error('step=validate native '+host+' completion: '+message+
  '; recovery='+completionRecovery,{cause}),{code});
}
function nativeObject(host,value,label) {
 if(!value||typeof value!=='object'||Array.isArray(value))
  nativeFailure(host,'NATIVE_HOST_EVENT_INVALID',label+' is not a record',value);
 return value;
}
function nativeParts(host,value,label) {
 if(!Array.isArray(value))nativeFailure(host,'NATIVE_HOST_EVENT_INVALID',label+' is not an array',value);
 return value;
}

// Host process success also requires its own successful model-turn terminal.
export function assertNativeCompletion(host,result,answer,selection) {
 assert.equal(result.code,0,result.stderr);
 let terminal,text,builtinPlugins,session,turn,initialized=false;
 for(const line of result.stdout.split('\n')) {
  if(!line)continue;
  let event;
  try {event=JSON.parse(line);}
  catch(cause) {nativeFailure(host,'NATIVE_HOST_EVENT_INVALID','malformed native JSON event',cause);}
  nativeObject(host,event,'native event');
  if(typeof event.type!=='string'||!event.type)
   nativeFailure(host,'NATIVE_HOST_EVENT_INVALID','native event is not a typed record',event);
  if(host==='codex') {
   if(event.type==='thread.started') {
    if(session!==undefined||typeof event.thread_id!=='string'||!event.thread_id)
     nativeFailure(host,'NATIVE_HOST_SESSION_MISMATCH','native Codex thread identity is missing or repeated',event);
    session=event.thread_id;
   }
   if(event.type==='turn.started') {
    if(turn==='active')nativeFailure(host,'NATIVE_HOST_TERMINAL_ORDER','native Codex turn restarted before completion',event);
    turn='active';text=undefined;
   }
   if(event.type==='turn.failed'||event.type==='error') {
    nativeFailure(host,'NATIVE_HOST_MODEL_FAILED','native Codex turn failed',event);
   }
   if((event.type==='turn.completed'||(event.type.startsWith('item.')&&event.item?.type!=='error'))&&turn!=='active')
    nativeFailure(host,'NATIVE_HOST_TERMINAL_ORDER','native Codex item or terminal outside its active turn',event);
   if(event.type==='item.completed') {
    const item=nativeObject(host,event.item,'native Codex item');
    if(item.type==='command_execution'||item.type==='mcp_tool_call') {
     try {
      toolOutcome(item,undefined,completionRecovery);
      assert.equal(item.status,'completed','native tool terminal is not successful');
      if(item.type==='command_execution')assert.equal(item.exit_code,0,'native shell exit is missing or unsuccessful');
     } catch(cause) {
      nativeFailure(host,'NATIVE_HOST_TOOL_FAILED','native Codex '+item.type+' failed (status='+item.status+
       ', exit_code='+item.exit_code+')',{item,cause});
     }
    }
   }
   if(event.type==='item.completed'&&event.item.type==='agent_message')text=event.item.text;
   if(event.type==='turn.completed') {turn='complete';terminal=event;}
  } else if(host==='claude-code') {
   if(selection&&event.type==='system'&&event.subtype==='init') {
    assert.equal(initialized,false,'duplicate native Claude initialization');initialized=true;
    if(typeof event.session_id!=='string'||!event.session_id)
     nativeFailure(host,'NATIVE_HOST_SESSION_MISMATCH','native Claude initialization lacks its session',event);
    session=event.session_id;
    assert.equal(event.model,selection.model,'native Claude model differs from approved selection');
    const count=selection.installed?1:0;
    assert.ok(Array.isArray(event.plugins),'native Claude plugin set differs from paired mode');
    assert.ok(Array.isArray(event.mcp_servers),'native Claude MCP connections differ from paired mode');
    // The pinned native host advertises its compiled-in plugins separately
    // from installed plugins. Retain their exact identities and require the
    // same native set in both legs; no user plugin is a baseline exemption.
    builtinPlugins=0;
    let installedPlugins=0;
    for(const plugin of event.plugins) {
     nativeObject(host,plugin,'native Claude plugin');
     const bit=plugin.name==='agents-md'?1:plugin.name==='plugin-authoring'?2:0;
     if(bit&&plugin.path==='builtin'&&plugin.source===plugin.name+'@builtin') {
      assert.equal(builtinPlugins&bit,0,'duplicate native Claude builtin plugin');
      builtinPlugins|=bit;
     } else {
      assert.equal(plugin.name,'acyclic','native Claude plugin set differs from paired mode');
      installedPlugins++;
     }
    }
    assert.equal(installedPlugins,count,'native Claude plugin set differs from paired mode');
    assert.equal(event.mcp_servers.length,count,'native Claude MCP connections differ from paired mode');
    if(selection.installed) {
     nativeObject(host,event.mcp_servers[0],'native Claude MCP connection');
     assert.equal(event.mcp_servers[0].name,'plugin:acyclic:acyclic-sessions','native Claude MCP connections differ from paired mode');
     assert.equal(event.mcp_servers[0].status,'connected','native Claude MCP connections differ from paired mode');
    }
   }
   if(event.type==='user') {
    const message=nativeObject(host,event.message,'native Claude user message');
    for(const part of nativeParts(host,message.content,'native Claude user content')) {
     nativeObject(host,part,'native Claude content part');
     if(part.type==='tool_result'&&part.is_error===true)
      nativeFailure(host,'NATIVE_HOST_TOOL_FAILED','native Claude tool result failed',part);
    }
   }
   if(event.type==='result') {
    if(selection&&!initialized)
     nativeFailure(host,'NATIVE_HOST_SESSION_MISMATCH','native Claude initialization missing',event);
    if(selection&&(event.session_id!==session||event.parent_tool_use_id!=null))
     nativeFailure(host,'NATIVE_HOST_SESSION_MISMATCH','native Claude result differs from the selected root session',event);
    if(event.subtype!=='success'||event.is_error!==false) {
     nativeFailure(host,'NATIVE_HOST_MODEL_FAILED','native Claude turn failed',event);
    }
    if(terminal)nativeFailure(host,'NATIVE_HOST_TERMINAL_ORDER','duplicate native Claude terminal',event);
    terminal=event;
   }
  } else if(host==='pi') {
   const message=event.type==='message_end'?nativeObject(host,event.message,'native Pi message'):undefined;
   if(message?.role!=='assistant')continue;
   terminal=message;
   if(message.stopReason==='error') {
    nativeFailure(host,'NATIVE_HOST_MODEL_FAILED',message.errorMessage,message);
   }
  } else {
   throw new Error('step=validate native terminal; recovery=select codex, claude-code or pi');
  }
 }
 if(host==='codex')assert.equal(turn,'complete','native Codex terminal missing');
 else if(host==='claude-code') {
  if(selection)assert.equal(initialized,true,'native Claude initialization missing');
  assert.ok(terminal,'native Claude terminal missing');
  text=terminal.result;
 } else {
  assert.equal(terminal?.stopReason,'stop','native Pi terminal missing');
  text='';
  for(const part of nativeParts(host,terminal.content,'native Pi content')) {
   nativeObject(host,part,'native Pi content part');
   if(part.type==='text') {
    if(typeof part.text!=='string')nativeFailure(host,'NATIVE_HOST_EVENT_INVALID','native Pi text is not a string',part);
    text+=part.text;
   }
  }
 }
 assert.ok(typeof text==='string'&&text===answer,'native answer missing');
 // Return the already decoded original terminal; consumers must not parse a
 // second copy or infer provider usage from process duration or session reuse.
 return {builtinPlugins,session:session??terminal?.session_id??null,terminal};
}


// Native policy arguments the root runs under and every managed child inherits
// (ACYCLIC_HOST_ARGS), plus each host's own keyless provider selection.
// Sandbox and approval policy stay unchanged. Codex asks the plugin's
// spawn permission hook, which the owner's approval policy answers; other
// prompts stay native. Claude Code uses its native coordination allow list.
export const nativeModel=(host,live)=>live?.model??(host==='claude-code'?'claude-haiku-5-5':'tiny');

// Query the host's installed state through the shipping native exchange.
// Native `plugin list` also fetches the remote discovery catalog, even when
// only its installed JSON rows are requested. Filtering one marketplace would
// hide an unrelated enabled plugin from the paired-profile admission.
export async function codexInstalled(run,executable,expected) {
 const result=await run(executable,['doctor','codex']);
 assert.equal(result.code,0,result.stderr);
 const state=JSON.parse(result.stdout).native_plugins;
 assert.ok(state&&Array.isArray(state.marketplaces)&&Array.isArray(state.marketplaceLoadErrors),
  'step=admit native installed-state response; recovery=use the matching shipping executable with plugin/installed observation');
 assert.deepEqual(state.marketplaceLoadErrors,[],
  'step=admit native installed-state closure; recovery=repair original native marketplace load errors');
 const installed=state.marketplaces.flatMap(marketplace=>{
  assert.ok(Array.isArray(marketplace.plugins),'native marketplace lacks installed plugin rows');
  return marketplace.plugins.map(plugin=>{
   assert.ok(typeof plugin.id==='string'&&typeof plugin.installed==='boolean'&&typeof plugin.enabled==='boolean',
    'step=admit native installed plugin; recovery=repair the original native installed-state response');
   return {pluginId:plugin.id,installed:plugin.installed,enabled:plugin.enabled,version:plugin.localVersion??plugin.version};
  });
 });
 if(expected!==undefined)assert.deepEqual(installed.filter(row=>row.enabled).map(row=>row.pluginId),expected,
  'step=admit native plugins; recovery=use an exclusive native login profile with no unrelated enabled plugins');
 return installed;
}

export async function launchNativeHost(host,{url,root,packageRoot,workspace,env,tools,nativePermissions,prompt,live,extension,installed=true,input=false,resume}) {
 if(resume!==undefined)assert.ok(host==='codex'&&typeof resume==='string'&&resume.length>0,
  'step=admit native continuation; recovery=supply the original Codex thread ID');
 const model=nativeModel(host,live);
 const proxy=nativeProxy(live);
 if(host==='codex') {
  if(proxy)env.ACYCLIC_NATIVE_PROXY_TOKEN=proxy.credential;
  const provider=proxy?['--config','model_provider="acyclic_proxy"','--config','model_providers.acyclic_proxy.name="Authenticated native proxy"',
   '--config','model_providers.acyclic_proxy.base_url='+JSON.stringify(proxy.baseUrl),
   '--config','model_providers.acyclic_proxy.wire_api="responses"',
   '--config','model_providers.acyclic_proxy.requires_openai_auth=false',
   '--config','model_providers.acyclic_proxy.env_key="ACYCLIC_NATIVE_PROXY_TOKEN"',
   '--config','shell_environment_policy.filters.ACYCLIC_NATIVE_PROXY_TOKEN="exclude"','--model',model]
   :live?['--config','model_provider="openai"','--model',model]
   :['--config','model_provider="mock"','--config','model_providers.mock.name="Local deterministic fixture"',
   '--config',`model_providers.mock.base_url="${url}"`,'--config','model_providers.mock.wire_api="responses"',
   '--config','model_providers.mock.requires_openai_auth=false','--model',model];
  const policy=[...provider,...codexSandboxConfig,
   '--ask-for-approval','never','--sandbox','workspace-write','--add-dir',workspace];
  return {policy,args:[...policy,'exec',...(resume===undefined?[]:['resume',resume]),'--json',input?'-':prompt]};
 }
 if(host==='claude-code') {
  if(proxy) {
   for(const key of ['CLAUDE_CODE_USE_FOUNDRY','CLAUDE_CODE_USE_BEDROCK','CLAUDE_CODE_USE_VERTEX'])delete env[key];
   Object.assign(env,{ANTHROPIC_BASE_URL:proxy.baseUrl,ANTHROPIC_API_KEY:proxy.credential,
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',CLAUDE_CODE_SUBPROCESS_ENV_SCRUB:'1'});
  }
  // Foundry's Messages endpoint with its documented gateway mode: no key,
  // token or account header reaches the localhost mock.
  if(!live)Object.assign(env,{CLAUDE_CODE_USE_FOUNDRY:'1',CLAUDE_CODE_SKIP_FOUNDRY_AUTH:'1',
   ANTHROPIC_FOUNDRY_BASE_URL:url.replace(/\/v1$/,''),CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'});
  const settings=join(root,'claude-policy.json');
  await writeFile(settings,JSON.stringify({permissions:{allow:[...tools.map(name=>'mcp__plugin_acyclic_acyclic-sessions__'+name),...nativePermissions]}}));
  const policy=['--model',model,'--settings',settings,'--permission-mode','default',
   // Keep native login and managed policy without loading unrelated user plugins.
   ...(live?['--setting-sources','']:[]),
   ...(installed?['--plugin-dir',join(packageRoot,'claude')]:[]),'--add-dir',workspace];
  return {policy,args:['-p',...(input?[]:[prompt]),'--output-format','stream-json','--verbose',...policy]};
 }
 if(proxy)env.ACYCLIC_NATIVE_PROXY=JSON.stringify(live);
 else if(!live)env.ACYCLIC_E2E_MODEL_URL=url;
 const policy=live?['--no-extensions',...(installed?['--extension',extension]:[]),
  ...(proxy?['--extension',fileURLToPath(new URL('./pi-provider.mjs',import.meta.url))]:[]),
  '--provider',live.provider,'--model',model,'--session-dir',join(root,'native-sessions')]
  :['--extension',fileURLToPath(new URL('./pi-provider.mjs',import.meta.url)),'--provider','fixture','--model',model];
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
 nativeProvider(native,fixture.live,fixture);
 const binary=fixture.env.E2E_PLUGIN_BINARY;
 assert.ok(binary&&isAbsolute(binary),'step=admit E2E_PLUGIN_BINARY; recovery=set E2E_PLUGIN_BINARY to its matching prebuilt absolute executable');
 const {workspace}=fixture,root=dirname(workspace),env={...fixture.env};
 if(fixture.live&&!fixture.live.proxy) {
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
 } else if(!fixture.live) {
  let endpoint;
  try {endpoint=new URL(fixture.url);}
  catch(cause) {throw new Error('step=admit mock endpoint; recovery=run through scripts/graphcoder-e2e/run.mjs',{cause});}
  assert.ok(endpoint.protocol==='http:'&&endpoint.hostname==='127.0.0.1',
   'step=admit mock endpoint; recovery=supply the original loopback fixture.url');
 }
 env.PATH=[dirname(native.entry),native.path,env.PATH].join(delimiter);
 let step='stage native package';
 const run=async(program,args,input,launchEnv=env)=>{
  // The long-lived workspace owner holds the one mount lease. Commands retain
  // process-tree custody without attempting another lease over its live views.
  // Owned external commands receive empty stdin; the one workspace-API request
  // with a body runs inside the same scope through its ordinary process path.
  const record={program,args,...await (input===undefined?processes.runOwned(program,args,{cwd:workspace,env:launchEnv})
   :processes.run(program,args,{cwd:workspace,env:launchEnv},input))};
  capture(record);return record;
 };
 try {
  const {packageRoot,executable}=await (packageStage??nativePackage(binary,join(root,'package')))();
  env.E2E_PLUGIN_BINARY=executable;
  step='install through shipping plugin';
  if(host==='codex') {
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot]);
   assert.equal(installation.code,0,installation.stderr);
   const installed=await codexInstalled(run,executable,fixture.live?['acyclic@acyclic']:undefined);
   assert.ok(installed.some(row=>row.pluginId==='acyclic@acyclic'&&row.enabled),'native plugin manager lacks acyclic@acyclic');
  } else if(host==='pi') {
   const extension=fixture.live?join(root,'acyclic.ts'):join(env.PI_CODING_AGENT_DIR,'extensions/acyclic.ts');
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot,'--settings',extension]);
   assert.equal(installation.code,0,installation.stderr);
   assert.ok((await readFile(extension,'utf8')).includes(JSON.stringify(toNamespacedPath(await realpath(executable)))),'native extension does not bind admitted shipping binary');
  }
  const configuration={url:fixture.url,live:fixture.live,root,packageRoot,workspace,env,tools,nativePermissions,prompt,
   extension:fixture.live&&host==='pi'?join(root,'acyclic.ts'):undefined};
  const {policy,args}=await launchNativeHost(host,configuration);
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
  Object.assign(env,{ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:JSON.parse(opened.stdout).task,ACYCLIC_HOST_ARGS:JSON.stringify(policy)});
  step='complete installed native session';
  if(execute)return await execute({native,configuration,run});
  const nativeEnv=fixture.nativeCapability===undefined?env:
   {...env,ACYCLIC_E2E_MODEL_CAPABILITY:fixture.nativeCapability};
  const completed=await run(native.program,[...native.args,...args],undefined,nativeEnv);
  assertNativeCompletion(host,completed,answer,host==='claude-code'
   ?{model:nativeModel(host,fixture.live),installed:true}:undefined);
 } catch(error) {
  throw new Error(`step=${host} ${step}: ${error.message}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`,{cause:error});
 }
}
