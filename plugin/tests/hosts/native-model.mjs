// Installed real native hosts against the shipping mock; no authentication
// fixture and no login. The acceptance matrix owns the process scope, mock,
// deadline and request oracle; this module only drives one installed host.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {existsSync} from 'node:fs';
import {readFile,writeFile} from 'node:fs/promises';
import {dirname,delimiter,isAbsolute,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {stagePlugin} from './package.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';

const programs=Object.freeze({codex:'codex','claude-code':'claude',pi:'pi'});

// The pinned host closure in this directory's package-lock.json, unless the
// caller names E2E_CODEX, E2E_CLAUDE or E2E_PI.
export function nativeExecutable(host,env) {
 const program=programs[host];
 assert.ok(program,'step=admit native host; recovery=select '+Object.keys(programs).join('|'));
 const name='E2E_'+program.toUpperCase();
 const path=env[name]??fileURLToPath(new URL('./node_modules/.bin/'+program,import.meta.url));
 assert.ok(isAbsolute(path)&&existsSync(path),
  `step=admit ${name}; recovery=npm ci --prefix plugin/tests/hosts, or set ${name} to the absolute pinned ${program} executable`);
 return path;
}

// Native policy arguments the root runs under and every managed child inherits
// (ACYCLIC_HOST_ARGS), plus each host's own keyless provider selection. Only
// the coordination tools the case calls are approved, by each host's native
// per-tool approval; sandbox and approval policy stay unchanged.
async function launch(host,{url,root,packageRoot,workspace,env,tools,prompt}) {
 if(host==='codex') {
  const policy=['--config','model_provider="mock"','--config','model_providers.mock.name="Local deterministic fixture"',
   '--config',`model_providers.mock.base_url="${url}"`,'--config','model_providers.mock.wire_api="responses"',
   '--config','model_providers.mock.requires_openai_auth=false','--model','tiny',
   ...tools.flatMap(name=>['--config',`plugins."acyclic@acyclic".mcp_servers."acyclic-sessions".tools.${name}.approval_mode="approve"`]),
   '--ask-for-approval','never','--sandbox','workspace-write','--add-dir',workspace];
  return {policy,args:[...policy,'exec','--json',prompt]};
 }
 if(host==='claude-code') {
  // Foundry's Messages endpoint with its documented gateway mode: no key,
  // token or account header reaches the localhost mock.
  Object.assign(env,{CLAUDE_CODE_USE_FOUNDRY:'1',CLAUDE_CODE_SKIP_FOUNDRY_AUTH:'1',
   ANTHROPIC_FOUNDRY_BASE_URL:url.replace(/\/v1$/,''),CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'});
  const settings=join(root,'claude-policy.json');
  await writeFile(settings,JSON.stringify({permissions:{allow:tools.map(name=>'mcp__plugin_acyclic_acyclic-sessions__'+name)}}));
  const policy=['--model','claude-haiku-5-5','--settings',settings,'--permission-mode','default',
   '--plugin-dir',join(packageRoot,'claude'),'--add-dir',workspace];
  return {policy,args:['-p',prompt,'--output-format','stream-json','--verbose',...policy]};
 }
 env.ACYCLIC_E2E_MODEL_URL=url;
 const policy=['--extension',fileURLToPath(new URL('./keyless-pi-provider.mjs',import.meta.url)),'--provider','fixture','--model','tiny'];
 return {policy,args:['--mode','json','--print','--no-skills','--no-prompt-templates','--no-themes',...policy,prompt]};
}

// One installed host session under the caller's process scope: stage the
// production package, install through the shipping CLI or the host's native
// plugin directory, open the root through the workspace API, then run the
// actual host once against the caller's mock endpoint.
export async function installedHost({host,processes,fixture,capture,tools,prompt,answer}) {
 const native=nativeExecutable(host,fixture.env);
 const binary=fixture.env.E2E_PLUGIN_BINARY;
 assert.ok(binary&&isAbsolute(binary),'step=admit E2E_PLUGIN_BINARY; recovery=set E2E_PLUGIN_BINARY to its matching prebuilt absolute executable');
 assert.ok(fixture.env.E2E_MOCK_URL,'step=admit mock endpoint; recovery=run through scripts/graphcoder-e2e/run.mjs');
 const {workspace}=fixture,root=dirname(workspace),env={...fixture.env};
 env.PATH=dirname(native)+delimiter+env.PATH;
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
  const packageRoot=join(root,'package');
  const executable=await stagePlugin(fileURLToPath(new URL('../../',import.meta.url)),binary,packageRoot);
  step='install through shipping plugin';
  if(host==='codex') {
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot]);
   assert.equal(installation.code,0,installation.stderr);
   const listed=await run(native,['plugin','list','--json']);assert.equal(listed.code,0,listed.stderr);
   assert.ok(JSON.parse(listed.stdout).installed.some(row=>row.pluginId==='acyclic@acyclic'&&row.enabled),'native plugin manager lacks acyclic@acyclic');
  } else if(host==='pi') {
   const extension=join(env.PI_CODING_AGENT_DIR,'extensions/acyclic.ts');
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot,'--settings',extension]);
   assert.equal(installation.code,0,installation.stderr);
   assert.ok((await readFile(extension,'utf8')).includes(JSON.stringify(executable)),'native extension does not bind admitted shipping binary');
  }
  step='start shipping workspace owner';
  await startWorkspaceOwner({scope:processes,binary:fixture.binary,workspace,state:join(root,'state'),env,root,
   capture:record=>capture({role:'owner',...record})});
  step='initialize workspace Git';
  const initialized=await run(env.E2E_GIT_BINARY??'git',['init','--quiet']);
  assert.equal(initialized.code,0,initialized.stderr);
  step='open native root through the workspace API';
  const opened=await run(executable,['__workspace'],JSON.stringify({kind:'open',host,session:randomUUID()}));
  assert.equal(opened.code,0,opened.stderr);
  const {policy,args}=await launch(host,{url:fixture.env.E2E_MOCK_URL,root,packageRoot,workspace,env,tools,prompt});
  Object.assign(env,{ACYCLIC_WORKSPACE:workspace,ACYCLIC_TASK:JSON.parse(opened.stdout).task,ACYCLIC_HOST_ARGS:JSON.stringify(policy)});
  step='complete installed native session';
  const completed=await run(native,args);
  assert.equal(completed.code,0,completed.stderr);
  assert.ok(completed.stdout.includes(answer),'native answer missing');
 } catch(error) {
  throw new Error(`step=${host} ${step}: ${error.message}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`,{cause:error});
 }
}
