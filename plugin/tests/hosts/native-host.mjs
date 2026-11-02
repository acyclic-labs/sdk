import assert from 'node:assert/strict';
import {readFile,realpath,stat} from 'node:fs/promises';
import {basename,dirname,extname,isAbsolute,join} from 'node:path';
import {fileURLToPath} from 'node:url';

const packages=Object.freeze({
 codex:['codex','@openai/codex','keyless-responses'],
 'claude-code':['claude','@anthropic-ai/claude-code','keyless-foundry'],
 // The pinned Pi Responses interface rejects empty auth before HTTP. Its real
 // provider/CLI control qualifies this refusal, not a substitute credential.
 pi:['pi','@earendil-works/pi-coding-agent',null],
});

// The capability is issued and checked by the original proxy. It is a real
// short-lived local credential, never an upstream provider API key.
export function nativeProxy(live) {
 if(live?.proxy===undefined)return;
 assert.deepEqual(Object.keys(live).sort(),['model','provider','proxy'],
  'step=admit native proxy configuration; recovery=supply only provider, model and the original proxy endpoint');
 let endpoint;
 try {endpoint=new URL(live.proxy);}
 catch(cause) {throw new Error('step=admit native proxy endpoint; recovery=supply the original loopback proxy URL',{cause});}
 assert.ok(endpoint.protocol==='http:'&&endpoint.hostname==='127.0.0.1'&&endpoint.port&&
  !endpoint.username&&!endpoint.password&&!endpoint.search&&!endpoint.hash&&/^\/[a-f0-9]{64}$/.test(endpoint.pathname),
 'step=admit native proxy capability; recovery=supply the original loopback bridge and its unexpired path capability');
 assert.ok(['openai','anthropic','gemini'].includes(live.provider),
  'step=admit native proxy provider; recovery=select openai, anthropic or gemini');
 assert.ok(typeof live.model==='string'&&/^[a-z0-9][a-z0-9.-]*$/.test(live.model),
  'step=admit native proxy model; recovery=supply the explicitly approved exact model ID');
 return {provider:live.provider,model:live.model,baseUrl:endpoint.origin+'/'+live.provider,credential:endpoint.pathname.slice(1)};
}

export function parseNativeProxy(text) {
 let live;
 try {live=JSON.parse(text);}
 catch(cause) {throw new Error('step=decode native proxy configuration; recovery=pass the original admitted proxy configuration',{cause});}
 const proxy=nativeProxy(live);
 assert.ok(proxy,'step=admit native proxy configuration; recovery=pass the original admitted proxy configuration');
 return proxy;
}

export function nativeFixture(url, credential) {
 if(credential===undefined)return;
 assert.ok(typeof credential==='string'&&/^[a-f0-9]{64}$/.test(credential),
  'step=admit native fixture credential; recovery=use the original runner-issued in-memory capability');
 let endpoint;
 try {endpoint=new URL(url);}
 catch(cause) {throw new Error('step=admit authenticated native fixture; recovery=use the original authenticated loopback model route',{cause});}
 assert.ok(endpoint.protocol==='http:'&&endpoint.hostname==='127.0.0.1'&&endpoint.port&&
  !endpoint.username&&!endpoint.password&&!endpoint.search&&!endpoint.hash&&/^\/native-[^/]+\/v1$/.test(endpoint.pathname),
  'step=admit authenticated native fixture; recovery=use the original authenticated loopback model route');
 return {baseUrl:endpoint.href,credential};
}

// Require the fixture's provider contract from the same admitted package/bin.
// An unsupported provider must never acquire an installed workspace owner.
export function nativeProvider(native,live,fixture) {
 if(live!==undefined) {
  assert.ok(live&&typeof live==='object'&&!Array.isArray(live),
   'step=admit live native configuration; recovery=supply model, profile and provider');
  const proxy=nativeProxy(live);
  if(!proxy)assert.deepEqual(Object.keys(live).sort(),['model','profile','provider'],
   'step=admit live native configuration; recovery=supply only model, profile and provider, never credentials');
  const provider={'@openai/codex':'openai','@anthropic-ai/claude-code':'anthropic',
   '@earendil-works/pi-coding-agent':'openai-codex'}[native.package.name];
  if(!(proxy&&native.package.name==='@earendil-works/pi-coding-agent')&&live.provider!==provider)
   throw Object.assign(new Error('step=admit live native provider: the selected host does not support this provider; recovery=select the pinned host native provider'),
    {code:'UNSUPPORTED_NATIVE_PROVIDER'});
  if(!proxy)assert.ok(typeof live.model==='string'&&/^[a-z0-9][a-z0-9.-]*$/.test(live.model),
   'step=admit live native model; recovery=supply the explicitly approved exact model ID');
  if(!proxy)assert.ok(isAbsolute(live.profile??''),'step=admit live native login; recovery=supply an existing dedicated native login profile by absolute path');
  if(native.package.name==='@anthropic-ai/claude-code')assert.equal(process.platform,'win32',
   'step=admit live Claude platform; recovery=run Claude Code live on Windows');
  return proxy?live.provider:provider;
 }
 if(native.provider!==null)return native.provider;
 if(nativeFixture(fixture?.url,fixture?.nativeCapability))return 'authenticated-responses';
 const error=new Error(`step=admit native provider: ${native.package.name}@${native.package.version} has no supported keyless Responses interface; recovery=select a native Pi release with a supported keyless Responses provider before running installed-pi-recursive`);
 error.code='UNSUPPORTED_NATIVE_KEYLESS_PROVIDER';
 throw error;
}

// Resolve the actual package entry, never an npm shell/cmd shim. Both launch
// and acceptance provenance use this same admission result.
export async function nativeHost(host,env,root=fileURLToPath(new URL('.',import.meta.url))) {
 const definition=packages[host];
 assert.ok(definition,'step=admit native host; recovery=select codex|claude-code|pi');
 const [name,pkg,provider]=definition,key='E2E_'+name.toUpperCase();
 try {
  const modules=join(root,'node_modules');
  const pinned=JSON.parse(await readFile(join(root,'package.json'),'utf8')).dependencies[pkg];
  let entry=env[key];
  if(entry===undefined) {
   const directory=join(modules,pkg);
   const manifest=JSON.parse(await readFile(join(directory,'package.json'),'utf8'));
   entry=join(directory,typeof manifest.bin==='string'?manifest.bin:manifest.bin[name]);
  }
  assert.ok(isAbsolute(entry),'host entry must be absolute');
  entry=await realpath(entry);
  assert.ok((await stat(entry)).isFile(),'host entry must be a file');
  assert.ok(!['.cmd','.bat'].includes(extname(entry).toLowerCase()),'provide the native or JavaScript entry, not an npm command shim');
  // An arbitrary wrapper can import or launch bytes outside the supplied
  // entry. Admit only the actual pinned package bin and its install tree.
  let directory=dirname(entry),manifest;
  for(;;) {
   try {manifest=JSON.parse(await readFile(join(directory,'package.json'),'utf8'));}
   catch(error) {if(error.code!=='ENOENT')throw error;}
   if(manifest?.name===pkg)break;
   const parent=dirname(directory);
   assert.notEqual(parent,directory,'host entry is not in its pinned package');
   directory=parent;manifest=undefined;
  }
  assert.equal(manifest.version,pinned,'host package differs from the pinned version');
  const bin=typeof manifest.bin==='string'?manifest.bin:manifest.bin?.[name];
  assert.ok(typeof bin==='string','host package does not declare its bin');
  assert.equal(await realpath(join(directory,bin)),entry,'override must select the declared package bin');
  let closure;
  for(let parent=directory;;parent=dirname(parent)) {
   if(basename(parent)==='node_modules')closure=parent;
   if(dirname(parent)===parent)break;
  }
  assert.ok(closure,'host package must retain its node_modules dependency tree');
  const javascript=['.js','.mjs','.cjs'].includes(extname(entry));
  return {program:javascript?process.execPath:entry,args:javascript?[entry]:[],entry,
   path:join(closure,'.bin'),closure,provider,package:{name:pkg,version:pinned}};
 } catch(cause) {
  throw new Error(`step=admit ${key}: ${cause.message}; recovery=npm ci --prefix plugin/tests/hosts and unset ${key}, or set ${key} to that pinned package's declared bin with its installed node_modules tree`,{cause});
 }
}
