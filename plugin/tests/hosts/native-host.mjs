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

// Require the fixture's provider contract from the same admitted package/bin.
// An unsupported provider must never acquire an installed workspace owner.
export function nativeProvider(native,live) {
 if(live!==undefined) {
  assert.ok(live&&typeof live==='object'&&!Array.isArray(live),
   'step=admit live native configuration; recovery=supply model, profile and provider');
  assert.deepEqual(Object.keys(live).sort(),['model','profile','provider'],
   'step=admit live native configuration; recovery=supply only model, profile and provider, never credentials');
  const provider={'@openai/codex':'openai','@anthropic-ai/claude-code':'anthropic',
   '@earendil-works/pi-coding-agent':'openai-codex'}[native.package.name];
  assert.equal(live.provider,provider,'step=admit live native provider; recovery=select the pinned host native login provider');
  assert.ok(typeof live.model==='string'&&/^[a-z0-9][a-z0-9.-]*$/.test(live.model),
   'step=admit live native model; recovery=supply the explicitly approved exact model ID');
  assert.ok(isAbsolute(live.profile??''),'step=admit live native login; recovery=supply an existing dedicated native login profile by absolute path');
  if(native.package.name==='@anthropic-ai/claude-code')assert.equal(process.platform,'win32',
   'step=admit live Claude platform; recovery=run Claude Code live on Windows');
  return provider;
 }
 if(native.provider!==null)return native.provider;
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
