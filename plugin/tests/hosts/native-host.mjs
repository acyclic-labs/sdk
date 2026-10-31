import assert from 'node:assert/strict';
import {readFile,realpath,stat} from 'node:fs/promises';
import {dirname,extname,isAbsolute,join} from 'node:path';
import {fileURLToPath} from 'node:url';

const packages=Object.freeze({
 codex:['codex','@openai/codex'],
 'claude-code':['claude','@anthropic-ai/claude-code'],
 pi:['pi','@earendil-works/pi-coding-agent'],
});

// Resolve the actual package entry, never an npm shell/cmd shim. Both launch
// and acceptance provenance use this same admission result.
export async function nativeHost(host,env,root=fileURLToPath(new URL('.',import.meta.url))) {
 const definition=packages[host];
 assert.ok(definition,'step=admit native host; recovery=select codex|claude-code|pi');
 const [name,pkg]=definition,key='E2E_'+name.toUpperCase();
 try {
  const modules=join(root,'node_modules');
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
  const javascript=['.js','.mjs','.cjs'].includes(extname(entry));
  return {program:javascript?process.execPath:entry,args:javascript?[entry]:[],entry,
   path:join(modules,'.bin'),closure:env[key]===undefined?modules:null};
 } catch(cause) {
  throw new Error(`step=admit ${key}: ${cause.message}; recovery=npm ci --prefix plugin/tests/hosts, or set ${key} to the absolute native or JavaScript host entry`,{cause});
 }
}
