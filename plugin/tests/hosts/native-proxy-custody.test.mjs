import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {test} from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {hostFixtureDirectory} from '../../../scripts/graphcoder-e2e/fixture-directory.mjs';
import {nativeHost} from './native-host.mjs';
import {launchNativeHost} from './native-model.mjs';

// Startup executes the actual native hook transport without asking a model to
// generate a tool call. The observer records presence only, never the credential.
test('native Claude startup withholds the proxy credential from child commands',
 {skip:process.platform!=='win32'},async()=>{
 const root=hostFixtureDirectory('native-proxy-custody-'),workspace=join(root,'workspace');
 const scope=new Processes(),report=join(root,'observed.json'),observer=join(root,'observe.mjs');
 try {
  await mkdir(workspace);
  await writeFile(observer,`import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(report)},JSON.stringify({api_key_present:Object.hasOwn(process.env,'ANTHROPIC_API_KEY')}));process.stdin.resume();\n`);
  const env=environment(join(root,'profile'));
  const native=await nativeHost('claude-code',env);
  // Initialization does not send a model request. The selected native API URL
  // is loopback even if that contract regresses; no upstream credential exists.
  const live={provider:'anthropic',model:'claude-haiku-5-5',
   proxy:'http://127.0.0.1:1/'+randomBytes(32).toString('hex')};
  const launch=await launchNativeHost('claude-code',{root,workspace,env,live,
   tools:[],nativePermissions:[],installed:false,prompt:'native credential custody'});
  const settings=join(root,'claude-policy.json'),policy=JSON.parse(await readFile(settings,'utf8'));
  policy.hooks={SessionStart:[{hooks:[{type:'command',command:'"'+process.execPath+'" "'+observer+'"'}]}]};
  await writeFile(settings,JSON.stringify(policy));
  const result=await scope.runOwned(native.program,[...native.args,'--init-only',...launch.policy],{cwd:workspace,env});
  assert.equal(result.code,0,result.stderr);
  assert.equal(JSON.parse(await readFile(report,'utf8')).api_key_present,false,
   'native child commands must not inherit the proxy credential');
 } finally {
  try {await scope.close();}finally {await rm(root,{recursive:true,force:true});}
 }
});
