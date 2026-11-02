import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {readFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {test} from 'node:test';
import {nativeProvider,nativeProxy,parseNativeProxy} from './native-host.mjs';
import {launchNativeHost} from './native-model.mjs';
import {fuzz} from './native-proxy.fuzz.mjs';

const {dependencies}=JSON.parse(readFileSync(new URL('./package.json',import.meta.url),'utf8'));
const native=name=>({package:{name,version:dependencies[name]}});
const cap=randomBytes(32).toString('hex'),proxy='http://127.0.0.1:12345/'+cap;

test('native proxy admission retains host families and rejects configuration before launch',()=> {
 for(const provider of ['openai','anthropic','gemini']) {
  const live={provider,model:'selected-model',proxy};
  assert.equal(nativeProvider(native('@earendil-works/pi-coding-agent'),live),provider);
  assert.deepEqual(parseNativeProxy(JSON.stringify(live)),nativeProxy(live));
  assert.equal(nativeProxy(live).baseUrl,'http://127.0.0.1:12345/'+provider);
 }
 assert.equal(nativeProvider(native('@openai/codex'),{provider:'openai',model:'selected-model',proxy}),'openai');
 for(const provider of ['anthropic','gemini'])
  assert.throws(()=>nativeProvider(native('@openai/codex'),{provider,model:'selected-model',proxy}),/step=admit live native provider/);
 for(const changed of [{proxy:'https://127.0.0.1/'+cap},{proxy:'http://example.com/'+cap},
  {proxy:proxy+'/'},{proxy:proxy+'?key=unadmitted'},{proxy:proxy+'#fragment'},
  {proxy:proxy.replace(cap,'short')},{profile:'/unadmitted'},{apiKey:'unadmitted'},
  {provider:'fixture'},{model:null}]) {
  const live={provider:'openai',model:'selected-model',proxy,...changed};
  assert.throws(()=>nativeProvider(native('@earendil-works/pi-coding-agent'),live),/step=admit native proxy/);
  fuzz(Buffer.from(JSON.stringify(live)));
 }
 for(const value of ['null','[]','{}','false','0','""','{'])fuzz(Buffer.from(value));
});

test('native proxy launch retains managed policy and gives only the ephemeral capability to native auth',async()=> {
 const root=mkdtempSync(join(tmpdir(),'native-proxy-'));
 try {
  const configuration={root,packageRoot:root,workspace:root,tools:[],nativePermissions:[],prompt:'admission',installed:false};
  for(const host of ['codex','claude-code','pi']) {
   const live={provider:host==='claude-code'?'anthropic':'openai',model:'selected-model',proxy},env={};
   const launched=await launchNativeHost(host,{...configuration,live,env});
   assert.ok(!JSON.stringify(launched).includes(cap),'capability must not enter argv or forwarded policy');
   if(host==='codex') {
    assert.ok(launched.policy.includes('never'));assert.ok(launched.policy.includes('workspace-write'));
    assert.equal(env.ACYCLIC_NATIVE_PROXY_TOKEN,cap);
    assert.ok(launched.policy.includes('shell_environment_policy.filters.ACYCLIC_NATIVE_PROXY_TOKEN="exclude"'));
   } else if(host==='claude-code') {
    assert.ok(launched.policy.includes('default'));assert.equal(env.ANTHROPIC_API_KEY,cap);
   } else assert.deepEqual(JSON.parse(env.ACYCLIC_NATIVE_PROXY),live);
  }
 } finally {rmSync(root,{recursive:true});}
});
