import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
import {fuzz,fuzzProvider} from './native-fixture.fuzz.mjs';
import {nativeProvider} from './native-host.mjs';

test('native fixture authority fuzzing rejects planted admission defects',async()=>{
 // Reuse the actual pinned SDK's request route. Credentials are generated
 // configuration, never a recorded upstream secret or fabricated provider wire.
 const original=JSON.parse(await readFile(new URL('./fixtures/native-pi-fixture-request.provenance.json',import.meta.url),'utf8'));
 const url='http://127.0.0.1:12345'+original.request_path.replace(/\/responses$/,'');
 const credential='0123456789abcdef'.repeat(4);
 const source=await readFile(new URL('./native-host.mjs',import.meta.url),'utf8');
 const fixtureStart=source.indexOf('export function nativeFixture(');
 assert.ok(fixtureStart>=0,'original fixture admission is missing');
 const bad=[
  {url:url.replace('127.0.0.1','example.com'),credential},
  {url,credential:'dummy'},
  {url:url.replace('/native-','/'),credential},
 ];
 const inputs=[{url,credential},{url},null,[],...bad,
  {url:'not a URL',credential},
  {url:url+'?query',credential},{url:url+'#fragment',credential},
  {url:url.replace('http:','https:'),credential},{url,credential:{}}];
 const packages=JSON.parse(await readFile(new URL('./package.json',import.meta.url),'utf8')).dependencies;
 const native={provider:null,package:{name:'@earendil-works/pi-coding-agent',version:packages['@earendil-works/pi-coding-agent']}};
 for(const input of inputs) {
  const bytes=Buffer.from(JSON.stringify(input));
  fuzz(bytes);
  // The identical authority law also checks the actual provider-dispatch join.
  fuzz(bytes,(url,credential)=>{
   if(credential===undefined)return;
   assert.equal(nativeProvider(native,undefined,{url,nativeCapability:credential}),'authenticated-responses');
   return {baseUrl:new URL(url).href,credential};
  });
 }
 assert.throws(()=>nativeProvider(native),{code:'UNSUPPORTED_NATIVE_KEYLESS_PROVIDER'});
 for(const provider of ['keyless-responses','keyless-foundry'])assert.equal(nativeProvider({provider}),provider);
 for(let byte=0;byte<256;byte++) {
  fuzz(Buffer.from([byte]));
  fuzz(Buffer.from(JSON.stringify({url:url+String.fromCharCode(byte),credential})));
 }
 for(const [index,[before,after]] of [
  ["&&endpoint.hostname==='127.0.0.1'",''],
  ["/^[a-f0-9]{64}$/.test(credential)",'true'],
  ['/^\\/native-[^/]+\\/v1$/.test(endpoint.pathname)','true'],
 ].entries()) {
  assert.ok(source.includes(before),'planted defect no longer matches production source');
  const changed=source.slice(0,fixtureStart)+source.slice(fixtureStart).replace(before,after);
  const mutant=await import('data:text/javascript;base64,'+Buffer.from(changed).toString('base64'));
  assert.throws(()=>fuzz(Buffer.from(JSON.stringify(bad[index])),mutant.nativeFixture),
   /untrusted fixture configuration was admitted/);
 }
});

test('native provider configuration fuzzing retains typed admission',async()=>{
 const packages=JSON.parse(await readFile(new URL('./package.json',import.meta.url),'utf8')).dependencies;
 const providers={'@openai/codex':'openai','@anthropic-ai/claude-code':'anthropic',
  '@earendil-works/pi-coding-agent':'openai-codex'};
 const source=await readFile(new URL('./native-host.mjs',import.meta.url),'utf8');
 for(const [name,provider] of Object.entries(providers)) {
  const native={package:{name,version:packages[name]}};
  const configuration={model:'tiny',provider,profile:process.cwd()};
  for(const value of [null,[],false,0,'',{},[null]]) {
   fuzzProvider(Buffer.from(JSON.stringify(value)),native);
   for(const key of Object.keys(configuration))fuzzProvider(Buffer.from(JSON.stringify({...configuration,[key]:value})),native);
  }
  fuzzProvider(Buffer.from(JSON.stringify(configuration)),native);
 }
 const before="typeof live.profile==='string'&&isAbsolute(live.profile)";
 assert.ok(source.includes(before),'planted defect no longer matches production source');
 const mutant=await import('data:text/javascript;base64,'+Buffer.from(source.replace(before,'isAbsolute(live.profile)')).toString('base64'));
 const native={package:{name:'@openai/codex',version:packages['@openai/codex']}};
 assert.throws(()=>fuzzProvider(Buffer.from(JSON.stringify({provider:'openai',model:'tiny',profile:{}})),native,mutant.nativeProvider),
  /native provider escaped its admission boundary/);
});
