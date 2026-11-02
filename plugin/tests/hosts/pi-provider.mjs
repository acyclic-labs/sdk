// Both servers retain the pinned provider's serializer and credential interface.
// Each issues a real in-memory capability. Missing fixture authentication keeps
// its original unsupported disposition when the native client requires a key.
import assert from 'node:assert/strict';
import {createProvider} from '@earendil-works/pi-ai/models';
import * as responses from '@earendil-works/pi-ai/api/openai-responses';
import {nativeFixture,parseNativeProxy} from './native-host.mjs';

function streamSimple(model,context,options) {
 try {return responses.streamSimple(model,context,options);}
 catch(cause) {
  if(cause.message!==`No API key for provider: ${model.provider}`)throw cause;
  const error=new Error('step=admit Pi keyless Responses provider: '+cause.message+
   '; recovery=select a native Pi release with a supported keyless Responses provider before running installed-pi-recursive',{cause});
  error.code='UNSUPPORTED_NATIVE_KEYLESS_PROVIDER';
  throw error;
 }
}

export default async function register(pi) {
 const proxy=process.env.ACYCLIC_NATIVE_PROXY,credential=process.env.ACYCLIC_E2E_MODEL_CAPABILITY;
 assert.ok(!(proxy&&credential),'step=admit Pi provider; recovery=select one original proxy or deterministic model capability');
 const key=proxy?'ACYCLIC_NATIVE_PROXY':'ACYCLIC_E2E_MODEL_CAPABILITY',bootstrap=proxy??credential;
 if(bootstrap) {
  delete process.env[key];
  // Only the trusted coordination bridge receives the retained bootstrap;
  // ordinary guest commands inherit the scrubbed native environment.
  pi.events.on('acyclic:coordination-environment',env=>{env[key]=bootstrap;});
 }
 if(proxy) {
  const {provider,model,baseUrl,credential}=parseNativeProxy(bootstrap);
  const family={openai:'openai',anthropic:'anthropic',gemini:'google'}[provider];
  const constants={openai:'OPENAI_MODELS',anthropic:'ANTHROPIC_MODELS',google:'GOOGLE_MODELS'};
  const tables=await import('@earendil-works/pi-ai/providers/'+family+'.models');
  const original=tables[constants[family]][model];
  assert.ok(original,'step=admit native Pi model; recovery=select an exact model in the pinned native provider catalogue');
  const protocol={openai:'openai-responses',anthropic:'anthropic-messages',google:'google-generative-ai'}[family];
  const api=await import('@earendil-works/pi-ai/api/'+protocol);
  pi.registerProvider(createProvider({id:provider,name:'Authenticated native proxy',baseUrl,
   models:[{...original,provider,baseUrl}],
   auth:{apiKey:{name:'Ephemeral proxy capability',resolve:async()=>({auth:{apiKey:credential}})}},
   api:{stream:api.stream,streamSimple:api.streamSimple}}));
  return;
 }
 const baseUrl=process.env.ACYCLIC_E2E_MODEL_URL;
 let endpoint;
 try {endpoint=new URL(baseUrl);}
 catch(cause) {throw new Error('step=admit Pi fixture endpoint; recovery=run node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL installed-pi-recursive with its loopback endpoint',{cause});}
 assert.equal(endpoint.protocol,'http:','step=admit Pi fixture endpoint; recovery=supply the loopback mock HTTP endpoint');
 assert.equal(endpoint.hostname,'127.0.0.1','step=admit Pi fixture endpoint; recovery=supply the loopback mock HTTP endpoint');
 nativeFixture(baseUrl,credential);
 pi.registerProvider(createProvider({
  id:'fixture',name:'Loopback fixture',baseUrl,
  auth:{apiKey:{name:'Native model capability',resolve:async()=>({auth:credential?{apiKey:credential}:{}})}},
  models:[{id:'tiny',name:'Deterministic acceptance',provider:'fixture',api:'openai-responses',baseUrl,
   reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:32768,maxTokens:1024}],
  api:{stream:responses.stream,streamSimple},
 }));
}
