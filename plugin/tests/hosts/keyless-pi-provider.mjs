// The pinned package's Provider.auth supports an empty local-server auth result.
// Its Responses implementation currently rejects that result before HTTP. Keep
// the real provider path and its original failure; never substitute credentials
// or strip an Authorization header after the native client creates it.
import assert from 'node:assert/strict';
import {createProvider} from '@earendil-works/pi-ai/models';
import * as responses from '@earendil-works/pi-ai/api/openai-responses';

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

export default function register(pi) {
 const baseUrl=process.env.ACYCLIC_E2E_MODEL_URL;
 let endpoint;
 try {endpoint=new URL(baseUrl);}
 catch(cause) {throw new Error('step=admit Pi fixture endpoint; recovery=run node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL installed-pi-recursive with its loopback endpoint',{cause});}
 assert.equal(endpoint.protocol,'http:','step=admit Pi fixture endpoint; recovery=supply the loopback mock HTTP endpoint');
 assert.equal(endpoint.hostname,'127.0.0.1','step=admit Pi fixture endpoint; recovery=supply the loopback mock HTTP endpoint');
 pi.registerProvider(createProvider({
  id:'fixture',name:'Keyless loopback fixture',baseUrl,
  auth:{apiKey:{name:'Keyless loopback',resolve:async()=>({auth:{}})}},
  models:[{id:'tiny',name:'Deterministic acceptance',provider:'fixture',api:'openai-responses',baseUrl,
   reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:32768,maxTokens:1024}],
  api:{stream:responses.stream,streamSimple},
 }));
}
