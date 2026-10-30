// Pinned Pi1.1.0's Responses implementation refuses a request without a
// credential (getClientApiKey), and the OpenAI client it builds always sends
// one as Authorization. Keep Pi's own Responses implementation and model loop;
// hand it an in-process placeholder, and remove that header at the fetch
// boundary so the localhost mock never receives a credential header.
import assert from 'node:assert/strict';
import {createProvider} from '@earendil-works/pi-ai/models';
import * as responses from '@earendil-works/pi-ai/api/openai-responses';

const keyless=(input,init)=>{
 const headers=new Headers(init?.headers);
 headers.delete('authorization');
 return fetch(input,{...init,headers});
};
const options=options=>({...options,apiKey:'keyless',fetch:keyless});

export default function register(pi) {
 const baseUrl=process.env.ACYCLIC_E2E_MODEL_URL;
 assert.ok(baseUrl,'step=configure keyless Pi provider; recovery=supply the shipping mock endpoint through the native-model driver');
 const endpoint=new URL(baseUrl);
 assert.equal(endpoint.protocol,'http:');assert.equal(endpoint.hostname,'127.0.0.1');
 pi.registerProvider(createProvider({
  id:'fixture',name:'Shipping mock on localhost',baseUrl,
  auth:{apiKey:{name:'Keyless localhost',resolve:async()=>({auth:{headers:{}},source:'keyless localhost'})}},
  models:[{id:'tiny',name:'Deterministic acceptance',provider:'fixture',api:'openai-responses',baseUrl,
   reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:32768,maxTokens:1024}],
  api:{stream:(model,context,given)=>responses.stream(model,context,options(given)),
   streamSimple:(model,context,given)=>responses.streamSimple(model,context,options(given))},
 }));
}
