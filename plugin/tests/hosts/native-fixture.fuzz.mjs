import assert from 'node:assert/strict';
import {nativeFixture,nativeProvider} from './native-host.mjs';

// Exercise the shipped admission with arbitrary JSON, including wrong shapes.
// The oracle describes the authority admitted, independently of error wording.
export function fuzz(data,decode=nativeFixture) {
 let input;
 try {input=JSON.parse(Buffer.from(data).toString('utf8'));}catch {return;}
 const url=input?.url,credential=input?.credential;
 let expected;
 if(credential!==undefined) {
  try {
   const endpoint=new URL(url);
   expected=typeof credential==='string'&&credential.length===64&&
    [...credential].every(character=>'0123456789abcdef'.includes(character))&&
    endpoint.origin===`http://127.0.0.1:${endpoint.port}`&&endpoint.port!==''&&
    endpoint.username===''&&endpoint.password===''&&endpoint.search===''&&endpoint.hash===''&&
    endpoint.pathname.split('/').length===3&&endpoint.pathname.split('/')[1].startsWith('native-')&&
    endpoint.pathname.split('/')[1].length>7&&endpoint.pathname.endsWith('/v1');
  }catch {expected=false;}
 }
 let result,error;
 try {result=decode(url,credential);}catch(cause){error=cause;}
 if(credential===undefined) {assert.equal(error,undefined);assert.equal(result,undefined);}
 else if(expected) {assert.equal(error,undefined);assert.deepEqual(result,{baseUrl:new URL(url).href,credential});}
 else {assert.ok(error,'untrusted fixture configuration was admitted');assert.match(error.message,/step=admit/);}
}

// The installed package descriptor is trusted; live configuration is external.
// Invalid configuration must refuse at admission, before a host is launched.
export function fuzzProvider(data,native,decode=nativeProvider) {
 let input;
 try {input=JSON.parse(Buffer.from(data).toString('utf8'));}catch {return;}
 try {decode(native,input);}
 catch(error) {
  assert.ok(error.code==='ERR_ASSERTION'||error.code==='UNSUPPORTED_NATIVE_PROVIDER',
   'native provider escaped its admission boundary: '+error.stack);
 }
}
