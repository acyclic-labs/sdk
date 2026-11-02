import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {nativeProvider,parseNativeProxy} from './native-host.mjs';

const {dependencies}=JSON.parse(readFileSync(new URL('./package.json',import.meta.url),'utf8'));
const hosts=Object.entries(dependencies).filter(([name])=>['@openai/codex','@anthropic-ai/claude-code','@earendil-works/pi-coding-agent'].includes(name));
export function fuzz(data) {
 const text=Buffer.from(data).toString('utf8');
 const check=operation=> {
  try {operation();}
  catch(error) {assert.ok(error.code==='ERR_ASSERTION'||error.message?.startsWith('step='),error.stack);}
 };
 check(()=>parseNativeProxy(text));
 let live;
 try {live=JSON.parse(text);}catch {return;}
 for(const [name,version] of hosts)check(()=>nativeProvider({package:{name,version}},live));
}
