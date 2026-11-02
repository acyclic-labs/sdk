import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {assertNativeCompletion} from './native-model.mjs';

// Seeds are the retained real native output, never an invented provider reply.
export const seeds=['native-completion.json','native-pair-initialization.json',
 'native-codex-tool-terminals.json','native-claude-tool-terminals.json'].flatMap(name=> {
 const captured=JSON.parse(readFileSync(new URL('./fixtures/'+name,import.meta.url),'utf8'));
 return (captured.records??[captured]).map(record=>({...record,
  stdout:record.stdout??record.events.map(event=>JSON.stringify(event)).join('\n'),
  selection:name==='native-pair-initialization.json'?{model:'claude-haiku-5-5',installed:record.mode==='installed'}:undefined}));
});

// Mutations cover every nested field of the original records. The oracle uses
// the production decoder: invalid external shapes must never escape as an
// accidental property-access TypeError or another unclassified exception.
export function check(record,stdout) {
 try {assertNativeCompletion(record.host,{...record,stdout},record.answer,record.selection);}
 catch(error) {
  assert.ok(error.code==='ERR_ASSERTION'||error.code?.startsWith('NATIVE_HOST_'),
   'native decoder escaped its refusal boundary: '+error.stack);
 }
}

export function paths(value,prefix=[]) {
 const result=[prefix];
 if(value&&typeof value==='object')for(const key of Object.keys(value))result.push(...paths(value[key],[...prefix,key]));
 return result;
}

export function replace(value,path,replacement) {
 if(!path.length)return replacement;
 let parent=value;
 for(const key of path.slice(0,-1))parent=parent[key];
 parent[path.at(-1)]=replacement;
 return value;
}

export function fuzz(data) {
 const input=Buffer.from(data),record=seeds[(input[0]??0)%seeds.length];
 const lines=record.stdout.trim().split('\n'),index=(input[1]??0)%lines.length;
 const event=JSON.parse(lines[index]),fields=paths(event);
 let replacement;
 const bytes=input.subarray(3).toString('utf8');
 try {replacement=JSON.parse(bytes);}catch {replacement=bytes;}
 lines[index]=JSON.stringify(replace(event,fields[(input[2]??0)%fields.length],replacement));
 check(record,lines.join('\n'));
 for(const host of ['codex','claude-code','pi'])check({...record,host,selection:undefined},input.toString('utf8'));
}
