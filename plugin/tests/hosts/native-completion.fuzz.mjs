import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {assertNativeCompletion} from './native-model.mjs';
import {assertCodexPairTools} from './native-pairs.mjs';

const namespace=JSON.parse(readFileSync(new URL('./fixtures/native-codex-namespace.json',import.meta.url),'utf8')).tool;
const answers=JSON.parse(readFileSync(new URL('./fixtures/native-completion.json',import.meta.url),'utf8')).records;

// Change only the authoritative answer of an original successful capture.
// The same production admission must reject every byte difference.
export function checkAnswer(record,answer) {
 const events=record.stdout.trim().split('\n').map(JSON.parse);
 for(const event of events) {
  if(record.host==='codex'&&event.type==='item.completed'&&event.item?.type==='agent_message')event.item.text=answer;
  if(record.host==='claude-code'&&event.type==='result')event.result=answer;
 }
 const changed={...record,stdout:events.map(event=>JSON.stringify(event)).join('\n')+'\n'};
 if(answer===record.answer)assertNativeCompletion(record.host,changed,record.answer);
 else assert.throws(()=>assertNativeCompletion(record.host,changed,record.answer),/native answer missing/);
}

// Seeds are the retained real native output, never an invented provider reply.
export const seeds=['native-completion.json','native-pair-initialization.json',
 'native-codex-tool-terminals.json','native-claude-tool-terminals.json',
 'native-claude-workflow-refusal.json'].flatMap(name=> {
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
 checkAnswer(answers[(input[0]??0)%answers.length],bytes);
 try {replacement=JSON.parse(bytes);}catch {replacement=bytes;}
 lines[index]=JSON.stringify(replace(event,fields[(input[2]??0)%fields.length],replacement));
 check(record,lines.join('\n'));
 const declaration=structuredClone(namespace),declarationFields=paths(declaration);
 const changed=replace(declaration,declarationFields[(input[2]??0)%declarationFields.length],replacement);
 try {assertCodexPairTools([{body:{tools:[changed]}}],Boolean((input[0]??0)&1));}
 catch(error) {assert.equal(error.code,'ERR_ASSERTION','native declaration escaped its admission boundary');}
 for(const host of ['codex','claude-code','pi'])check({...record,host,selection:undefined},input.toString('utf8'));
}
