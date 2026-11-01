import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
import {assertNativeCompletion} from './native-model.mjs';

// Real pinned-host output, with event deletion as the missing-terminal control.
const {records}=JSON.parse(await readFile(new URL('./fixtures/native-completion.json',import.meta.url),'utf8'));
for(const record of records)test(`${record.host} requires its original successful native terminal`,()=>{
 assertNativeCompletion(record.host,record,record.answer);
 const events=record.stdout.trim().split('\n').map(line=>({line,event:JSON.parse(line)}));
 const terminal=record.host==='codex'?'turn.completed':'result';
 const missing={...record,stdout:events.filter(({event})=>event.type!==terminal).map(({line})=>line).join('\n')+'\n'};
 assert.ok(missing.stdout.includes(record.answer),'original assistant output still contains the answer');
 assert.throws(()=>assertNativeCompletion(record.host,missing,record.answer),/native .* terminal missing/);
 assert.throws(()=>assertNativeCompletion(record.host,record,'UNOBSERVED_NATIVE_ANSWER'),/native answer missing/);
 assert.throws(()=>assertNativeCompletion(record.host,{...record,code:1},record.answer));
});

test('Claude native pair initialization fences model namespace and bare contamination',async()=>{
 const {records}=JSON.parse(await readFile(new URL('./fixtures/native-pair-initialization.json',import.meta.url),'utf8'));
 for(const record of records) {
  const selection={model:'claude-haiku-5-5',installed:record.mode==='installed'};
  assertNativeCompletion(record.host,record,record.answer,selection);
  assert.throws(()=>assertNativeCompletion(record.host,record,record.answer,{...selection,model:'unapproved'}),/model differs/);
  assert.throws(()=>assertNativeCompletion(record.host,record,record.answer,{...selection,installed:!selection.installed}),/plugin set differs/);
  const lines=record.stdout.trim().split('\n');
  const missing={...record,stdout:lines.filter(line=>JSON.parse(line).type!=='system').join('\n')+'\n'};
  assert.throws(()=>assertNativeCompletion(record.host,missing,record.answer,selection),/initialization missing/);
  if(selection.installed) {
   const namespace={...record,stdout:record.stdout.replace('plugin:acyclic:acyclic-sessions','acyclic-sessions')};
   assert.throws(()=>assertNativeCompletion(record.host,namespace,record.answer,selection),/MCP connections differ/);
  }
 }
});
