import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
import {assertNativeCompletion} from './native-model.mjs';

test('Claude failed tool results survive a later successful parent terminal', async()=> {
 const record=JSON.parse(await readFile(new URL('./fixtures/native-claude-tool-terminals.json',import.meta.url),'utf8'));
 const result=events=>({...record,stdout:events.map(event=>JSON.stringify(event)).join('\n')+'\n'});
 assertNativeCompletion(record.host,result(record.events),record.answer);
 for(let index=0;index<record.events.length-1;index++) {
  const failed=structuredClone(record.events);
  const part=failed[index].message.content[0];
  part.is_error=true;
  assert.throws(()=>assertNativeCompletion(record.host,result(failed),record.answer),error=> {
   assert.equal(error.code,'NATIVE_HOST_TOOL_FAILED');
   assert.deepEqual(error.cause,part);
   return true;
  });
 }
 // A successful first result cannot hide a failed sibling in the same message.
 const siblings=structuredClone(record.events);
 siblings[0].message.content.push({...siblings[1].message.content[0],is_error:true});
 assert.throws(()=>assertNativeCompletion(record.host,result(siblings),record.answer),{code:'NATIVE_HOST_TOOL_FAILED'});
});

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

test('Codex tool failure survives a successful parent answer and process exit',async()=>{
 const record=JSON.parse(await readFile(new URL('./fixtures/native-codex-tool-terminals.json',import.meta.url),'utf8'));
 const result=events=>({...record,stdout:events.map(event=>JSON.stringify(event)).join('\n')+'\n'});
 const refused=events=>assert.throws(()=>assertNativeCompletion('codex',result(events),record.answer),error=>{
  assert.equal(error.code,'NATIVE_HOST_TOOL_FAILED');
  assert.match(error.message,/step=validate native codex completion:.*recovery=node --test/);
  return true;
 });
 // Actual failed shell items precede an exit-zero host's successful final
 // answer. Removing the guard admits this very same captured sequence.
 refused(record.events);
 const failures=record.events.filter(event=>event.item?.type==='command_execution'&&event.item.exit_code!==0);
 assert.equal(failures.length,2);
 const success=record.events.filter(event=>!failures.includes(event));
 assertNativeCompletion('codex',result(success),record.answer);
 for(const failure of failures) {
  // Removing the other failure isolates the original merge and finish joins.
  const isolated=record.events.filter(event=>!failures.includes(event)||event===failure);
  refused(isolated);
  for(const changed of [{...failure.item,status:'completed'},{...failure.item,exit_code:0},
   {...failure.item,status:'completed',exit_code:null}])
   refused(isolated.map(event=>event===failure?{...event,item:changed}:event));
 }
 const mcp=success.find(event=>event.item?.type==='mcp_tool_call');
 // Mutations of the captured native MCP terminal: status and error are
 // independent evidence, including an error payload with a falsy value.
 for(const changed of [{...mcp.item,status:'failed'},...[false,0,''].map(error=>({...mcp.item,error}))])
  refused(success.map(event=>event===mcp?{...event,item:changed}:event));
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
