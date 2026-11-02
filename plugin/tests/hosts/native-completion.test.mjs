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
 // Earlier native turn failures must survive a later successful result, too.
 const terminal=record.events.at(-1);
 for(const changed of [{...terminal,is_error:true},{...terminal,subtype:'error_during_execution'}]) {
  assert.throws(()=>assertNativeCompletion(record.host,result([changed,...record.events]),record.answer),error=> {
   assert.equal(error.code,'NATIVE_HOST_MODEL_FAILED');
   assert.deepEqual(error.cause,changed);
   return true;
  });
 }
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
 if(record.host==='codex') {
  const result=sequence=>({...record,stdout:sequence.map(({line})=>line).join('\n')+'\n'});
  const messages=events.filter(({event})=>event.item?.type==='agent_message');
  // Reorder/delete/duplicate original records: an answer must be inside the
  // same native turn, rather than accepted after its successful terminal.
  for(const sequence of [events.filter(({event})=>event.type!=='turn.started'),
   [...events.filter(({event})=>event.item?.type!=='agent_message'),...messages],
   [...events,...events.filter(({event})=>event.type==='turn.completed')]])
   assert.throws(()=>assertNativeCompletion(record.host,result(sequence),record.answer),{code:'NATIVE_HOST_TERMINAL_ORDER'});
 }
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
 const builtin=JSON.parse(await readFile(new URL('./fixtures/native-claude-builtin-plugins.json',import.meta.url),'utf8'));
 for(const original of records)for(const plugins of [[],builtin.plugins]) {
  // Keep the original native terminals and project the real Windows native
  // plugin inventory into the original bare/installed initialization records.
  const withPlugins=added=>({...original,stdout:original.stdout.trim().split('\n').map(line=>{
   const event=JSON.parse(line);
   if(event.type==='system'&&event.subtype==='init')event.plugins.push(...added);
   return JSON.stringify(event);
  }).join('\n')+'\n'});
  const record=withPlugins(plugins);
  const selection={model:'claude-haiku-5-5',installed:record.mode==='installed'};
  assert.equal(assertNativeCompletion(record.host,record,record.answer,selection),plugins.length?3:0);
  assert.throws(()=>assertNativeCompletion(record.host,record,record.answer,{...selection,model:'unapproved'}),/model differs/);
  assert.throws(()=>assertNativeCompletion(record.host,record,record.answer,{...selection,installed:!selection.installed}),/plugin set differs/);
  const lines=record.stdout.trim().split('\n');
  const missing={...record,stdout:lines.filter(line=>JSON.parse(line).type!=='system').join('\n')+'\n'};
  assert.throws(()=>assertNativeCompletion(record.host,missing,record.answer,selection),/initialization missing/);
  if(selection.installed) {
   const namespace={...record,stdout:record.stdout.replace('plugin:acyclic:acyclic-sessions','acyclic-sessions')};
   assert.throws(()=>assertNativeCompletion(record.host,namespace,record.answer,selection),/MCP connections differ/);
  }
  const changedResult=change=>({...record,stdout:lines.map(line=>{
   const event=JSON.parse(line);return event.type==='result'?JSON.stringify({...event,...change}):line;
  }).join('\n')+'\n'});
  // Corrupt the original terminal identity, keeping the answer and exit zero.
  for(const change of [{session_id:'foreign-session'},{parent_tool_use_id:'paired_shell'}])
   assert.throws(()=>assertNativeCompletion(record.host,changedResult(change),record.answer,selection),{code:'NATIVE_HOST_SESSION_MISMATCH'});
  const terminal=lines.find(line=>JSON.parse(line).type==='result');
  assert.throws(()=>assertNativeCompletion(record.host,{...record,stdout:record.stdout+terminal+'\n'},record.answer,selection),{code:'NATIVE_HOST_TERMINAL_ORDER'});
  for(const changed of [{...builtin.plugins[0],path:'installed'},
   {...builtin.plugins[0],source:'agents-md@user'}, {...builtin.plugins[0],name:'unrelated'},
   ...builtin.plugins]) {
   const contaminated=withPlugins([...builtin.plugins,changed]);
   assert.throws(()=>assertNativeCompletion(record.host,contaminated,record.answer,selection),/plugin/);
  }
 }
});
