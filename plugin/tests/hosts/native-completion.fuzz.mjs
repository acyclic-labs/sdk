import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {assertNativeCompletion} from './native-model.mjs';
import {assertCodexPairTools} from './native-pairs.mjs';

const namespace=JSON.parse(readFileSync(new URL('./fixtures/native-codex-namespace.json',import.meta.url),'utf8')).tool;

const answers=JSON.parse(readFileSync(new URL('./fixtures/native-completion.json',import.meta.url),'utf8')).records;

// Change only the authoritative answer of an original successful capture.
// The same production admission must reject every byte difference.
export function checkAnswer(record,answer,decode=assertNativeCompletion) {
 const events=record.stdout.trim().split('\n').map(JSON.parse);
 for(const event of events) {
  if(record.host==='codex'&&event.type==='item.completed'&&event.item?.type==='agent_message')event.item.text=answer;
  if(record.host==='claude-code'&&event.type==='result')event.result=answer;
 }
 const changed={...record,stdout:events.map(event=>JSON.stringify(event)).join('\n')+'\n'};
 if(answer===record.answer)decode(record.host,changed,record.answer);
 else assert.throws(()=>decode(record.host,changed,record.answer),/native answer missing/);
}

// Seeds are the retained real native output, never an invented provider reply.
export const seeds=['native-completion.json','native-pair-initialization.json',
 'native-codex-tool-terminals.json','native-claude-tool-terminals.json',
 'native-claude-workflow-refusal.json','native-pi-refusal.json','native-codex-refusal.json'].flatMap(name=> {
 const captured=JSON.parse(readFileSync(new URL('./fixtures/'+name,import.meta.url),'utf8'));
 return (captured.records??[captured]).map(record=>({code:0,...record,
  stdout:record.stdout??record.events.map(event=>JSON.stringify(event)).join('\n'),
  selection:name==='native-pair-initialization.json'?{model:'claude-haiku-5-5',installed:record.mode==='installed'}:undefined}));
});

// Mutations cover every nested field of the original records. The oracle uses
// the production decoder: invalid external shapes must never escape as an
// accidental property-access TypeError or another unclassified exception.
export function check(record,stdout,decode=assertNativeCompletion) {
 try {decode(record.host,{...record,stdout},record.answer,record.selection);}
 catch(error) {
  assert.ok(error.code==='ERR_ASSERTION'||error.code?.startsWith('NATIVE_HOST_'),
   'native decoder escaped its refusal boundary: '+error.stack);
 }
}

// Original failures must retain the complete native cause even when exit zero
// and a later parent terminal would otherwise succeed. No parallel decoder.
export function checkRefusals(decode=assertNativeCompletion) {
 for(const record of seeds) {
  const events=record.stdout.trim().split('\n').map(JSON.parse);
  if(record.host==='claude-code') {
   const terminal=events.findLast(event=>event.type==='result');
   for(const event of events)for(const part of event.message?.content??[]) {
    if(part.type!=='tool_result'||part.is_error!==true)continue;
    const stdout=[event,terminal].map(value=>JSON.stringify(value)).join('\n');
    assert.throws(()=>decode(record.host,{...record,stdout},terminal.result),error=>{
     assert.equal(error.code,'NATIVE_HOST_TOOL_FAILED');
     assert.deepEqual(error.cause,part);return true;
    });
   }
  } else if(record.host==='codex') {
   for(const failed of events.filter(event=>event.type==='error'||event.type==='turn.failed'))
    assert.throws(()=>decode(record.host,{...record,stdout:JSON.stringify(failed)},''),error=>{
     assert.equal(error.code,'NATIVE_HOST_MODEL_FAILED');
     assert.deepEqual(error.cause,failed);return true;
    });
  } else if(record.host==='pi') {
   const failed=events.find(event=>event.type==='message_end'&&event.message?.stopReason==='error');
   assert.ok(failed,'original Pi failure corpus lacks its native terminal');
   assert.throws(()=>decode(record.host,record,''),error=>{
    assert.equal(error.code,'NATIVE_HOST_MODEL_FAILED');
    assert.deepEqual(error.cause,failed.message);return true;
   });
  }
 }
}

// Reorder or omit original events rather than authoring a second host trace.
export function checkTransitions(decode=assertNativeCompletion) {
 const warning=seeds.flatMap(record=>record.host==='codex'?record.stdout.trim().split('\n').map(JSON.parse):[])
  .find(event=>event.type==='item.completed'&&event.item?.type==='error');
 assert.ok(warning,'original native metadata warning is missing');
 for(const record of [...answers,...seeds.filter(record=>record.selection)]) {
  const events=record.stdout.trim().split('\n').map(JSON.parse);
  const run=changed=>decode(record.host,{...record,stdout:changed.map(event=>JSON.stringify(event)).join('\n')},record.answer,record.selection);
  if(record.host==='codex'&&record.answer) {
   const start=events.find(event=>event.type==='turn.started');
   if(!start)continue;
   run([warning,...events]);
   assert.throws(()=>run(events.filter(event=>event!==start)),{code:'NATIVE_HOST_TERMINAL_ORDER'});
   const index=events.indexOf(start);
   assert.throws(()=>run([...events.slice(0,index),start,...events.slice(index)]),{code:'NATIVE_HOST_TERMINAL_ORDER'});
  } else if(record.selection) {
   const terminal=events.find(event=>event.type==='result');
   for(const changed of [{...terminal,session_id:terminal.session_id+'0'},
    {...terminal,parent_tool_use_id:terminal.session_id}])
    assert.throws(()=>run(events.map(event=>event===terminal?changed:event)),{code:'NATIVE_HOST_SESSION_MISMATCH'});
   assert.throws(()=>run([...events,terminal]),{code:'NATIVE_HOST_TERMINAL_ORDER'});
  }
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
