import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
import {check,checkAnswer,checkRefusals,checkTransitions,fuzz,paths,replace,seeds} from './native-completion.fuzz.mjs';

for(const record of seeds)test(record.host+' native decoder contains generated shape failures ('+(record.mode??'tools')+')',()=> {
 const lines=record.stdout.trim().split('\n');
 for(let index=0;index<lines.length;index++) {
  const original=JSON.parse(lines[index]);
  for(const path of paths(original))for(const value of [null,false,0,'',[],{},[null]]) {
   const changed=lines.slice();
   changed[index]=JSON.stringify(replace(structuredClone(original),path,value));
   check(record,changed.join('\n'));
  }
 }
});

test('byte fuzz entry retains the original decoder and raw JSON refusal',()=> {
 for(let seed=0;seed<seeds.length;seed++)for(const value of ['null','[]','{}','false','0','""','[null]','{'])
  fuzz(Buffer.concat([Buffer.from([seed,2,0]),Buffer.from(value)]));
});

test('recorded refusal and answer oracles reject planted production decoder defects',async()=>{
 checkRefusals();
 checkTransitions();
 const url=new URL('./native-model.mjs',import.meta.url);
 const source=(await readFile(url,'utf8')).replace(/from '([^']+)'/g,(match,path)=>
  path.startsWith('.')?`from '${new URL(path,url).href}'`:match);
 const answer=seeds.find(record=>record.host==='codex');
 for(const [before,after,oracle] of [
  ['text===answer','text.includes(answer)',decode=>checkAnswer(answer,'0'+answer.answer,decode)],
  ["part.type==='tool_result'&&part.is_error===true",'false',checkRefusals],
  ["if(message.stopReason==='error')","if(false)",checkRefusals],
  ["event.type==='turn.failed'||event.type==='error'",'false',checkRefusals],
  ["nativeObject(host,event,'native event');",'',decode=>check(answer,'null',decode)],
  ["&&turn!=='active'",'&&false',checkTransitions],
  ['event.session_id!==session','false',checkTransitions],
 ]) {
  assert.ok(source.includes(before),'planted defect no longer matches production source');
  const mutant=await import('data:text/javascript;base64,'+Buffer.from(source.replace(before,after)).toString('base64'));
  assert.throws(()=>oracle(mutant.assertNativeCompletion),{code:'ERR_ASSERTION'});
 }
});
