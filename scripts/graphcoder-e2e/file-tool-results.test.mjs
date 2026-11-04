import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {modelInputs,requests} from './assertions.mjs';

// Captured shipping results retain their source identity and digest in the fixture.
const captured=JSON.parse(readFileSync(new URL('./fixtures/file-tool-results.json',import.meta.url),'utf8'));
for(const [adapter,fixture] of Object.entries(captured)) {
 test(`${adapter} file acceptance observes exact checkout publication and read bytes`,()=>{
  // This capture predates opaque versions. Keep its original public contract
  // pinned here rather than assigning it a changed-source workflow pass.
  const selected={id:fixture.case,assert:{requests:4,steps:[0,1,2,3],files:{'result.txt':'after edit\n'},tool_results:[
   {file:{descriptor:{byte_length:12},volume:{class:'project'}}},
   {file:{descriptor:{byte_length:11},volume:{class:'project'}}},
   'after edit\n',
  ]}};
  requests(selected,fixture.records);
  for(const wrong of ['after edit','before edit\n',{'file':{'descriptor':{'byte_length':13}}},{'file':{'volume':{'class':'agent_private'}}}]) {
   const negative={...selected,assert:{...selected.assert,tool_results:[wrong]}};
   assert.throws(()=>requests(negative,fixture.records),/actual tool result missing exact value/);
  }
  const omitted=fixture.records.map(record=>({...record,body:{}}));
  assert.throws(()=>requests(selected,omitted),/actual tool result missing exact value/);
  assert.deepEqual(selected.assert.files,{'result.txt':'after edit\n'});
 });
 test(`${adapter} model inputs exclude observer metadata and encode each original body once`,()=>{
  let encodings=0;
  const records=fixture.records.map(record=>{
   const body=structuredClone(record.body);
   Object.defineProperty(body,'toJSON',{value(){encodings++;return this;}});
   return {...record,body,observer:fixture.original_capture_sha256};
  });
  const expected={model_contains:['byte_length'],last_model_contains:['byte_length'],
   model_absent:[fixture.original_capture_sha256],each_model_absent:[fixture.original_capture_sha256]};
  modelInputs({},records);
  assert.equal(encodings,0,'no input query needs no encoding');
  modelInputs({last_model_contains:['byte_length']},records);
  assert.equal(encodings,1,'a final-input query needs only its original body');
  encodings=0;
  modelInputs(expected,records);
  assert.equal(encodings,records.length,'one body encoding serves every input assertion');
  assert.throws(()=>modelInputs({model_contains:[fixture.original_capture_sha256]},records),/model wire missing/);
  assert.throws(()=>modelInputs(expected,records.map(record=>({...record,body:{}}))),/model wire missing/);
 });
}
