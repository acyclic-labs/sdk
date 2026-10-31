import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {requests} from './assertions.mjs';

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
}
