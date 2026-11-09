// Byte-directed programs over the installed MCP adapter, using captured native
// completions. Each child's dependency chain survives arbitrary interleaving.
import assert from 'node:assert/strict';

export const concurrentCorpus=['00','01','0eaa80'].map(hex=>Buffer.from(hex,'hex'));
export function program(input){
 const data=input.length?input:Buffer.from([0]),flags=data[0];
 const chains=['left','right'].map(side=>[
  ['spawn',side,`${side}:0`],
  ...Array.from({length:flags%3},(_,index)=>['send',side,`${side}:${index+1}`]),
  ...(flags&8?[['spawn',side,`${side}:0`]]:[]),
  ...Array.from({length:1+((flags>>2)&1)},()=>['wait',side,undefined]),
 ]);
 const calls=[],length=chains[0].length+chains[1].length;
 for(let bit=0;bit<length;bit++){
  let side=(data[(1+Math.floor(bit/8))%data.length]>>(bit%8))&1;
  if(!chains[side].length)side^=1;
  calls.push(chains[side].shift());
 }
 return calls;
}

export function verify(calls,replies){
 assert.equal(replies.length,calls.length,'an admitted call lost its reply');
 const children=new Map();
 for(const [index,[tool,reference]] of calls.entries()){
  const row=replies[index];
  assert.ok(row&&typeof row==='object'&&!Array.isArray(row),'tool result is not an object');
  if(!children.has(reference)){
   assert.equal(tool,'spawn','a child was used before spawn');
   assert.equal(typeof row.task,'string','spawn lost its task identity');
   assert.equal(typeof row.cwd,'string','spawn lost its physical view');
   for(const other of children.values()){
    assert.notEqual(row.task,other.task,'distinct children alias');
    assert.notEqual(row.cwd,other.cwd,'distinct children alias');
   }
   children.set(reference,{task:row.task,cwd:row.cwd,accepted:[]});
  }
  const child=children.get(reference);
  assert.equal(row.task,child.task,"reply changed its child's task");
  if(tool==='spawn')assert.equal(row.cwd,child.cwd,'spawn replay changed its physical view');
  if(tool==='spawn'||tool==='send'){
   const accepted={id:row.id,inbox:row.inbox};
   assert.equal(typeof accepted.id,'string','acceptance lost its prompt identity');
   assert.ok(Number.isSafeInteger(accepted.inbox),'acceptance lost its inbox');
   if(tool==='spawn'&&child.accepted.length)assert.deepEqual(accepted,child.accepted[0],'spawn replay changed its prompt');
   else{
    for(const old of child.accepted){
     assert.notEqual(accepted.id,old.id,'accepted prompts alias');
     assert.ok(accepted.inbox>old.inbox,'accepted prompts reorder');
    }
    child.accepted.push(accepted);
   }
  }else{
   assert.equal(row.reply?.kind,'answered','wait returned before settlement');
   assert.ok(Object.hasOwn(row.reply.outcome??{},'Succeeded'),'a generated input failed to complete');
   assert.equal(row.status?.settled,true,'wait lost settlement');
   assert.equal(row.status.turn,child.accepted.length,'wait lost, duplicated or skipped an accepted turn');
   if(child.wait)assert.deepEqual(row.reply,child.wait,'concurrent waits disagree on the same cut');
   child.wait=row.reply;
  }
 }
 assert.equal(children.size,2,'program did not join both original children');
 assert.ok([...children.values()].every(child=>child.wait),'program did not join both original children');
}

export async function fuzz(data,tool){
 const calls=program(data);
 // Attach rejection consumers at admission, including reverse-read schedules.
 const pending=calls.map(call=>tool(...call).then(value=>({value}),error=>({error})));
 const replies=Array(calls.length),order=Array.from(calls.keys());
 if(data.at(-1)&128)order.reverse();
 for(const index of order){
  const result=await pending[index];if(result.error)throw result.error;
  replies[index]=result.value;
 }
 verify(calls,replies);
 const controls=[],spawns=calls.flatMap(([tool],index)=>tool==='spawn'?[index]:[]);
 const second=spawns.find(index=>calls[index][1]!==calls[spawns[0]][1]);
 const aliased=structuredClone(replies);aliased[second].task=replies[spawns[0]].task;controls.push(aliased);
 const seen=new Set();
 for(const index of spawns){
  const reference=calls[index][1];
  if(seen.has(reference)){
   const moved=structuredClone(replies);moved[index].cwd+='/changed-view';controls.push(moved);break;
  }
  seen.add(reference);
 }
 const wait=calls.findIndex(([tool])=>tool==='wait');
 const early=structuredClone(replies);early[wait].reply={kind:'pending'};controls.push(early);
 const skipped=structuredClone(replies);skipped[wait].status.turn++;controls.push(skipped);
 for(const changed of controls)assert.throws(()=>verify(calls,changed),assert.AssertionError,
  'a planted identity, view, progress or turn-count defect escaped');
 return {input:Buffer.from(data).toString('hex'),calls,replies,negative_controls:controls.length};
}
