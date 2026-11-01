import assert from 'node:assert/strict';
import {randomInt} from 'node:crypto';

// Counterbalance each adjacent pair of samples. Randomize its first order,
// retain only that bit, and emit the reverse order next. Count is the caller's
// explicit experiment size; no schedule or completed history is materialized.
export function* pairOrders(count) {
 assert.ok(Number.isSafeInteger(count)&&count>0,
  'step=admit native pair count; recovery=supply a positive integer PAIRS');
 let first;
 for(let index=0;index<count;index++) {
  if(index%2===0)first=randomInt(2);
  yield {index,order:(first^(index%2))?['installed','bare']:['bare','installed']};
 }
}

const phases=new Set(['node-spawn','stdin-write-admitted','stdin-write-complete','stdin-write-failed',
 'stdin-end-submitted','stdin-finish','stdin-close','stdout-first-write','stderr-first-write',
 'stdout-eof','stderr-eof','stdout-close','stderr-close','stdout-finish','stderr-finish',
 'node-exit','node-close','reaped','closed']);
const required=['node-spawn','stdin-write-admitted','stdin-write-complete','stdin-end-submitted',
 'stdin-finish','stdout-first-write','stdout-eof','stderr-eof','stdout-finish','stderr-finish',
 'node-exit','node-close','reaped','closed'];

// A fixed number of original observations for ONE directly launched process.
// These are independent boundaries, not an assumed stdin/EOF/output/exit order.
// In particular, transcript write completion is not pipe arrival, stdin finish
// is not child consumption, and Node close is not native tree retirement.
export class ProcessPhases {
 #operation; #ordinal; #report; #times=Object.create(null); #exit;
 constructor(operation) {this.#operation=operation;}
 observe(event) {
  if(event.operation!==this.#operation)return;
  assert.equal(event.schema,'acyclic.process-progress.v1');
  this.#ordinal??=event.ordinal;
  assert.equal(event.ordinal,this.#ordinal,'one original launch per sample');
  const report={generation:event.report_generation,request_id:event.report_request_id};
  this.#report??=report;assert.deepEqual(report,this.#report,'original selected report binding changed');
  assert.ok(phases.has(event.phase),'unknown physical phase');
  assert.ok(!Object.hasOwn(this.#times,event.phase),'duplicate physical phase');
  assert.ok(Number.isFinite(event.elapsed_seconds)&&event.elapsed_seconds>=0);
  this.#times[event.phase]=event.elapsed_seconds;
  if(event.phase==='node-exit')this.#exit={code:event.code,signal:event.signal};
 }
 snapshot() {
  return {schema:'acyclic.direct-process-phases.v1',operation:this.#operation,ordinal:this.#ordinal,report:this.#report,
   clock:'node-launch-relative-seconds',offsets:{...this.#times},exit:this.#exit,
   child_consumption:null,pipe_read_arrival:null,native_tree_retirement:null,warm_turn:null};
 }
 complete(result) {
  for(const phase of required)assert.ok(Object.hasOwn(this.#times,phase),'missing original phase: '+phase);
  assert.ok(!Object.hasOwn(this.#times,'stdin-write-failed'),'original input write failed');
  assert.deepEqual(this.#exit,{code:result.code,signal:result.signal});
  assert.equal(result.code,0,'native host must complete its result');
  assert.equal(result.signal,null);
  const t=this.#times;
  assert.ok(t['node-exit']<=t['node-close']&&t['node-close']<=t.closed);
  // Keep raw offsets so overlapping EOF/exit and async file-write completion
  // cannot be turned into invented, sequential native execution intervals.
  return {...this.snapshot(),
   intervals:{launch_to_spawn:t['node-spawn'],
    input_pipe_acceptance:t['stdin-write-complete']-t['stdin-write-admitted'],
    exit_to_closed:t.closed-t['node-exit'],launch_to_closed:t.closed}};
 }
}

export function pairPhases(first,second) {
 assert.ok((first.mode==='bare'&&second.mode==='installed')||
  (first.mode==='installed'&&second.mode==='bare'),'pair needs one bare and one installed leg');
 const bare=first.mode==='bare'?first:second,installed=first.mode==='installed'?first:second;
 assert.deepEqual(bare.binding,installed.binding,'pair must retain original source, host, profile, policy and effect');
 assert.notEqual(bare.physical.operation,installed.physical.operation,'pair needs distinct original operations');
 assert.notEqual(bare.physical.ordinal,installed.physical.ordinal,'pair needs distinct original launches');
 assert.equal(bare.effect,installed.effect,'both native shells must produce the same exact bytes');
 return {schema:'acyclic.native-process-pair.v2',binding:bare.binding,
  launch_order:[first.mode,second.mode],raw:[first,second],
  launch_to_closed_delta_seconds:installed.physical.intervals.launch_to_closed-bare.physical.intervals.launch_to_closed,
  warm_turn_pairs:0};
}
