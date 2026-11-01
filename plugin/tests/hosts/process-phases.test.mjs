import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import test from 'node:test';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {ProcessPhases,pairOrders,pairPhases} from './process-phases.mjs';

test('phase pairs retain original real pipe terminals and reject incomplete or foreign samples',async()=>{
 let collector;
 const scope=new Processes({onProgress:event=>collector?.observe(event)});
 try {
  assert.throws(()=>pairOrders(0).next(),/admit native pair count/);
  assert.throws(()=>pairOrders(NaN).next(),/admit native pair count/);
  let preceding;
  for(const {index,order} of pairOrders(2)) {
   if(index===1)assert.deepEqual(order,[...preceding].reverse(),'adjacent pairs counterbalance order');
   preceding=order;
   const samples=[];
   for(const mode of order) {
    const operation=randomUUID(),observed=new ProcessPhases(operation),missing=new ProcessPhases(operation);
    collector={observe(event){observed.observe(event);if(event.phase!=='stdout-eof')missing.observe(event);}};
    const result=await scope.run(process.execPath,['-e','process.stdin.pipe(process.stdout)'],{scopeOperation:operation},'original exact bytes');
    assert.equal(result.stdout,'original exact bytes');
    assert.throws(()=>missing.complete(result),/missing original phase: stdout-eof/);
    samples.push({mode,binding:{source:'same admitted source'},effect:result.stdout,physical:observed.complete(result)});
   }
   const pair=pairPhases(...samples);
   assert.deepEqual(pair.launch_order,order);
   assert.deepEqual(pair.raw.map(sample=>sample.mode),order,'raw samples retain physical launch order');
   const bare=samples.find(sample=>sample.mode==='bare'),installed=samples.find(sample=>sample.mode==='installed');
   assert.equal(pair.launch_to_closed_delta_seconds,
    installed.physical.intervals.launch_to_closed-bare.physical.intervals.launch_to_closed);
   assert.equal(pair.raw.length,2);assert.equal(pair.warm_turn_pairs,0);
   assert.equal(pair.raw[0].physical.native_tree_retirement,null);
   assert.throws(()=>pairPhases(samples[0],{...samples[1],binding:{source:'foreign source'}}),/original source/);
   assert.throws(()=>pairPhases(samples[0],{...samples[1],physical:samples[0].physical}),/distinct original operations/);
   assert.throws(()=>pairPhases(samples[0],{...samples[1],effect:'wrong bytes'}),/exact bytes/);
   assert.throws(()=>pairPhases(samples[0],{...samples[1],mode:samples[0].mode}),/one bare and one installed/);
  }
 } finally {await scope.close();}
});
