import assert from 'node:assert/strict';
import {test} from 'node:test';
import {materializeActorState} from '../dist/state-binding.js';

// Only the admitted host seam is scripted here. The shared guest projection
// owns staging, byte isolation, transaction lifetimes and publication fencing.
function host(seed = {metadata:[],journalTail:'0'}) {
  return {seed,commits:[],fences:[],
    async commit(request) { this.commits.push(request); },
    async readJournal() { return []; },
    fence(reason) { this.fences.push(reason); },
  };
}

test('state bytes do not alias the seed, inputs, reads or journal pages', async () => {
  const seed = Uint8Array.of(1), page = Uint8Array.of(2);
  const owner = host({metadata:[{key:'seed',value:seed}],journalTail:'1'});
  owner.readJournal = async () => [page];
  const binding = materializeActorState(owner);
  seed[0] = 99;
  const input = Uint8Array.of(3), frame = Uint8Array.of(4);
  binding.transaction(state => {
    assert.deepEqual(state.get('seed'), Uint8Array.of(1));
    state.put('written',input);
    state.appendJournal(1n,[frame]);
    input[0] = 99; frame[0] = 99;
    state.get('written')[0] = 99;
    for (const [,bytes] of state.entries()) bytes[0] = 99;
    assert.deepEqual(state.get('written'),Uint8Array.of(3));
    assert.deepEqual(state.get('seed'),Uint8Array.of(1));
  });
  const frames = await binding.readJournal(0n,1);
  frames[0][0] = 99;
  assert.deepEqual(page,Uint8Array.of(2));
  await binding.flush();
  assert.deepEqual(owner.commits[0].writes,[{key:'written',value:Uint8Array.of(3)}]);
  assert.deepEqual(owner.commits[0].journal,[Uint8Array.of(4)]);
});

test('borrowed handles expire and Promise callbacks cannot publish', async () => {
  const owner = host(), binding = materializeActorState(owner);
  let borrowed, entries;
  binding.transaction(state => {
    borrowed = state;
    state.put('kept',Uint8Array.of(1));
    entries = state.entries();
  });
  for (const use of [
    () => borrowed.get('kept'), () => borrowed.put('late',Uint8Array.of(2)),
    () => entries.next(), () => borrowed.journalTail,
    () => borrowed.appendJournal(0n,[Uint8Array.of(2)]), () => borrowed.alarm(null),
  ]) assert.throws(use,/expired/);
  assert.throws(() => binding.transaction(state => {
    state.put('async',Uint8Array.of(2));
    return Promise.resolve();
  }),/synchronously/);
  await binding.flush();
  assert.deepEqual(owner.commits[0].writes,[{key:'kept',value:Uint8Array.of(1)}]);
});

test('rollback preserves an earlier pending metadata, journal and alarm group', async () => {
  const owner = host(), binding = materializeActorState(owner);
  binding.transaction(state => {
    state.put('kept',Uint8Array.of(1));
    state.appendJournal(0n,[Uint8Array.of(2)]);
    state.alarm(123n);
  });
  const refusal = new Error('callback refused');
  assert.throws(() => binding.transaction(state => {
    state.put('kept',Uint8Array.of(9));
    state.put('discarded',Uint8Array.of(9));
    state.appendJournal(1n,[Uint8Array.of(9)]);
    state.alarm(null);
    throw refusal;
  }),error => error === refusal);
  binding.transaction(state => {
    assert.deepEqual(state.get('kept'),Uint8Array.of(1));
    assert.equal(state.get('discarded'),undefined);
    assert.equal(state.journalTail,1n);
  });
  await binding.flush();
  assert.deepEqual(owner.commits,[{
    writes:[{key:'kept',value:Uint8Array.of(1)}],journalFrom:'0',
    journal:[Uint8Array.of(2)],alarm:{type:'set',deadline:'123'},
  }]);
});

test('flush excludes concurrent calls until its original host commit resolves', async () => {
  const owner = host(), binding = materializeActorState(owner);
  const entered = Promise.withResolvers(), committed = Promise.withResolvers();
  owner.commit = async request => { owner.commits.push(request); entered.resolve(); await committed.promise; };
  binding.transaction(state => state.put('value',Uint8Array.of(1)));
  const flushing = binding.flush();
  await entered.promise;
  assert.throws(() => binding.transaction(() => {}),/fenced/);
  await assert.rejects(binding.flush(),/fenced/);
  await assert.rejects(binding.readJournal(0n,1),/fenced/);
  assert.equal(owner.commits.length,1);
  committed.resolve();
  await flushing;
  binding.transaction(state => assert.deepEqual(state.get('value'),Uint8Array.of(1)));
  assert.deepEqual(owner.fences,[]);
});

test('a rejected commit preserves its cause, fences the binding and never redispatches', async () => {
  const owner = host(), binding = materializeActorState(owner);
  const refusal = new Error('publication outcome unknown');
  owner.commit = async request => { owner.commits.push(request); throw refusal; };
  binding.transaction(state => state.put('value',Uint8Array.of(1)));
  await assert.rejects(binding.flush(),error => error === refusal);
  assert.deepEqual(owner.fences,[refusal]);
  assert.throws(() => binding.transaction(() => {}),/fenced/);
  await assert.rejects(binding.flush(),/fenced/);
  await assert.rejects(binding.readJournal(0n,1),/fenced/);
  assert.equal(owner.commits.length,1);
});
