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
  const owner = host({metadata:[{key:'seed',value:Uint8Array.of(7)}],journalTail:'0'}), binding = materializeActorState(owner);
  binding.transaction(state => {
    state.put('kept',Uint8Array.of(1));
    state.appendJournal(0n,[Uint8Array.of(2)]);
    state.alarm(123n);
  });
  const refusal = new Error('callback refused');
  assert.throws(() => binding.transaction(state => {
    state.put('kept',Uint8Array.of(9));
    state.put('kept',Uint8Array.of(8));
    state.put('discarded',Uint8Array.of(9));
    state.put('seed',Uint8Array.of(9));
    state.appendJournal(1n,[Uint8Array.of(9)]);
    state.alarm(null);
    throw refusal;
  }),error => error === refusal);
  binding.transaction(state => {
    assert.deepEqual(state.get('kept'),Uint8Array.of(1));
    assert.equal(state.get('discarded'),undefined);
    assert.deepEqual(state.get('seed'),Uint8Array.of(7));
    assert.equal(state.journalTail,1n);
  });
  await binding.flush();
  assert.deepEqual(owner.commits,[{
    writes:[{key:'kept',value:Uint8Array.of(1)}],journalFrom:'0',
    journal:[Uint8Array.of(2)],alarm:{type:'set',deadline:'123'},
  }]);
});

test('transaction work excludes unrelated resident keys, pending writes and journal frames', async () => {
  // Count real Map traversal and Array slicing, including copying controls.
  function counted(operation) {
    const iterate = Map.prototype[Symbol.iterator], slice = Array.prototype.slice;
    let entries = 0, frames = 0;
    Map.prototype[Symbol.iterator] = function* () {
      for (const entry of iterate.call(this)) { entries++; yield entry; }
    };
    Array.prototype.slice = function (...args) { frames += this.length; return Reflect.apply(slice,this,args); };
    try { operation(); return {entries,frames}; }
    finally { Map.prototype[Symbol.iterator] = iterate; Array.prototype.slice = slice; }
  }
  for (const size of [1,100000]) {
    const metadata = Array.from({length:size},(_,index) => ({key:`key-${index}`,value:Uint8Array.of(1)}));
    const owner = host({metadata,journalTail:'0'}), binding = materializeActorState(owner);
    const frames = Array.from({length:size},() => Uint8Array.of(3));
    const copied = new Map(metadata.map(({key,value}) => [key,value]));
    assert.deepEqual(counted(() => { new Map(copied); frames.slice(); }),{entries:size,frames:size});
    binding.transaction(state => {
      for (const {key} of metadata) state.put(key,Uint8Array.of(2));
      state.appendJournal(0n,frames);
      state.alarm(123n);
    });
    assert.deepEqual(counted(() => binding.transaction(state => {
      assert.deepEqual(state.get('key-0'),Uint8Array.of(2));
    })),{entries:0,frames:0});
    assert.deepEqual(counted(() => binding.transaction(state => {
      state.put('key-0',Uint8Array.of(4));
      state.appendJournal(BigInt(size),[Uint8Array.of(5)]);
    })),{entries:0,frames:0});
    const refusal = new Error('discard touched work');
    assert.deepEqual(counted(() => assert.throws(() => binding.transaction(state => {
      state.put('key-0',Uint8Array.of(9));
      state.put('key-0',Uint8Array.of(8));
      state.put('discarded',Uint8Array.of(9));
      state.appendJournal(BigInt(size + 1),[Uint8Array.of(9)]);
      state.alarm(null);
      throw refusal;
    }),error => error === refusal)),{entries:0,frames:0});
    binding.transaction(state => {
      assert.deepEqual(state.get('key-0'),Uint8Array.of(4));
      assert.equal(state.get('discarded'),undefined);
      assert.equal(state.journalTail,BigInt(size + 1));
    });
    await binding.flush();
    assert.equal(owner.commits.length,1);
    assert.equal(owner.commits[0].writes.length,size);
    assert.deepEqual(owner.commits[0].writes[0],{key:'key-0',value:Uint8Array.of(4)});
    assert.equal(owner.commits[0].journal.length,size + 1);
    assert.deepEqual(owner.commits[0].journal.at(-1),Uint8Array.of(5));
    assert.deepEqual(owner.commits[0].alarm,{type:'set',deadline:'123'});
  }
});

test('a transaction during an in-flight flush stages the next packet, dispatched only after the first is durable', async () => {
  const owner = host(), binding = materializeActorState(owner);
  const entered = Promise.withResolvers(), committed = Promise.withResolvers();
  owner.commit = async request => { owner.commits.push(request); if (owner.commits.length === 1) { entered.resolve(); await committed.promise; } };
  binding.transaction(state => state.appendJournal(0n,[Uint8Array.of(1)]));
  const first = binding.flush();
  await entered.promise;
  binding.transaction(state => { assert.equal(state.journalTail,1n); state.appendJournal(1n,[Uint8Array.of(2)]); state.put('value',Uint8Array.of(3)); });
  await assert.rejects(binding.readJournal(1n,1),/invalid actor journal page/);
  const second = binding.flush();
  await Promise.resolve();
  assert.equal(owner.commits.length,1);
  committed.resolve();
  await Promise.all([first,second]);
  assert.deepEqual(owner.commits.map(commit => commit.journalFrom),['0','1']);
  assert.deepEqual(owner.commits[1].writes,[{key:'value',value:Uint8Array.of(3)}]);
  assert.deepEqual(owner.fences,[]);
});

test('a rejected commit preserves its cause, fences the binding and never redispatches', async () => {
  const owner = host(), binding = materializeActorState(owner);
  const refusal = new Error('publication outcome unknown');
  const entered = Promise.withResolvers(), completed = Promise.withResolvers();
  owner.commit = async request => { owner.commits.push(request); entered.resolve(); await completed.promise; };
  binding.transaction(state => state.put('value',Uint8Array.of(1)));
  const first = binding.flush();
  await entered.promise;
  binding.transaction(state => state.put('value',Uint8Array.of(2)));
  const second = binding.flush();
  const rejected = Promise.all([first,second].map(packet => assert.rejects(packet,error => error === refusal)));
  completed.reject(refusal);
  await rejected;
  assert.deepEqual(owner.fences,[refusal]);
  assert.throws(() => binding.transaction(() => {}),/fenced/);
  await assert.rejects(binding.flush(),/fenced/);
  await assert.rejects(binding.readJournal(0n,1),/fenced/);
  assert.equal(owner.commits.length,1);
});
