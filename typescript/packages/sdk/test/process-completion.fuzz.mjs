import assert from 'node:assert/strict';
import {decodeCompletion} from '../native/process-completion.mjs';

// Retained byte fuzz entry point. Seed with original native v3 receipts; the
// real-binary control supplies its producer's bytes on every execution.
export function fuzz(data, decode = decodeCompletion) {
  const source = Buffer.from(data).toString('utf8');
  let seed;
  try { seed = JSON.parse(source); } catch { seed = null; }
  const identity = typeof seed?.invocation === 'string' ? seed.invocation : '';
  const invalid = value => assert.throws(() => decode(JSON.stringify(value), identity),
    {code: 'PROCESS_COMPLETION_INVALID'});
  const check = bytes => {
    try {
      const result = decode(bytes, identity);
      assert.equal(result.invocation, identity);
      assert.ok(result.failure !== null || result.process_tree_retired);
      if (result.process_tree_retired) assert.ok(result.root_status);
    } catch (error) {
      if (error.code !== 'PROCESS_COMPLETION_INVALID') throw error;
    }
  };
  check(source);
  // Fixed mutations per fuzz input, covering truncation, binding loss and the
  // physical terminal branch without copying a parallel decoder model.
  check(source.slice(0, Math.floor(source.length / 2)));
  if (seed && typeof seed === 'object') {
    invalid({...seed, invocation: `${identity}-different`});
    for (const value of [null, false, 0, '', [], {}]) invalid(value);
    check(JSON.stringify({...seed, root_status: null}));
    check(JSON.stringify({...seed, process_tree_retired: false}));
    // Mutate original producer fields at every admitted scalar boundary.
    // These are corruptions of real wire bytes, not replacement responses.
    for (const [field, values] of [
      ['schema', [undefined, null, '', 0]], ['platform', [undefined, null, 0]],
      ['host_exit_code', [undefined, null, -1, 0x100000000, 0.5]],
      ['cancelled', [undefined, null, 0, 'false']],
      ['process_tree_retired', [undefined, null, 0, 'true']],
      ['failure', [false, {}, {phase: 0}]],
    ]) {
      for (const value of values) invalid({...seed, [field]: value});
    }
    if (seed.root_status) {
      for (const [field, values] of [['native', [undefined, null, 0.5]],
        ['code', [undefined, '0', 0.5]], ['signal', [undefined, 0, -1, 'TERM', 0.5]]]) {
        for (const value of values) invalid(
          {...seed, root_status: {...seed.root_status, [field]: value}});
      }
      // Corrupt either status representation independently; two simultaneous
      // statuses must not mask a missing signal-domain guard.
      for (const signal of [0, -1, 'TERM', 0.5]) invalid(
        {...seed, root_status: {...seed.root_status, code: null, signal}});
      for (const [code, signal] of [[null, null], [17, 9]]) invalid(
        {...seed, root_status: {...seed.root_status, code, signal}});
      invalid({...seed, root_status: null, process_tree_retired: true});
    }
    invalid({...seed, process_tree_retired: false, failure: null});
  }
}
