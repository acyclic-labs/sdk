import assert from 'node:assert/strict';
import {decodeCompletion} from '../native/process-completion.mjs';

// Retained byte fuzz entry point. Seed with original native v3 receipts; the
// real-binary control supplies its producer's bytes on every execution.
export function fuzz(data) {
  const source = Buffer.from(data).toString('utf8');
  let seed;
  try { seed = JSON.parse(source); } catch { seed = null; }
  const identity = typeof seed?.invocation === 'string' ? seed.invocation : '';
  const check = bytes => {
    try {
      const result = decodeCompletion(bytes, identity);
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
    const refused = {...seed, invocation: `${identity}-different`};
    assert.throws(() => decodeCompletion(JSON.stringify(refused), identity),
      {code: 'PROCESS_COMPLETION_INVALID'});
    check(JSON.stringify({...seed, root_status: null}));
    check(JSON.stringify({...seed, process_tree_retired: false}));
  }
}
