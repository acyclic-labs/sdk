import assert from 'node:assert/strict';
import {readFile, readdir} from 'node:fs/promises';
import {test} from 'node:test';
import {decodeCompletion} from '../native/process-completion.mjs';
import {fuzz} from './process-completion.fuzz.mjs';

test('original native receipt corpus rejects corruptions and planted admission losses', async () => {
  const directory = new URL('./corpus/process-completion/', import.meta.url);
  const originals = await Promise.all((await readdir(directory)).filter(name => name.endsWith('.json'))
    .map(name => readFile(new URL(name, directory))));
  assert.ok(originals.length > 0, 'original producer corpus is required');
  const replay = decode => {
    for (const original of originals) {
      const expected = JSON.parse(original);
      assert.doesNotThrow(() => decode(original, expected.invocation));
      fuzz(original, decode);
    }
  };
  replay(decodeCompletion);
  for (const original of originals) {
    for (let end = 0; end < original.length; end++) fuzz(original.subarray(0, end));
    for (let at = 0; at < original.length; at++) {
      const corrupt = Buffer.from(original);
      corrupt[at] ^= 0x80;
      fuzz(corrupt);
    }
  }
  // Execute the actual decoder with one production guard removed at a time.
  // Each omission must fail the same fuzz oracle, not an alternate decoder.
  const source = await readFile(new URL('../native/process-completion.mjs', import.meta.url), 'utf8');
  const guards = [
    "value.schema !== 'acyclic.process-custody-completion.v3'",
    'value.invocation !== invocation', "typeof value.platform !== 'string'",
    '!integer(value.host_exit_code)', 'value.host_exit_code < 0', 'value.host_exit_code > 0xffffffff',
    "typeof value.cancelled !== 'boolean'", "typeof value.process_tree_retired !== 'boolean'",
    '!integer(root.native)', '!integer(root.code)', '!integer(root.signal)', 'root.signal <= 0',
    '((root.code === null) === (root.signal === null))',
    '(value.process_tree_retired && !root)',
    '(!value.process_tree_retired && value.failure === null)',
    "typeof value.failure.phase !== 'string'", "typeof value.failure.error?.kind !== 'string'",
    '!integer(value.failure.error.raw_os_error)',
  ];
  for (const [guard, replacement] of [...guards.map(guard => [guard, 'false']),
    ['return value;', 'return {...value, failure: value.failure && {...value.failure, error: {...value.failure.error, raw_os_error: null}}};'],
    ['return value;', 'throw invalid();']]) {
    const planted = source.replace(guard, replacement);
    assert.notEqual(planted, source, `production guard missing: ${guard}`);
    const {decodeCompletion} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`);
    assert.throws(() => replay(decodeCompletion), {code: 'ERR_ASSERTION'}, guard);
  }
});
