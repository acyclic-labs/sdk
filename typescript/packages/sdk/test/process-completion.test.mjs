import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
import {decodeCompletion} from '../native/process-completion.mjs';
import {fuzz} from './process-completion.fuzz.mjs';

test('original native receipt corpus rejects corruptions and planted admission losses', async () => {
  const original = await readFile(new URL('./corpus/process-completion/linux-v3.json', import.meta.url));
  const expected = JSON.parse(original);
  const replay = decode => {
    let decoded;
    assert.doesNotThrow(() => { decoded = decode(original, expected.invocation); });
    assert.deepEqual(decoded, expected, 'retain every original producer field');
    fuzz(original, decode);
  };
  replay(decodeCompletion);
  for (let end = 0; end < original.length; end++) fuzz(original.subarray(0, end));
  for (let at = 0; at < original.length; at++) {
    const corrupt = Buffer.from(original);
    corrupt[at] ^= 0x80;
    fuzz(corrupt);
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
  ];
  for (const [guard, replacement] of [...guards.map(guard => [guard, 'false']),
    ['return value;', 'throw invalid();']]) {
    const planted = source.replace(guard, replacement);
    assert.notEqual(planted, source, `production guard missing: ${guard}`);
    const {decodeCompletion} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`);
    assert.throws(() => replay(decodeCompletion), {code: 'ERR_ASSERTION'}, guard);
  }
});
