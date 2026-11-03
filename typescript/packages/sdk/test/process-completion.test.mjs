import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {test} from 'node:test';
import {fuzz} from './process-completion.fuzz.mjs';

test('original native receipt corpus rejects corruptions and a planted binding loss', async () => {
  const original = await readFile(new URL('./corpus/process-completion/linux-v3.json', import.meta.url));
  fuzz(original);
  for (let end = 0; end < original.length; end++) fuzz(original.subarray(0, end));
  for (let at = 0; at < original.length; at++) {
    const corrupt = Buffer.from(original);
    corrupt[at] ^= 0x80;
    fuzz(corrupt);
  }
  // Execute the actual decoder after planting only invocation-binding loss.
  // The unchanged fuzz oracle must detect that mutation.
  const source = await readFile(new URL('../native/process-completion.mjs', import.meta.url), 'utf8');
  const planted = source.replace('value.invocation !== invocation', 'false');
  assert.notEqual(planted, source);
  const {decodeCompletion} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`);
  assert.throws(() => fuzz(original, decodeCompletion), {code: 'ERR_ASSERTION'});
});
