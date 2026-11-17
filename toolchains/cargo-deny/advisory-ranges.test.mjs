import assert from 'node:assert/strict';
import test from 'node:test';
import {ranges} from './advisory-ranges.mjs';

test('commented-out ranges never make an affected version safe', () => {
  assert.deepEqual(ranges('patched = [\n  # ">= 0",\n  ">= 2.0", # "< 1"\n]', 'patched', 'example'), ['>= 2.0']);
});
