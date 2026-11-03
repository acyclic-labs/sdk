import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {fuzzGitBatch} from './no-polling-git.fuzz.mjs';

test('Git batch fuzz oracle retains real sources and rejects damaged frames', () => {
 const capture = JSON.parse(readFileSync(new URL('./fixtures/no-polling/git-batch.json', import.meta.url), 'utf8'));
 for (let length = 0; length < Buffer.from(capture.bytes, 'base64').length; length++) {
  fuzzGitBatch(Uint8Array.from({length}, (_, offset) => (length + offset) & 255));
 }
});

test('Git batch oracle rejects actual production framing mutations', async () => {
 const source = readFileSync(new URL('./source-policy/git.mjs', import.meta.url), 'utf8');
 for (const [original, replacement, diagnostic] of [
  [" || original[at + size] !== 10", '', /lost body terminator/],
  ["if (at !== original.length)", 'if (false)', /unconsumed response/],
  ["if (!request || header.slice(request[0].length) !== file + ' missing')", 'if (false)', /invalid missing-object header/],
 ]) {
  assert.equal(source.split(original).length, 2, 'one actual production branch');
  const mutated = await import('data:text/javascript;base64,' + Buffer.from(source.replace(original, replacement)).toString('base64'));
  assert.throws(() => fuzzGitBatch(Uint8Array.of(1), mutated.gitBatchSources, mutated.GitBatchSyntaxError), {name: 'AssertionError', message: diagnostic});
 }
});
