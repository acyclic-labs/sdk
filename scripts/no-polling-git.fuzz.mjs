import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {gitBatchSources, GitBatchSyntaxError} from './source-policy/git.mjs';

// Original Git output and independently captured git-show sources; mutations
// below are deliberately damaged input, never fabricated external responses.
const capture = JSON.parse(readFileSync(new URL('./fixtures/no-polling/git-batch.json', import.meta.url), 'utf8'));
const original = Buffer.from(capture.bytes, 'base64');
const terminator = original.indexOf(10) + 1 + Buffer.byteLength(capture.sources[0][1]);

export function fuzzGitBatch(data, decode = gitBatchSources) {
 assert.deepEqual(decode(capture.files, original), new Map(capture.sources));
 const damaged = Buffer.from(original);
 damaged[terminator] = (data[0] ?? 0) === 10 ? 11 : (data[0] ?? 0);
 assert.throws(() => decode(capture.files, damaged), {name: 'GitBatchSyntaxError'}, 'lost body terminator');
 assert.throws(() => decode(capture.files, Buffer.concat([original, Buffer.from(data), Buffer.of(0)])),
  {name: 'GitBatchSyntaxError'}, 'unconsumed response');
 const cut = data.length % original.length;
 assert.throws(() => decode(capture.files, original.subarray(0, cut)), {name: 'GitBatchSyntaxError'});
 const headerEnd = original.indexOf(10);
 const header = original.subarray(0, headerEnd).toString();
 const fields = header.split(' ');
 for (const invalid of [header.replace(fields[0], 'not-an-object'), header + ' extra',
  header.replace(/ \d+$/, ' +' + fields[2]), header.replace(' blob ', ' tree ')]) {
  assert.throws(() => decode(capture.files, Buffer.concat([Buffer.from(invalid), original.subarray(headerEnd)])),
   {name: 'GitBatchSyntaxError'}, invalid);
 }
 try { decode(capture.files, Buffer.from(data)); }
 catch (error) { assert.ok(error instanceof GitBatchSyntaxError, String(error)); }
}
