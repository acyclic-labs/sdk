import assert from 'node:assert/strict';
import {pollingSites} from './no-polling.mjs';

// Byte-oriented source mutations, not captured tool output. Exercise the real
// tokenizer: decoding must preserve raw candidates and expose escaped ones.
export function fuzz(data) {
 const text = new TextDecoder().decode(new Uint8Array(data));
 for (const command of ['sleep`' + text + '` 1', 'Start-S`leep ' + text, 'Thread.\\u0053leep(' + text + ')']) {
  const source = 'runHost(' + JSON.stringify(command) + ');';
  assert.ok(pollingSites('scripts/control.mjs', source).some(site => site.kind === 'embedded-timer-or-retry'), source);
 }
 const comment = '<#' + text.replaceAll('#', '') + '#>';
 assert.deepEqual(pollingSites('scripts/control.ps1', comment), []);
 const source = comment + ' Start-Sleep 1';
 assert.deepEqual(pollingSites('scripts/control.ps1', source).map(site => site.start), [source.lastIndexOf('Sleep')]);
 assert.throws(() => pollingSites('scripts/control.ps1', comment.slice(0, -2)), /unterminated block comment/);
}
