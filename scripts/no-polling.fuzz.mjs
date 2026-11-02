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
 const word = 'value' + Buffer.from(data).toString('hex');
 for (const command of ['sl\\eep', 'sl\\\neep', 's\\l\\e\\e\\p']) {
  const source = `${command} ${word}`;
  assert.ok(pollingSites('scripts/control.sh', source).some(site => site.kind === 'timer-or-readiness'), source);
  assert.ok(pollingSites('scripts/control.mjs', 'runHost(' + JSON.stringify(source) + ')')
   .some(site => site.kind === 'embedded-timer-or-retry'), source);
 }
 for (const [extension, command, wait, escape] of [['ps1', 'Write-Output', 'Start-Sleep', '`'], ['sh', 'printf', 'sleep', '\\']]) {
  const arguments_ = [word + '#fragment', escape + '#fragment', word + escape + ' #fragment',
   word + escape + '\n#fragment', word + '"quoted"#fragment'];
  if (extension === 'ps1') arguments_.push(word + '<#fragment', word + '`\r\n#fragment');
  for (const argument of arguments_) {
   const source = `${command} ${argument}; ${wait} 1`;
   assert.deepEqual(pollingSites(`scripts/control.${extension}`, source).map(site => site.start), [source.indexOf(';') + 2 + wait.indexOf(extension === 'ps1' ? 'Sleep' : 'sleep')], source);
  }
  assert.deepEqual(pollingSites(`scripts/control.${extension}`, `${command} ${escape}\n# ${wait} 1`), []);
 }
}
