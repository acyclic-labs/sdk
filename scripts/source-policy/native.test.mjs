import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {pollingUnit, pollingSites, SourceSyntaxError} from './index.mjs';
const nativeTokens = (source, file) => pollingUnit(file, source).stream;

test('the actual console helper and verbatim strings preserve following native operations', () => {
 const file = 'scripts/graphcoder-conpty.cs';
 const actual = readFileSync(new URL('../graphcoder-conpty.cs', import.meta.url), 'utf8');
 assert.doesNotThrow(() => nativeTokens(actual, file));
 for (const prefix of ['@', '$@', '@$']) {
  const source = `${prefix}"\\\\.\\pipe\\"; Thread.Sleep(1);`;
  const tokens = nativeTokens(source, file);
  assert.ok(tokens.some(token => token.kind === 'word' && token.value === 'Sleep'));
  assert.equal(tokens.filter(token => token.kind === 'literal').length, 1);
  assert.ok(pollingSites(file, source).some(site => site.kind === 'timer-or-readiness'));
 }
 const quoted = '@"first line\n""Thread.Sleep(1)""\\"; Thread.Sleep(2);';
 const tokens = nativeTokens(quoted, file);
 assert.ok(tokens.some(token => token.kind === 'literal' && token.value.includes('Thread.Sleep(1)')));
 assert.ok(tokens.some(token => token.kind === 'word' && token.value === 'Sleep' && token.line === 2));
 assert.throws(() => nativeTokens('@"unterminated\\', file), SourceSyntaxError);
});

test('Bash substitutions and heredocs preserve the following command', () => {
 const source = [
  String.raw`channel="$(sed -nE 's/^channel = "([^"]+)"$/\1/p' rust-toolchain.toml)"`,
  String.raw`IFS=$'\t' read -r label <<< "$channel"`,
  "node --input-type=module - <<'JS'",
  "// a payload's quote does not start shell syntax",
  'JS',
  '((value = 1 << shift))',
  'sleep 1',
 ].join('\n');
 assert.ok(pollingSites('control.sh', source).some(site => site.start === source.lastIndexOf('sleep')));
 assert.throws(() => pollingSites('control.sh', "cat <<'EOF'\nunterminated"), /unterminated shell here-document/);
});
