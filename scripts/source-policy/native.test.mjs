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

test('shell heredocs match the complete delimiter word', () => {
 for (const word of ['END-TEXT', "END'-TEXT'", String.raw`END\-TEXT`]) {
  const source = ['cat <<' + word, "payload can't open a quote", 'END-TEXT', 'sleep 1'].join('\n');
  assert.ok(pollingSites('control.sh', source).some(site => site.start === source.lastIndexOf('sleep')));
 }
});

test('shell command substitutions consume heredoc bodies before scanning quotes', () => {
 const source = [
  'text="$(cat <<-\'EOF\' <<END-TEXT',
  "\tpayload can't open a quote or close the substitution: )",
  '\tEOF',
  'more text with an unmatched "',
  'END-TEXT',
  ')"',
  'sleep 1',
 ].join('\n');
 assert.ok(pollingSites('control.sh', source).some(site => site.start === source.lastIndexOf('sleep')));
});

test('shell substitution comments do not start heredocs', () => {
 const source = ['text="$(printf ok # <<EOF', ')"', 'sleep 1'].join('\n');
 assert.ok(pollingSites('control.sh', source).some(site => site.start === source.lastIndexOf('sleep')));
 const redirect = ['text="$(printf payload#part <<EOF', "can't start a shell quote", 'EOF', ')"', 'sleep 1'].join('\n');
 assert.ok(pollingSites('control.sh', redirect).some(site => site.start === redirect.lastIndexOf('sleep')));
});
