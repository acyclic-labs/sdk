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

test('futures::poll! advances one state machine, like Future::poll', () => {
 const kinds = source => pollingSites('step.rs', source).filter(site => site.kind === 'timer-or-readiness')
  .map(site => source.slice(site.start).match(/^\w+/)[0]);
 assert.deepEqual(kinds('fn f() { let step = futures::poll!(fut); let again = poll!(fut); drop((step, again)); }'), []);
 assert.deepEqual(kinds('fn f() { let step = Future::poll(fut.as_mut(), cx); drop(step); }'), []);
 // Readiness polls keep their sites: a native poll(2), a poll method, and a poll! of a sleep.
 assert.deepEqual(kinds('fn f() { let ready = libc::poll(fds, 1, 0); drop(ready); }'), ['ready', 'poll', 'ready']);
 assert.deepEqual(kinds('fn f() { let ready = stream.poll(); drop(ready); }'), ['ready', 'poll', 'ready']);
 assert.deepEqual(kinds('fn f() { let late = futures::poll!(tokio::time::sleep(d)); drop(late); }'), ['late', 'sleep', 'late']);
});

test('a timer alias reaches only its own binding scope', () => {
 const named = source => pollingSites('scope.rs', source).filter(site => site.kind === 'timer-or-readiness')
  .map(site => `${source.slice(site.start).match(/^\w+/)[0]}@${site.start}`);
 const source = [
  'fn first() { let x = client.timeout(d); let request = x; request.send(); }',
  'fn second() { let x = 1; let request = x; request.send(); }',
 ].join('\n');
 const second = source.indexOf('fn second');
 const sites = named(source);
 assert.ok(sites.some(site => site.startsWith('timeout@')));
 assert.deepEqual(sites.filter(site => site.startsWith('request@')).length, 2, 'the tainted fn keeps its alias chain');
 assert.deepEqual(sites.filter(site => Number(site.split('@')[1]) > second), [], 'another fn binding of the same name is not a poll');
 // Shadowing in one block starts a new binding; a nested block still sees the outer one.
 const shadow = 'fn f() { let x = timer.sleep(d); let x = 1; use_it(x); }';
 assert.deepEqual(named(shadow).filter(site => site.startsWith('x@')).map(site => Number(site.split('@')[1])), [shadow.indexOf('x')]);
 const nested = 'fn f() { let x = timer.sleep(d); { let y = x; y.await; } }';
 assert.ok(named(nested).some(site => site.startsWith('y@')));
 // An undeclared name (a field, a parameter, a type) keeps its file-wide identity.
 const field = 'fn a(s: &mut S) { s.wait = interval(d); } fn b(s: &S) { s.wait.tick(); }';
 assert.ok(named(field).some(site => site.startsWith('wait@') && Number(site.split('@')[1]) > field.indexOf('fn b')));
 // A timer handed to another function is still caught there: by its type, and by the loop that waits on it.
 const passed = 'fn a() { let tick = interval(d); run(tick); } fn run(tick: Interval) { loop { tick.tick().await; } }';
 const callee = pollingSites('scope.rs', passed).filter(site => site.start > passed.indexOf('fn run'));
 assert.deepEqual(callee.map(site => site.kind), ['timer-or-readiness', 'loop']);
});
