import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {readFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import {expandFuzzIncludes, fuzzOracleViolations, rustTokens} from './fuzz-oracle-policy.mjs';

const fixtures = JSON.parse(readFileSync(new URL('./fixtures/fuzz-oracles.json', import.meta.url), 'utf8'));
for (const [name, source] of Object.entries(fixtures)) {
  test(name, () => {
    const errors = fuzzOracleViolations(name, source);
    if (name.startsWith('bad_')) {
      assert.ok(errors.length, `${name}: negative control escaped the lint`);
      assert.match(errors.join('\n'), /fuzz round-trip oracle/);
    } else assert.deepEqual(errors, []);
  });
}

test('whitespace, comments and raw identifiers cannot hide a guard', () => {
  const source = fixtures['bad_decode_alias.rs'];
  for (const gap of [' ', '\n', ' /* an inert /* nested */ comment */ ']) {
    const changed = rustTokens(source).map(token => token.text === 'read' ? 'r#read' : token.text).join(gap);
    assert.ok(fuzzOracleViolations('mutated.rs', changed).length);
  }
});

test('unreadable token trees fail closed instead of skipping an oracle', () => {
  for (const source of ['/* unclosed', 'r###"unclosed', '"unclosed', '{)', '(']) {
    assert.throws(() => fuzzOracleViolations('broken.rs', source));
  }
});

test('aliases and guards remain rejected under bounded exhaustive renaming', () => {
  const guards = [
    name => `if let Ok(value) = ${name} { assert_eq!(value, input); }`,
    name => `if ${name}.is_ok() { assert_eq!(${name}.unwrap(), input); }`,
    name => `for value in ${name} { assert_eq!(value, input); }`,
    name => `${name}.map(|value| assert_eq!(value, input));`,
  ];
  for (const save of ['store', 'r#type', 'δ']) for (const read of ['restore', 'r#match', 'λ']) {
    for (const guard of guards) {
      const source = `use serde_json::{to_vec as ${save}, from_slice as ${read}};
        let wire = ${save}(&input).unwrap(); let outcome = ${read}::<Value>(&wire); ${guard('outcome')}`;
      assert.ok(fuzzOracleViolations('renamed.rs', source).length);
    }
  }
});

test('literal includes are analyzed and opaque or cyclic includes fail closed', () => {
  const files = new Map([
    ['entry.rs', 'include!("oracle.rs");'],
    ['oracle.rs', fixtures['bad_direct.rs']],
  ]);
  const expanded = expandFuzzIncludes('entry.rs', files.get('entry.rs'), (_, reference) => reference, name => {
    assert.ok(files.has(name)); return files.get(name);
  });
  assert.ok(fuzzOracleViolations('entry.rs', expanded).length);
  assert.throws(() => expandFuzzIncludes('entry.rs', 'include!(path);', () => '', () => ''));
  assert.throws(() => expandFuzzIncludes('entry.rs', 'include!("entry.rs");', (_, reference) => reference, name => files.get(name)));
});

test('the actual CLI rejects working-tree controls while its main baseline passes', () => {
  const root = mkdtempSync(join(tmpdir(), 'sdk-fuzz-oracle-'));
  const absent = process.platform === 'win32' ? 'NUL' : '/dev/null';
  const env = {...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: absent};
  const git = (args, input) => execFileSync('git', args, {cwd: root, env, input, encoding: 'utf8'});
  const runner = fileURLToPath(new URL('./check-fuzz-oracles.mjs', import.meta.url));
  const target = 'rust/demo/fuzz_targets/oracle.rs';
  const header = 'use acyclic_fuzz_support as fuzz_support;\n';
  const baseline = header + fixtures['good_final_result.rs'];
  const sharedPath = 'rust/fuzz/support/round_trip.rs';
  const shared = readFileSync(new URL('../' + sharedPath, import.meta.url), 'utf8');
  try {
    git(['init', '-q', '--initial-branch=main', '--template=']);
    const files = new Map([
      [target, baseline], [sharedPath, shared],
      ['rust/demo/Cargo.toml', '[package]\nname = "demo"\nversion = "0.0.0"\n[dependencies]\nacyclic-fuzz-support = { path = "../fuzz/support" }\n'],
    ]);
    let commit = 'commit refs/heads/main\ncommitter Contract <contract@example.invalid> 1700000000 +0000\ndata 8\nbaseline\n';
    for (const [path, value] of files) commit += `M 100644 inline ${path}\ndata ${Buffer.byteLength(value)}\n${value}\n`;
    git(['fast-import', '--quiet'], commit + 'done\n');
    git(['read-tree', 'HEAD']);
    for (const [path, value] of files) {
      mkdirSync(dirname(join(root, path)), {recursive: true});
      writeFileSync(join(root, path), value);
    }
    const run = args => spawnSync(process.execPath, [runner, ...args], {cwd: root, env, encoding: 'utf8'});
    assert.equal(run([]).status, 0);
    for (const name of ['bad_direct.rs', 'bad_decode_alias.rs', 'bad_macro_guard.rs', 'bad_reexport.rs']) {
      writeFileSync(join(root, target), header + fixtures[name]);
      const result = run([]);
      assert.equal(result.status, 1, result.stderr);
      assert.match(result.stderr, /fuzz round-trip oracle/);
    }
    const main = run(['main']);
    assert.equal(main.status, 0, main.stderr);
    assert.match(main.stdout, /1 Rust sources at [0-9a-f]+/);
    writeFileSync(join(root, target), baseline);
    const alias = 'rust/demo/facade.rs';
    writeFileSync(join(root, alias), 'pub use acyclic_fuzz_support::test_input as admit;');
    git(['add', '-N', alias]);
    const reexport = run([]);
    assert.equal(reexport.status, 1, reexport.stderr);
    assert.match(reexport.stderr, /raw fuzz-input factory outside shared oracle/);
    assert.equal(run(['main']).status, 0);
  } finally {
    assert.equal(dirname(resolve(root)), resolve(tmpdir()));
    rmSync(root, {recursive: true, force: true});
  }
});

test('the existing tiny static job executes the checker and its controls', () => {
  const workflow = readFileSync(new URL('../.github/workflows/static.yml', import.meta.url), 'utf8');
  assert.match(workflow, /node --test scripts\/fuzz-oracle-policy\.test\.mjs/);
  assert.match(workflow, /node scripts\/check-fuzz-oracles\.mjs/);
  const runner = readFileSync(new URL('./check-fuzz-oracles.mjs', import.meta.url), 'utf8');
  assert.match(runner, /fuzz_targets/);
  assert.match(runner, /analysis failed closed/);
});
