import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {testFeatureFindings, testGateEnabled} from './policy-core.mjs';

function fixture(t, manifest, source = '#![cfg(feature = "local")]\n', extra = {}) {
  const root = mkdtempSync(join(tmpdir(), 'test-features-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  for (const [path, text] of Object.entries({'Cargo.toml': '[package]\nname="example"\nversion="0.0.0"\n' + manifest,
    'tests/case.rs': source, 'rust/vendor/dep/Cargo.toml': 'invalid', 'scripts/tests/fixtures/Cargo.toml': 'invalid', ...extra})) {
    mkdirSync(join(root, path, '..'), {recursive: true});
    writeFileSync(join(root, path), text);
  }
  execFileSync('git', ['init', '-q', root]);
  execFileSync('git', ['add', '.'], {cwd: root});
  return root;
}

test('real Cargo targets retain feature closure, alternatives, platform independence and model opt-in', t => {
  const declared = features => `[[test]]\nname="case"\nrequired-features=${JSON.stringify(features)}\n`;
  for (const [manifest, source, count] of [
    ['', undefined, 1], ['[[test]]\nname="case"\n', undefined, 1], ['autotests=false\n', undefined, 0],
    [declared(['backend']) + '[features]\nbackend=["local"]\nlocal=["backend"]\n', undefined, 0],
    ['', '#![cfg(all(not(target_arch="wasm32"),feature="local"))]', 1], ['', '#![cfg(target_os="linux")]', 0],
    ...['loom', 'kani'].flatMap(model => [['', `#![cfg(${model})]`, 1], [declared(['model-checking']), `#![cfg(${model})]`, 0]]),
    ...['local', 'remote'].map(feature => [declared([feature]), '#![cfg(any(feature="local",feature="remote"))]', 0]),
    ['', '#![cfg(any(feature="local",feature="remote"))]', 1],
    [declared(['local', 'auth']), '#![cfg(all(not(target_arch="wasm32"),feature="auth",any(feature="local",feature="remote")))]', 0],
  ]) assert.equal(testFeatureFindings(fixture(t, manifest, source)).length, count);
  const errors = testFeatureFindings(fixture(t, '', '#![cfg(not(feature="remote"))]'));
  assert.match(errors[0], /negated feature gate/);
  assert.doesNotMatch(errors[0], /add required-features/);
  assert.equal(testFeatureFindings(fixture(t, '', '', {'tests/nested/main.rs': '#![cfg(feature="local")]'})).length, 1);
  assert.equal(testFeatureFindings(fixture(t, 'autotests=false\n[[test]]\nname="custom"\npath="custom.rs"\n', '', {'custom.rs': '#![cfg(feature="local")]'})).length, 1);
});

test('bounded cfg model: direct evaluation agrees with the former independent DNF representation', () => {
  const atoms = ['feature="local"', 'feature="remote"', 'loom', 'not(target_os="linux")']
    .map((source, index) => ({source, options: [index < 3 ? [index] : []]}));
  const compose = (kind, left, right) => ({source: `${kind}(${left.source},${right.source})`,
    options: kind === 'any' ? [...left.options, ...right.options]
      : left.options.flatMap(first => right.options.map(second => [...first, ...second]))});
  const level = children => ['all', 'any'].flatMap(kind => children.flatMap(left => children.map(right => compose(kind, left, right))));
  for (const node of [...atoms, ...level(atoms), ...level(level(atoms))]) {
    for (let bits = 0; bits < 8; bits++) {
      const enabled = new Set(['local', 'remote', 'model-checking'].filter((_, index) => bits & (1 << index)));
      const expected = node.options.some(option => option.every(index => bits & (1 << index)));
      assert.equal(testGateEnabled(node.source, enabled), expected, node.source + ':' + bits);
    }
  }
  for (const gate of ['', 'all(', 'any(feature="local") trailing', 'not(feature="local")', 'any(target_os="linux",not(feature="local"))']) {
    assert.throws(() => testGateEnabled(gate, new Set(['local'])));
  }
});

test('the actual policy CLI refuses a tracked ungated target and accepts explicit opt-in', t => {
  for (const [manifest, status] of [['', 1], ['[[test]]\nname="case"\nrequired-features=["local"]\n', 0]]) {
    const root = fixture(t, manifest);
    mkdirSync(join(root, 'scripts'), {recursive: true});
    for (const name of ['policy-core.mjs', 'check-test-features.mjs']) copyFileSync(new URL(name, import.meta.url), join(root, 'scripts', name));
    const result = spawnSync(process.execPath, [join(root, 'scripts/check-test-features.mjs')], {cwd: tmpdir(), encoding: 'utf8'});
    assert.equal(result.status, status, result.stderr);
    if (status) assert.match(result.stderr, /STEP_FAILED test-feature-hygiene.*\n.*required-features/);
  }
});
