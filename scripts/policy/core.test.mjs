import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {checkManifestFields, toml, trackedFiles} from '../policy-core.mjs';
import {fuzz as licenseGraph} from '../check-js-licenses.fuzz.mjs';

test('shared license graph conformance: aliases, cycles and canonical physical reads', () =>
  licenseGraph(Uint8Array.of(7, 0, 1, 2, 3, 4, 5, 6, 7)));

test('Git inventory preserves path bytes, pathspecs, and missing tracked files', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-core-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  execFileSync('git', ['init', '-q', root]);
  const paths = ['space name.rs', 'quote"name.rs', 'line\nbreak.rs', 'tab\tname.rs', 'unicodé.rs', 'Cargo.toml'];
  for (const path of paths) writeFileSync(join(root, path), '');
  writeFileSync(join(root, 'Cargo.toml'), '[package]\nname = "literal#value"\nedition = "2024"\n');
  execFileSync('git', ['add', '.'], {cwd: root});
  rmSync(join(root, paths[0]));
  writeFileSync(join(root, 'untracked.rs'), '');
  assert.deepEqual(trackedFiles(root).sort(), paths.sort());
  assert.deepEqual(trackedFiles(root, ['*.rs', ':!tab*']).sort(), paths.filter(path => path.endsWith('.rs') && !path.startsWith('tab')));
  assert.equal(toml(root, 'Cargo.toml').package.name, 'literal#value');
  assert.throws(() => toml(root, 'absent.toml'));
});

test('manifest policy checks the declared table and every dependency identity', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-manifests-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  writeFileSync(join(root, 'Cargo.toml'), '[package]\nversion = "1.0.0"\n[package.metadata]\nreleased = 2026-11-17\n[workspace.package]\nversion = "2.0.0"\n');
  writeFileSync(join(root, 'package.json'), JSON.stringify({dependencies: {'@acyclic-labs/stream': '3.0.0'}}));
  const checks = [['Cargo.toml', ['package', 'version'], '1.0.0'], ['package.json', ['dependencies', '@acyclic-labs/stream'], '3.0.0']];
  checkManifestFields(root, checks);
  assert.throws(() => checkManifestFields(root, [['Cargo.toml', ['package', 'metadata', 'released'], '2026-11-17']]), /must be/);
  for (const [file, keys] of checks) {
    assert.throws(() => checkManifestFields(root, [[file, keys, '2.0.0']]), /must be/);
    assert.throws(() => checkManifestFields(root, [[file, [...keys, 'missing'], '1.0.0']]), /found undefined/);
  }
});
