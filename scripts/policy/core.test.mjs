import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {checkFileCopies, checkManifestFields, checkTypeScriptReferences, toml, trackedFiles} from '../policy-core.mjs';
import {fuzz as licenseGraph} from '../check-js-licenses.fuzz.mjs';

test('shared license graph conformance: aliases, cycles and canonical physical reads', () =>
  licenseGraph(Uint8Array.of(7, 0, 1, 2, 3, 4, 5, 6, 7)));

test('Git inventory preserves path bytes, pathspecs, and missing tracked files', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-core-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  execFileSync('git', ['init', '-q', root]);
  const paths = ['space name.rs', 'quote"name.rs', 'line\nbreak.rs', 'tab\tname.rs', 'unicodé.rs', 'Cargo.toml', '.gitignore'];
  for (const path of paths) writeFileSync(join(root, path), '');
  writeFileSync(join(root, 'Cargo.toml'), '[package]\nname = "literal#value"\nedition = "2024"\n');
  writeFileSync(join(root, '.gitignore'), 'ignored.rs\n');
  execFileSync('git', ['add', '.'], {cwd: root});
  rmSync(join(root, paths[0]));
  writeFileSync(join(root, 'untracked.rs'), '');
  writeFileSync(join(root, 'ignored.rs'), '');
  assert.deepEqual(trackedFiles(root).sort(), paths.sort());
  assert.deepEqual(trackedFiles(root, ['*.rs', ':!tab*']).sort(), paths.filter(path => path.endsWith('.rs') && !path.startsWith('tab')));
  assert.deepEqual(trackedFiles(root, ['*.rs'], {untracked: true}).sort(), [...paths.filter(path => path.endsWith('.rs')), 'untracked.rs'].sort());
  assert.equal(toml(root, 'Cargo.toml').package.name, 'literal#value');
  assert.throws(() => toml(root, 'absent.toml'));
});

test('policy entrypoints reject violations through symlinks and stay inert on import', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-entry-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  mkdirSync(join(root, 'scripts'));
  for (const name of ['check-cargo-callers.mjs', 'policy-core.mjs']) copyFileSync(new URL(`../${name}`, import.meta.url), join(root, 'scripts', name));
  writeFileSync(join(root, 'scripts/cargo-callers.json'), '[]');
  copyFileSync(new URL('../fixtures/cargo-callers/recorded-log.txt', import.meta.url), join(root, 'scripts/violation.sh'));
  execFileSync('git', ['init', '-q', root]);
  execFileSync('git', ['add', '.'], {cwd: root});
  symlinkSync(join(root, 'scripts/check-cargo-callers.mjs'), join(root, 'check.mjs'));
  const result = spawnSync(process.execPath, ['check.mjs'], {cwd: root, encoding: 'utf8'});
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /scripts\/violation.sh invokes Cargo/);
  const imported = spawnSync(process.execPath, ['--input-type=module', '-e', 'await import("./scripts/check-cargo-callers.mjs")'], {cwd: root, encoding: 'utf8'});
  assert.equal(imported.status, 0, imported.stderr);
});

test('manifest policy checks declared identities and exact packaged source bytes', t => {
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
  for (const file of ['canonical.bin', 'packaged.bin']) writeFileSync(join(root, file), Uint8Array.of(0, 0x80));
  checkFileCopies(root, [['canonical.bin', 'packaged.bin']]);
  writeFileSync(join(root, 'packaged.bin'), Uint8Array.of(0, 0x81));
  assert.throws(() => checkFileCopies(root, [['canonical.bin', 'packaged.bin']]), /packaged source drift/);
});
test('the root TypeScript build covers every configured package exactly once', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-ts-references-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  for (const name of ['first', 'second', 'without-config']) mkdirSync(join(root, 'typescript/packages', name), {recursive: true});
  for (const name of ['first', 'second']) writeFileSync(join(root, 'typescript/packages', name, 'tsconfig.json'), '{}');
  const check = names => {
    writeFileSync(join(root, 'tsconfig.json'), JSON.stringify({references: names.map(name => ({path: `./typescript/packages/${name}`}))}));
    return () => checkTypeScriptReferences(root);
  };
  check(['second', 'first'])();
  checkTypeScriptReferences(root, ['first', 'second']);
  assert.throws(() => checkTypeScriptReferences(root, ['without-config']), /reference every package/);
  for (const names of [[], ['first'], ['first', 'second', 'second'], ['first', 'second', 'without-config'], ['first', 'second', 'missing']])
    assert.throws(check(names), /reference every package/);
});
