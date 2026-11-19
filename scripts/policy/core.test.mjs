import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {checkFileCopies, checkManifestFields, checkTypeScriptReferences, featureClosure, readSources, toml, trackedFiles} from '../policy-core.mjs';
import {workspaceMembers} from '../workspace-manifests.mjs';
import {fuzz as licenseGraph} from '../check-js-licenses.fuzz.mjs';

test('shared license graph conformance: aliases, cycles and canonical physical reads', () =>
  licenseGraph(Uint8Array.of(7, 0, 1, 2, 3, 4, 5, 6, 7)));

test('feature expansion agrees with transitive matrices for every three-node graph and root set', () => {
  const names = ['a', 'b', 'constructor'];
  for (let edges = 0; edges < 512; edges++) {
    const graph = names.map((_, i) => names.map((_, j) => Boolean(edges & (1 << (i * 3 + j)))));
    const features = Object.fromEntries(names.map((name, i) => [name, names.filter((_, j) => graph[i][j])]));
    for (let k = 0; k < 3; k++) for (let i = 0; i < 3; i++) for (let j = 0; j < 3; j++) graph[i][j] ||= graph[i][k] && graph[k][j];
    for (let roots = 0; roots < 8; roots++) {
      const selected = names.filter((_, i) => roots & (1 << i));
      const expected = names.filter((_, j) => selected.includes(names[j]) || names.some((name, i) => selected.includes(name) && graph[i][j]));
      assert.deepEqual([...featureClosure(features, selected)].sort(), expected.sort());
    }
  }
  assert.deepEqual([...featureClosure({}, ['constructor'])], ['constructor']);
});

test('Git inventory preserves path bytes, pathspecs, and missing tracked files', async t => {

  const root = mkdtempSync(join(tmpdir(), 'policy-core-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  execFileSync('git', ['init', '-q', root]);
  const paths = ['space name.rs', 'quote"name.rs', 'line\nbreak.rs', 'tab\tname.rs', 'unicodé.rs', 'Cargo.toml', '.gitignore'];
  for (const path of paths) writeFileSync(join(root, path), path);
  writeFileSync(join(root, 'Cargo.toml'), '[package]\nname = "literal#value"\nedition = "2024"\n');
  writeFileSync(join(root, '.gitignore'), 'ignored.rs\n');
  execFileSync('git', ['add', '.'], {cwd: root});
  const missing = paths[0];
  rmSync(join(root, missing));
  writeFileSync(join(root, 'untracked.rs'), '');
  writeFileSync(join(root, 'ignored.rs'), '');
  assert.deepEqual(trackedFiles(root).sort(), paths.sort());
  assert.deepEqual(trackedFiles(root, ['*.rs', ':!tab*']).sort(), paths.filter(path => path.endsWith('.rs') && !path.startsWith('tab')));
  assert.deepEqual(trackedFiles(root, ['*.rs'], {untracked: true}).sort(), [...paths.filter(path => path.endsWith('.rs')), 'untracked.rs'].sort());
  const existing = trackedFiles(root, ['*.rs', ':!space*']);
  assert.deepEqual(await readSources(root, existing), existing.map(file => [file, file]));
  await assert.rejects(readSources(root, [missing]), {code: 'ENOENT'});
  assert.equal(toml(root, 'Cargo.toml').package.name, 'literal#value');
  assert.throws(() => toml(root, 'absent.toml'));
});

test('policy entrypoints reject violations through symlinks and stay inert on import', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-entry-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  mkdirSync(join(root, 'scripts'));
  for (const name of ['check-cargo-callers.mjs', 'check-incremental.mjs', 'policy-core.mjs']) copyFileSync(new URL(`../${name}`, import.meta.url), join(root, 'scripts', name));
  writeFileSync(join(root, 'scripts/cargo-callers.json'), '[]');
  const recorded = new URL('../fixtures/cargo-callers/recorded-log.txt', import.meta.url);
  copyFileSync(recorded, join(root, 'scripts/violation.sh'));
  const [program] = readFileSync(recorded, 'utf8').split(' ');
  writeFileSync(join(root, 'download.bzl'), `def provision(ctx):
    components = [("${program}", ctx.os.arch)]
    ctx.execute(["rustc", "--version"])
    return components
`);
  writeFileSync(join(root, 'execute.bzl'), `def build(ctx):
    argv = [ctx.path("${program}"), "--version"]
    ctx.execute(argv)
`);
  execFileSync('git', ['init', '-q', root]);
  execFileSync('git', ['add', '.'], {cwd: root});
  symlinkSync(join(root, 'scripts/check-cargo-callers.mjs'), join(root, 'check.mjs'));
  const result = spawnSync(process.execPath, ['check.mjs'], {cwd: root, encoding: 'utf8'});
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /scripts\/violation.sh invokes Cargo/);
  assert.match(result.stderr, /execute\.bzl invokes Cargo/);
  assert.doesNotMatch(result.stderr, /download\.bzl invokes Cargo/);
  const imported = spawnSync(process.execPath, ['--input-type=module', '-e', 'await import("./scripts/check-cargo-callers.mjs")'], {cwd: root, encoding: 'utf8'});
  assert.equal(imported.status, 0, imported.stderr);
  writeFileSync(join(root, 'scripts/incremental-disabled.sh'), 'CARGO_INCREMENTAL=0\n');
  execFileSync('git', ['add', 'scripts/incremental-disabled.sh'], {cwd: root});
  const incremental = spawnSync(process.execPath, ['scripts/check-incremental.mjs'], {cwd: root, encoding: 'utf8'});
  assert.equal(incremental.status, 1, incremental.stderr);
  assert.match(incremental.stderr, /incremental-disabled\.sh:1: sets CARGO_INCREMENTAL to 0/);
});

test('source pages retain every input under a configured descriptor budget', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-sources-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  const files = Array.from({length: 512}, (_, index) => `source-${index}.rs`);
  for (const file of files) writeFileSync(join(root, file), file);
  const script = 'const {readSources} = await import(process.argv[2]); console.log(JSON.stringify(await readSources(process.argv[1], JSON.parse(process.argv[3]))));';
  const result = spawnSync('sh', ['-c', 'ulimit -n 128 && exec "$@"', 'source-pages', process.execPath,
    '--input-type=module', '-e', script, root, new URL('../policy-core.mjs', import.meta.url).href, JSON.stringify(files)], {encoding: 'utf8'});
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), files.map(file => [file, file]));
});

test('manifest policy checks declared identities and exact packaged source bytes', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-manifests-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  writeFileSync(join(root, 'Cargo.toml'), '[package]\nversion = "1.0.0"\n[package.metadata]\nreleased = 2026-11-17\n[workspace]\nmembers = ["first/.", "second"]\n[workspace.dependencies]\nsecond = {path="second"}\n[workspace.package]\nversion = "2.0.0"\n');
  writeFileSync(join(root, 'package.json'), JSON.stringify({dependencies: {'@acyclic-labs/stream': '3.0.0'}}));
  for (const directory of ['first', 'second']) mkdirSync(join(root, directory));
  writeFileSync(join(root, 'first/Cargo.toml'), '[package]\nname="first"\n[dependencies]\nsecond.workspace=true\nvendor={path="../vendor"}\n[target.unix.dev-dependencies]\nsecond={path="../second"}\n');
  writeFileSync(join(root, 'second/Cargo.toml'), '[package]\nname="second"\n');
  assert.deepEqual([...workspaceMembers(root)], [
    ['first/.', {name: 'first', dependencies: [{directory: 'second', kind: 'normal'}, {directory: 'second', kind: 'dev'}]}],
    ['second', {name: 'second', dependencies: []}],
  ]);
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
