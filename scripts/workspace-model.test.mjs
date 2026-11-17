import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {workspaceGraph, workspacePackages, versionMatches} from './workspace-model.mjs';

test('manifest and Bazel lock projection preserves aliases, target edges and local feature forwarding', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-model-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n[workspace.package]\nversion="0.1.0"\nedition="2024"\n[workspace.dependencies]\nrenamed={package="backend",path="backend",version="=0.1.0",default-features=false}\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion.workspace=true\nedition.workspace=true\n[dependencies]\nrenamed.workspace=true\n[features]\ntest-support=["renamed/test-support"]\n[target.\'cfg(windows)\'.dependencies]\nwin="1"\n[[bin]]\nname="custom"\npath="src/main.rs"\n');
  write('app/src/main.rs', 'fn main() {}\n');
  write('backend/Cargo.toml', '[package]\nname="backend"\nversion="0.1.0"\n[features]\ntest-support=[]\ndefault=[]\n');
  write('backend/src/lib.rs', '');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["backend","win"]\n[[package]]\nname="backend"\nversion="0.1.0"\n[[package]]\nname="win"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
  write('cargo-bazel.Cargo.lock', 'version=4\n[[package]]\nname="direct-cargo-bazel-deps"\nversion="0.0.1"\ndependencies=["win"]\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions: {}, crates: {'win 1.0.0': {targets: [{Library: {crate_name: 'win'}}], common_attrs: {}}}}));
  const packages = workspacePackages(root), app = packages.find(p => p.name === 'app');
  assert.equal(app.edition, '2024');
  assert.deepEqual(app.targets.map(t => t.name), ['custom']);
  assert.equal(app.dependencies[0].rename, 'renamed');
  assert.equal(app.dependencies[0].path, path.join(root, 'backend'));
  const linux = workspaceGraph(root), windows = workspaceGraph(root, 'x86_64-pc-windows-msvc');
  const node = (graph, name) => graph.resolve.nodes.find(n => n.id === graph.packages.find(p => p.name === name).id);
  assert.deepEqual(node(linux, 'app').deps.map(d => d.name), ['renamed']);
  assert.deepEqual(node(windows, 'app').deps.map(d => d.name), ['renamed', 'win']);
  assert.deepEqual(node(linux, 'backend').features, ['test-support']);
});

test('locked version selection distinguishes aliases across incompatible versions', () => {
  assert.equal(versionMatches('0.12.9', '^0.12'), true);
  assert.equal(versionMatches('0.13.5', '^0.12'), false);
  assert.equal(versionMatches('1.1.6+spec-1.1.0', '^1.1.6'), true);
  assert.equal(versionMatches('0.0.4', '^0.0.3'), false);
  assert.equal(versionMatches('2.4.1', '>=2, <3'), true);
  assert.equal(versionMatches('2.5.0', '~2.4.0'), false);
  assert.throws(() => versionMatches('1.0.0', 'not a requirement'), /unsupported/);
});
