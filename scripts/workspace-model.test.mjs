import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {assertRegistryPinsCurrent, workspaceBrowserGraph, workspaceGraph, workspacePackages, versionMatches} from './workspace-model.mjs';

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
  write('cargo-bazel-lock.json', JSON.stringify({conditions: {}, crates: {'win 1.0.0': {targets: [{Library: {crate_name: 'win'}}], common_attrs: {crate_features: {common: [], selects: {'x86_64-pc-windows-msvc': ['windows-only']}}}}}}));
  write('cargo-wasm-bazel-lock.json', JSON.stringify({conditions: {}, crates: {}}));
  const packages = workspacePackages(root), app = packages.find(p => p.name === 'app');
  assert.equal(app.edition, '2024');
  assert.deepEqual(app.targets.map(t => t.name), ['custom']);
  assert.equal(app.dependencies[0].rename, 'renamed');
  assert.equal(app.dependencies[0].path, path.join(root, 'backend'));
  const linux = workspaceGraph(root), windows = workspaceGraph(root, 'x86_64-pc-windows-msvc');
  const node = (graph, name) => graph.resolve.nodes.find(n => n.id === graph.packages.find(p => p.name === name).id);
  assert.deepEqual(node(linux, 'app').deps.map(d => d.name), ['renamed']);
  assert.deepEqual(node(windows, 'app').deps.map(d => d.name), ['renamed', 'win']);
  assert.deepEqual(node(windows, 'win').features, ['windows-only']);
  assert.deepEqual(node(linux, 'win').features, []);
  assert.deepEqual(node(linux, 'backend').features, ['test-support']);
  const browser = workspaceBrowserGraph(root, [['-p', 'app']]);
  assert.deepEqual(node(browser, 'app').deps.map(d => d.name), ['renamed']);
  assert.deepEqual(node(browser, 'backend').features, []);
  const selected = workspaceBrowserGraph(root, [['-p', 'app', '--features', 'test-support']]);
  assert.deepEqual(node(selected, 'backend').features, ['test-support']);
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

test('registry features follow the current dependency declaration', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'registry-features-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nserde_json={version="1",features=["preserve_order"]}\n');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["serde_json"]\n[[package]]\nname="serde_json"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
  write('cargo-bazel.Cargo.lock', 'version=4\n[[package]]\nname="direct-cargo-bazel-deps"\nversion="0.0.1"\ndependencies=["serde_json"]\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions: {}, crates: {'serde_json 1.0.0': {targets: [{Library: {crate_name: 'serde_json'}}], common_attrs: {crate_features: ['default']}}}}));
  assert.throws(() => assertRegistryPinsCurrent(root), /serde_json.*preserve_order.*repin/i);
  write('cargo-wasm-bazel-lock.json', JSON.stringify({conditions: {}, crates: {'serde_json 1.0.0': {targets: [{Library: {crate_name: 'serde_json'}}], common_attrs: {crate_features: ['default']}}}}));
  const stale = workspaceBrowserGraph(root, [['-p', 'app']]);
  assert.deepEqual([...stale.registry_inputs.values()][0].features, new Set(['preserve_order']));
  write('cargo-wasm-bazel-lock.json', JSON.stringify({conditions: {}, crates: {'serde_json 1.0.0': {targets: [{Library: {crate_name: 'serde_json'}}], common_attrs: {crate_features: ['default', 'preserve_order']}}}}));
  const browser = workspaceBrowserGraph(root, [['-p', 'app']]);
  assert.deepEqual(browser.resolve.nodes.find(node => node.id.includes('#serde_json@')).features, ['default', 'preserve_order']);
});

test('registry dependencies are selected from the current Cargo lock', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'registry-new-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nnew_crate="1"\n');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["new_crate"]\n[[package]]\nname="new_crate"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
  write('cargo-bazel.Cargo.lock', 'version=4\n[[package]]\nname="direct-cargo-bazel-deps"\nversion="0.0.1"\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions: {}, crates: {}}));
  assert.throws(() => assertRegistryPinsCurrent(root), /new_crate.*1\.0\.0.*repin/i);
});

test('browser registry inputs retain direct default-feature requests', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'browser-defaults-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nserde_json="1"\ncfg-if={version="1",default-features=false}\n');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["serde_json","cfg-if"]\n[[package]]\nname="serde_json"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n[[package]]\nname="cfg-if"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
  write('cargo-wasm-bazel-lock.json', JSON.stringify({conditions: {}, crates: Object.fromEntries(['serde_json', 'cfg-if'].map(name =>
    [`${name} 1.0.0`, {targets: [{Library: {crate_name: name.replaceAll('-', '_')}}], common_attrs: {crate_features: ['stale_feature']}}]))}));
  const graph = workspaceBrowserGraph(root, [['-p', 'app']]);
  const defaults = [...graph.registry_inputs].filter(([, value]) => value.defaultFeatures)
    .map(([id]) => graph.packages.find(pkg => pkg.id === id).name);
  assert.deepEqual(defaults, ['serde_json']);
  for (const input of graph.registry_inputs.values()) assert.equal(input.features.size, 0);
});

test('unselected platforms and disabled optional dependencies do not require registry pins', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'registry-inactive-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nhelper={path="../helper",default-features=false}\n[target.\'cfg(target_arch = "wasm32")\'.dependencies]\nwasm="1"\n');
  write('helper/Cargo.toml', '[package]\nname="helper"\nversion="0.1.0"\n[dependencies]\nunused={version="1",optional=true}\n[features]\ndefault=["dep:unused"]\n');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["helper", "wasm"]\n[[package]]\nname="helper"\nversion="0.1.0"\ndependencies=["unused"]\n[[package]]\nname="wasm"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n[[package]]\nname="unused"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions: {}, crates: {}}));
  assert.doesNotThrow(() => assertRegistryPinsCurrent(root));
  const browser = workspaceGraph(root, 'wasm32-unknown-unknown');
  const app = browser.packages.find(pkg => pkg.name === 'app');
  assert.deepEqual(browser.resolve.nodes.find(node => node.id === app.id).deps.map(dep => dep.name), ['helper', 'wasm']);
});
