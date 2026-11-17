import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {assertRegistryPinsCurrent, workspaceBrowserGraph, workspaceGraph, workspacePackages, workspaceProfileMetadata, versionMatches} from './workspace-model.mjs';

test('manifest and Bazel lock projection preserves aliases, target edges and local feature forwarding', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-model-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\nexclude=["backend"]\n[workspace.package]\nversion="0.1.0"\nedition="2024"\n[workspace.dependencies]\nrenamed={package="backend",path="backend",version="=0.1.0",default-features=false}\n');
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
  const profile = workspaceProfileMetadata(root);
  const workspaceOnly = workspaceProfileMetadata(root, {workspaceOnly: true});
  assert.deepEqual(profile.packages.map(p => p.name), ['app', 'backend']);
  assert.deepEqual(workspaceOnly.packages.map(p => p.name), ['app']);
  assert.deepEqual(profile.packages.find(p => p.name === 'app').targets.map(t => t.name), ['custom']);
  assert.equal(profile.packages.find(p => p.name === 'app').dependencies[0].rename, 'renamed');
  assert.deepEqual(profile.workspace_members, workspaceOnly.workspace_members);
  write('archive/Cargo.toml', '[package]\nname="archive"\nversion="0.1.0"\n');
  write('archive/src/lib.rs', '');
  write('producer/config.toml', '[resolver]\n');
  const archived = workspaceProfileMetadata(path.join(root, 'archive'), {workspaceOnly: true, sourceRoot: root, additionalInputs: ['producer/config.toml']});
  assert.equal(archived.workspace_root, root);
  assert.equal(archived.metadata.acyclic_profile_manifest_v1, 'archive/Cargo.toml');
  assert.ok(archived.metadata.acyclic_profile_inputs_v1['archive/Cargo.toml']);
  assert.ok(archived.metadata.acyclic_profile_inputs_v1['producer/config.toml']);
  assert.throws(() => workspaceProfileMetadata(root, {workspaceOnly: true, additionalInputs: ['../outside']}), /escapes root/);
  assert.throws(() => workspaceProfileMetadata(root, {workspaceOnly: true, sourceRoot: path.join(root, 'archive')}), /escapes source root/);
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

test('captured sibling path dependencies stay inside the declared source boundary', t => {
  const sourceRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-sibling-'));
  t.after(() => fs.rmSync(sourceRoot, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(sourceRoot, file)), {recursive: true}); fs.writeFileSync(path.join(sourceRoot, file), text);};
  write('owner/Cargo.toml', '[package]\nname="owner"\nversion="0.1.0"\n[dependencies]\nhelper={path="../helper"}\n');
  write('owner/src/lib.rs', '');
  write('helper/Cargo.toml', '[package]\nname="helper"\nversion="0.1.0"\n');
  write('helper/src/lib.rs', '');
  const root = path.join(sourceRoot, 'owner');
  assert.throws(() => workspacePackages(root), /escapes source root/);
  const packages = workspacePackages(root, {sourceRoot});
  assert.deepEqual(packages.map(p => p.name), ['helper', 'owner']);
  assert.deepEqual(packages.filter(p => p.workspace_member).map(p => p.name), ['owner']);
  const graph = workspaceProfileMetadata(root, {sourceRoot, workspaceOnly: true});
  assert.ok(graph.metadata.acyclic_profile_inputs_v1['helper/Cargo.toml']);
});

test('retained producer patches use their declared config and separate lock', t => {
  const sourceRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-patch-'));
  t.after(() => fs.rmSync(sourceRoot, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(sourceRoot, file)), {recursive: true}); fs.writeFileSync(path.join(sourceRoot, file), text);};
  write('owner/Cargo.toml', '[package]\nname="owner"\nversion="0.1.0"\n[dependencies]\nrenamed={package="helper",version="=0.1.0",optional=true}\n');
  write('owner/src/lib.rs', '');
  write('owner/Cargo.lock', 'original published lock');
  const producer = '.docs-producer/owner-0.1.0';
  const config = `${producer}/config.toml`, lock = `${producer}/Cargo.lock`;
  write(config, '[resolver]\nlockfile-path="owner-0.1.0/Cargo.lock"\n[patch.crates-io]\n"helper-0.1.0"={package="helper",path="owner-0.1.0/dependencies/helper-0.1.0"}\nunused={path="owner-0.1.0/dependencies/unused-0.1.0"}\n');
  write(lock, 'version=4\n[[package]]\nname="owner"\nversion="0.1.0"\ndependencies=["helper"]\n[[package]]\nname="helper"\nversion="0.1.0"\n');
  write(`${producer}/dependencies/helper-0.1.0/Cargo.toml`, '[package]\nname="helper"\nversion="0.1.0"\n');
  write(`${producer}/dependencies/helper-0.1.0/src/lib.rs`, '');
  write(`${producer}/dependencies/unused-0.1.0/Cargo.toml`, '[package]\nname="unused"\nversion="0.1.0"\n');
  write(`${producer}/dependencies/unused-0.1.0/src/lib.rs`, '');
  const universe = `${producer}/universe.json`;
  write(universe, JSON.stringify({conditions: {}, crates: {}}));
  const root = path.join(sourceRoot, 'owner'), options = {sourceRoot, patchConfig: config, lockFile: lock};
  const packages = workspacePackages(root, options);
  const owner = packages.find(p => p.name === 'owner');
  assert.equal(owner.dependencies[0].rename, 'renamed');
  assert.equal(owner.dependencies[0].resolved_path, path.join(sourceRoot, producer, 'dependencies/helper-0.1.0'));
  const graph = workspaceProfileMetadata(root, {...options, workspaceOnly: true});
  assert.deepEqual(graph.metadata.acyclic_profile_selection_v1, {patch_config: config, lock_file: lock});
  for (const input of [config, lock, 'owner/Cargo.lock', `${producer}/dependencies/helper-0.1.0/Cargo.toml`])
    assert.ok(graph.metadata.acyclic_profile_inputs_v1[input], input);
  const cli = fileURLToPath(new URL('./workspace-profile-metadata.mjs', import.meta.url));
  const declared = JSON.parse(execFileSync(process.execPath, [cli, sourceRoot, 'workspace', 'owner', config, lock], {encoding: 'utf8'}));
  assert.deepEqual(declared, graph);
  const full = workspaceProfileMetadata(root, {...options, universeFile: universe});
  assert.deepEqual(full.metadata.acyclic_profile_selection_v1, {patch_config: config, lock_file: lock});
  assert.deepEqual(full.packages.map(p => p.name), ['helper', 'owner']);
  assert.deepEqual(new Set(full.metadata.acyclic_profile_local_resolution_v1), new Set(full.packages.map(p => p.id)));
  assert.ok(full.metadata.acyclic_profile_inputs_v1[universe]);
  const declaredFull = JSON.parse(execFileSync(process.execPath, [cli, sourceRoot, 'all', 'owner', config, lock, universe], {encoding: 'utf8'}));
  assert.deepEqual(declaredFull, full);
  assert.throws(() => workspacePackages(root, {...options, patchConfig: '../foreign.toml'}), /escapes source root/);
});

test('retained lock reaches transitive patches without making patches workspace members', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-transitive-patch-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n[patch.crates-io]\ndirect={path="direct"}\ntransitive={path="transitive"}\nunused={path="unused"}\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nbridge="1"\ndirect="1"\n');
  for (const name of ['direct', 'transitive', 'unused']) {
    write(`${name}/Cargo.toml`, `[package]\nname="${name}"\nversion="1.0.0"\n[features]\nextra=[]\n[dev-dependencies]\nghost={path="../ghost"}\n`);
    write(`${name}/src/lib.rs`, '');
  }
  write('ghost/Cargo.toml', '[package]\nname="ghost"\nversion="1.0.0"\n');
  write('ghost/src/lib.rs', '');
  write('app/src/lib.rs', '');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["bridge", "direct"]\n[[package]]\nname="bridge"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\ndependencies=["transitive"]\n[[package]]\nname="direct"\nversion="1.0.0"\n[[package]]\nname="transitive"\nversion="1.0.0"\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions:{}, crates:{'bridge 1.0.0': {targets:[{Library:{crate_name:'bridge'}}], common_attrs:{deps:[{id:'transitive 1.0.0', target:'transitive'}]}}}}));
  const options = {sourceRoot:root, lockFile:'Cargo.lock'};
  const packages = workspacePackages(root, options);
  assert.deepEqual(packages.map(p => p.name), ['app', 'direct', 'transitive']);
  assert.deepEqual(packages.filter(p => p.workspace_member).map(p => p.name), ['app']);
  const workspace = workspaceProfileMetadata(root, {...options, workspaceOnly:true});
  assert.deepEqual(workspace.packages.map(p => p.name), ['app']);
  const graph = workspaceGraph(root, null, options);
  const node = name => graph.resolve.nodes.find(n => n.id === graph.packages.find(p => p.name === name).id);
  assert.deepEqual(node('bridge').deps.map(dep => dep.name), ['transitive']);
  assert.deepEqual(node('direct').features, []);
  assert.deepEqual(node('direct').deps, []);
  assert.deepEqual(workspaceProfileMetadata(root, options).packages.map(p => p.name), ['app', 'direct', 'transitive']);
});

test('a patch discovered before its path edge gains its workspace test dependencies', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-late-member-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\n[patch.crates-io]\npatched={path="patched"}\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="1.0.0"\n[dependencies]\nmiddle={path="../middle"}\n');
  write('middle/Cargo.toml', '[package]\nname="middle"\nversion="1.0.0"\n[dependencies]\npatched={path="../patched"}\n');
  write('patched/Cargo.toml', '[package]\nname="patched"\nversion="1.0.0"\n[dev-dependencies]\nhelper={path="../helper"}\n');
  write('helper/Cargo.toml', '[package]\nname="helper"\nversion="1.0.0"\n');
  for (const name of ['app', 'middle', 'patched', 'helper']) write(`${name}/src/lib.rs`, '');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="1.0.0"\ndependencies=["middle"]\n[[package]]\nname="middle"\nversion="1.0.0"\ndependencies=["patched"]\n[[package]]\nname="patched"\nversion="1.0.0"\ndependencies=["helper"]\n[[package]]\nname="helper"\nversion="1.0.0"\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions:{}, crates:{}}));
  const graph = workspaceGraph(root);
  assert.deepEqual(graph.packages.filter(p => p.workspace_member).map(p => p.name), ['app', 'helper', 'middle', 'patched']);
  const patched = graph.packages.find(p => p.name === 'patched');
  assert.deepEqual(graph.resolve.nodes.find(n => n.id === patched.id).deps.map(d => d.name), ['helper']);
});

test('workspace discovery includes implicit path members and respects excluded subtrees', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'workspace-members-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (file, text) => {fs.mkdirSync(path.dirname(path.join(root, file)), {recursive: true}); fs.writeFileSync(path.join(root, file), text);};
  write('Cargo.toml', '[workspace]\nmembers=["app"]\nexclude=["vendor/*"]\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nimplicit={path="../implicit"}\nexternal={path="../vendor/external"}\n');
  write('implicit/Cargo.toml', '[package]\nname="implicit"\nversion="0.1.0"\n');
  write('vendor/external/Cargo.toml', '[package]\nname="external"\nversion="0.1.0"\n');
  const packages = workspacePackages(root);
  assert.deepEqual(packages.filter(p => p.workspace_member).map(p => p.name), ['app', 'implicit']);
  assert.equal(packages.find(p => p.name === 'external').workspace_member, false);
  assert.deepEqual(workspaceProfileMetadata(root, {workspaceOnly: true}).packages.map(p => p.name), ['app', 'implicit']);
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
  write('Cargo.toml', '[workspace]\nmembers=["app"]\nexclude=["helper"]\n');
  write('app/Cargo.toml', '[package]\nname="app"\nversion="0.1.0"\n[dependencies]\nhelper={path="../helper",default-features=false}\n[target.\'cfg(target_arch = "wasm32")\'.dependencies]\nwasm="1"\n');
  write('helper/Cargo.toml', '[package]\nname="helper"\nversion="0.1.0"\n[dependencies]\nunused={version="1",optional=true}\n[features]\ndefault=["dep:unused"]\n');
  write('Cargo.lock', 'version=4\n[[package]]\nname="app"\nversion="0.1.0"\ndependencies=["helper", "wasm"]\n[[package]]\nname="helper"\nversion="0.1.0"\ndependencies=["unused"]\n[[package]]\nname="wasm"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n[[package]]\nname="unused"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
  write('cargo-bazel-lock.json', JSON.stringify({conditions: {}, crates: {}}));
  assert.doesNotThrow(() => assertRegistryPinsCurrent(root));
  const browser = workspaceGraph(root, 'wasm32-unknown-unknown');
  const app = browser.packages.find(pkg => pkg.name === 'app');
  assert.deepEqual(browser.resolve.nodes.find(node => node.id === app.id).deps.map(dep => dep.name), ['helper', 'wasm']);
});
