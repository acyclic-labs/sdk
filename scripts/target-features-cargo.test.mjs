// Run on bb1 / the qualification lane: these controls use the actual compiler.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {checks} from './target-features.mjs';

function cargo(root, args) {
  const result = spawnSync('cargo', args, {cwd: root, encoding: 'utf8', env: {...process.env, CARGO_NET_OFFLINE: 'true'}, maxBuffer: 1 << 24});
  if (result.error) throw result.error;
  assert.equal(result.signal, null, result.stderr);
  return result;
}

function fixture(t) {
  const root = fs.mkdtempSync(path.join(process.cwd(), '.target-feature-fixture-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const write = (name, source) => {
    const file = path.join(root, name);
    fs.mkdirSync(path.dirname(file), {recursive: true});
    fs.writeFileSync(file, source);
  };
  write('helper/Cargo.toml', '[package]\nname="helper"\nversion="0.0.0"\nedition="2024"\n');
  write('helper/src/lib.rs', 'pub struct Plain;');
  write('bridge/Cargo.toml', '[package]\nname="bridge"\nversion="0.0.0"\nedition="2024"\n[dependencies]\nhelper={path="../helper"}\n');
  write('bridge/src/lib.rs', 'pub fn plain() -> helper::Plain { helper::Plain }');
  write('src/lib.rs', 'pub mod api;');
  write('src/api.rs', '#![cfg(feature="grpc")]\npub fn connect() {}');
  const manifest = required => `[package]
name="fixture"
version="0.0.0"
edition="2024"
autotests=false
[workspace]
members=["helper","bridge"]
[features]
default=[]
grpc=["dep:helper"]
[dependencies]
helper={path="helper",optional=true}
bridge={path="bridge"}
[[test]]
name="target"
path="tests/target.rs"
required-features=${JSON.stringify(required)}
`;
  write('Cargo.toml', manifest([]));
  const lock = cargo(root, ['generate-lockfile', '--offline']);
  assert.equal(lock.status, 0, lock.stderr);
  return {root, write, manifest};
}

function compare(fixture, source, declared, expected) {
  fixture.write('Cargo.toml', fixture.manifest(declared ? ['grpc'] : []));
  fixture.write('tests/target.rs', source);
  const meta = cargo(fixture.root, ['metadata', '--offline', '--locked', '--no-deps', '--format-version', '1']);
  assert.equal(meta.status, 0, meta.stderr);
  const metadata = JSON.parse(meta.stdout);
  const root = metadata.packages.find(pkg => pkg.name === 'fixture');
  const plan = checks(metadata, [root.id]);
  assert.ok(plan.length > 0);
  const planned = plan.map(command => cargo(fixture.root, command.args));
  const direct = cargo(fixture.root, ['check', '--offline', '--locked', '-p', 'fixture', '--lib', '--tests', ...(declared ? ['--features', 'grpc'] : [])]);
  assert.equal(direct.status === 0, expected, direct.stderr);
  assert.equal(planned.every(result => result.status === 0), direct.status === 0, planned.map(result => result.stderr).join('\n'));
  if (!expected) assert.match(direct.stderr, /E0432|E0433/);
}

test('Cargo rejects the direct optional-edge and inner-cfg reproductions and accepts declared fixes', t => {
  const workspace = fixture(t);
  for (const source of ['use helper::Plain; #[test] fn ready() {}', 'use fixture::api::connect; #[test] fn ready() {}']) {
    compare(workspace, source, false, false);
    compare(workspace, source, true, true);
  }
});

test('large unexpanded macro token trees never enter a source scanner', t => {
  const workspace = fixture(t);
  for (const bytes of [4096, 16384]) {
    const source = `macro_rules! unused { () => { ${'helper::Plain;'.repeat(Math.ceil(bytes / 14))} } } #[test] fn ready() {}`;
    compare(workspace, source, false, true);
  }
});

test('seeded target/feature property agrees with Cargo across shadowing and cfg_attr', t => {
  const workspace = fixture(t);
  let seed = 0x862946;
  const next = () => { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; return seed >>> 0; };
  const cases = [
    {source: 'use helper::Plain; #[test] fn ready() {}', gated: true},
    {source: 'use fixture::api::connect; #[test] fn ready() {}', gated: true},
    {source: 'mod helper { pub struct Plain; } use helper::Plain; #[test] fn ready() {}', gated: false},
    {source: '#[cfg_attr(not(feature="grpc"), cfg(any()))] fn absent() { helper::Plain; } #[test] fn ready() {}', gated: false},
  ];
  for (let index = 0; index < 6; index++) {
    const selected = cases[next() % cases.length];
    const declared = Boolean(next() & 1);
    compare(workspace, selected.source, declared, declared || !selected.gated);
  }
});

function planFor(workspace) {
  const meta = cargo(workspace.root, ['metadata', '--offline', '--locked', '--no-deps', '--format-version', '1']);
  assert.equal(meta.status, 0, meta.stderr);
  const metadata = JSON.parse(meta.stdout);
  return {metadata, plan: checks(metadata)};
}

test('dev features cannot hide a broken production binary or library', t => {
  const workspace = fixture(t);
  workspace.write('tests/target.rs', '#[test] fn ready() {}');
  const manifest = required => workspace.manifest([]) + `
[dev-dependencies]
fixture={path=".",features=["grpc"]}
[[bin]]
name="probe"
path="src/main.rs"
required-features=${JSON.stringify(required)}
`;
  workspace.write('Cargo.toml', manifest([]));
  workspace.write('src/lib.rs', '#[cfg(feature="grpc")] pub fn connect() {}');
  workspace.write('src/main.rs', 'fn main() { fixture::connect(); }');
  assert.equal(cargo(workspace.root, ['generate-lockfile', '--offline']).status, 0);
  const contaminated = cargo(workspace.root, ['check', '--locked', '-p', 'fixture', '--lib', '--tests', '--bin', 'probe']);
  assert.equal(contaminated.status, 0, contaminated.stderr);
  const direct = cargo(workspace.root, ['check', '--locked', '-p', 'fixture', '--bin', 'probe']);
  assert.notEqual(direct.status, 0);
  assert.match(direct.stderr, /E0425/);
  const broken = planFor(workspace).plan.filter(command => command.package === 'fixture');
  assert.ok(broken.some(command => command.stage === 'production' && command.args.includes('--bin')));
  assert.ok(broken.filter(command => command.stage === 'production')
    .some(command => cargo(workspace.root, command.args).status !== 0));
  workspace.write('Cargo.toml', manifest(['grpc']));
  assert.ok(planFor(workspace).plan.every(command => cargo(workspace.root, command.args).status === 0));
  // The same distinction is required for a library even when no binary uses it.
  workspace.write('src/lib.rs', '#[cfg(feature="grpc")] pub fn connect() {} pub fn call() { connect(); }');
  const library = planFor(workspace).plan.find(command => command.package === 'fixture'
    && command.stage === 'production' && command.features.length === 0);
  const devLibrary = cargo(workspace.root, ['check', '--locked', '-p', 'fixture', '--lib', '--tests']);
  assert.equal(devLibrary.status, 0, devLibrary.stderr);
  const directLibrary = cargo(workspace.root, ['check', '--locked', '-p', 'fixture', '--lib']);
  assert.notEqual(directLibrary.status, 0);
  assert.match(directLibrary.stderr, /E0425/);
  const result = cargo(workspace.root, library.args);
  assert.equal(result.status, directLibrary.status);
  assert.match(result.stderr, /E0425/);
});

test('an invalid nonstandard library cannot disappear in a mixed workspace', t => {
  const workspace = fixture(t);
  workspace.write('Cargo.toml', workspace.manifest([]).replace('members=["helper","bridge"]', 'members=["helper","bridge","dynamic"]'));
  workspace.write('tests/target.rs', '#[test] fn ready() {}');
  workspace.write('dynamic/src/lib.rs', 'pub fn broken() { missing_symbol(); }');
  for (const kind of ['cdylib', 'staticlib', 'dylib']) {
    workspace.write('dynamic/Cargo.toml', `[package]\nname="dynamic"\nversion="0.0.0"\nedition="2024"\n[lib]\ncrate-type=["${kind}"]\n`);
    assert.equal(cargo(workspace.root, ['generate-lockfile', '--offline']).status, 0);
    const {metadata, plan} = planFor(workspace);
    assert.ok(metadata.packages.find(pkg => pkg.name === 'dynamic').targets.some(target => target.kind.includes(kind)));
    assert.deepEqual(new Set(plan.map(command => command.package)), new Set(metadata.packages.map(pkg => pkg.name)));
    const selected = plan.find(command => command.package === 'dynamic' && command.stage === 'production');
    assert.ok(selected, kind);
    const result = cargo(workspace.root, selected.args);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /E0425/);
    if (kind === 'cdylib') {
      for (const command of plan.filter(command => command.package !== 'dynamic')) {
        const healthy = cargo(workspace.root, command.args);
        assert.equal(healthy.status, 0, healthy.stderr);
      }
    }
  }
});
