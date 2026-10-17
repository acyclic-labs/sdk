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
