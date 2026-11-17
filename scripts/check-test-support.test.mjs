import assert from 'node:assert/strict';
import test from 'node:test';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {checkDeserializedTestSupport, checkLoomOptIn, checkNoLoomCfgFlag, checkTestSupport, checkTestSupportExports} from './check-test-support.mjs';

/** @param {{optional?: boolean, kind?: string | null, target?: string | null, alias?: string}} options */
function fixture({optional = false, kind = null, target = null, alias = 'backend-alias'} = {}) {
  const features = () => /** @type {Record<string, string[]>} */ ({'test-support': []});
  const dependency = {id: 'backend@1', name: 'backend', features: features()};
  const consumer = {id: 'consumer@1', name: 'consumer', features: features(),
    dependencies: [{name: 'backend', rename: alias, optional, kind, target,
      features: /** @type {string[]} */ ([])}]};
  const node = {id: consumer.id, features: /** @type {string[]} */ ([]), deps: [{name: alias.replaceAll('-', '_'), pkg: dependency.id,
    dep_kinds: [{kind, target}]}]};
  return {packages: [consumer, dependency], workspace_members: [consumer.id],
    resolve: {nodes: [node]}, consumer, dependency, node};
}

test('missing forwarding fails for normal, optional, build and foreign-target dependencies', () => {
  for (const options of [{}, {optional: true}, {kind: 'build'}, {target: 'cfg(windows)'}]) {
    const metadata = fixture(options);
    assert.throws(() => checkTestSupport(metadata), /consumer: test-support must forward backend-alias\??\/test-support/);
    metadata.consumer.features['test-support'].push(`backend-alias${options.optional ? '?' : ''}/test-support`);
    assert.equal(checkTestSupport(metadata), 1);
  }
});

/** @param {{dependency?: 'optional' | 'required' | 'dev' | null, feature?: string[] | null, script?: string | null, name?: string}} options */
function loomPackage({dependency = 'optional', feature = ['dep:loom'], script = 'fn main() {}', name = 'models'} = {}) {
  const dependencies = {optional: [{name: 'loom', kind: null, optional: true}], required: [{name: 'loom', kind: null, optional: false}],
    dev: [{name: 'loom', kind: 'dev', optional: false}]}[dependency ?? ''] ?? [];
  const pkg = {id: `${name}@1`, name, manifest_path: `/w/crates/${name}/Cargo.toml`,
    features: feature ? {'model-checking': feature} : {}, dependencies,
    targets: [{kind: ['lib'], src_path: `/w/crates/${name}/src/lib.rs`},
      ...(script === null ? [] : [{kind: ['custom-build'], src_path: `/w/crates/${name}/build.rs`}])]};
  const inner = {id: 'inner@1', name: 'inner', manifest_path: '/w/crates/models/inner/Cargo.toml',
    features: {}, dependencies: [], targets: []};
  return /** @type {const} */ ([{workspace_root: '/w', workspace_members: [pkg.id, inner.id], packages: [pkg, inner]}, () => script]);
}
const stanza = readFileSync('rust/crates/mcp/build.rs', 'utf8');
/** @param {Parameters<typeof loomPackage>[0]} options @param {[string, string][]} sources */
function optIn(options, sources) {
  const [metadata, read] = loomPackage(options);
  return checkLoomOptIn(metadata, sources, read);
}

test('a package with loom code and a loom dependency opts in only through the feature and its build script', () => {
  const sources = [['crates/models/src/sync.rs', '#[cfg(all(test, not(loom)))]\nmod plain;\n']];
  assert.throws(() => optIn({}, sources), /models: its build script must emit cfg\(loom\)/);
  assert.throws(() => optIn({script: null}, sources), /models: its build script/);
  assert.throws(() => optIn({script: stanza, feature: null}, sources), /models: its model-checking feature must enable dep:loom/);
  assert.throws(() => optIn({script: stanza, feature: []}, sources), /models: its model-checking feature must enable dep:loom/);
  for (const dependency of /** @type {const} */ (['required', 'dev']))
    assert.throws(() => optIn({script: stanza, dependency}, sources), /models: loom must be an optional normal dependency/, dependency);
  assert.equal(optIn({script: stanza}, sources), 1);
  assert.equal(optIn({script: stanza.replaceAll('cargo:', 'cargo::')}, sources), 1);
  // Without a loom dependency the branches compile only where another package includes the file.
  assert.equal(optIn({dependency: null}, sources), 0);
  // A nested package owns its own files, and loom inside a string is not a cfg.
  assert.equal(optIn({}, [['crates/models/inner/src/lib.rs', '#[cfg(loom)]\nmod m;\n'],
    ['crates/models/src/lib.rs', 'const S: &str = "#[cfg(loom)]";\n#[cfg(feature = "loom")]\nmod m;\n']]), 0);
});

test('removing the stanza from a real model package is rejected', () => {
  const real = readFileSync('rust/crates/stream/build.rs', 'utf8');
  const sources = [['crates/models/src/lib.rs', '#[cfg(loom)]\nuse loom::sync::Mutex;\n']];
  assert.equal(optIn({script: real}, sources), 1);
  const stripped = real.replace('println!("cargo:rustc-cfg=loom");', '');
  assert.notEqual(stripped, real);
  assert.throws(() => optIn({script: stripped}, sources), /must emit cfg\(loom\)/);
});

test('no command passes the loom cfg by hand, and the removed wrapper and target-cfg forms are rejected', () => {
  // Spelled apart so this command file does not itself pass the flag.
  const flag = ['-', '-', 'cfg'].join('');
  for (const line of [`cargo rustc --test t -- ${flag} loom`, `RUSTFLAGS="${flag}=loom" cargo test`,
    `["--", "${flag}", "loom"]`, `['${flag}', 'loom']`])
    assert.throws(() => checkNoLoomCfgFlag([['run.sh', `${line}\n`]]), /run.sh:1: set ACYCLIC_LOOM=1/, line);
  for (const line of ['ACYCLIC_LOOM=1 cargo test --features model-checking', `${flag} loomish`, `${flag} 'feature="loom"'`])
    assert.equal(checkNoLoomCfgFlag([['run.sh', `${line}\n`]]), 1, line);
  const file = 'scripts/qualify-mcp-loom.mjs';
  const real = readFileSync(file, 'utf8');
  assert.equal(checkNoLoomCfgFlag([[file, real]]), 1);
  const restored = real.replace('"--no-run",', `"--no-run", "--", "${flag}", "loom",`);
  assert.notEqual(restored, real);
  assert.throws(() => checkNoLoomCfgFlag([[file, restored]]), /qualify-mcp-loom.mjs:\d+: set ACYCLIC_LOOM=1/);
  // The deleted workspace wrapper, restored as it was: it appended the cfg to every opted-in rustc call.
  const wrapper = 'def loom_args(args):\n' +
    `    if 'feature="model-checking"' in args and "${flag}" in args:\n` +
    `        return [*args, "${flag}", "loom"]\n    return args\n`;
  assert.throws(() => checkNoLoomCfgFlag([['scripts/loom-rustc.py', wrapper]]), /^Error: scripts\/loom-rustc.py:3: set ACYCLIC_LOOM=1[^\n]*$/);
});

test('local feature chains forward, including cycles, but unrelated enabled features do not', () => {
  const metadata = fixture({optional: true});
  metadata.consumer.features['test-support'] = ['fixtures'];
  metadata.consumer.features.fixtures = ['test-support', 'backend-alias?/test-support'];
  assert.equal(checkTestSupport(metadata), 1);
  metadata.consumer.features['test-support'] = [];
  metadata.node.features = ['test-support', 'fixtures'];
  assert.throws(() => checkTestSupport(metadata), /must forward/);
  const implicit = fixture({alias: 'constructor', optional: true});
  implicit.consumer.features['test-support'] = ['constructor', 'constructor?/test-support'];
  assert.equal(checkTestSupport(implicit), 1);
});

test('dev dependency unification cannot conceal a missing production forwarding edge', () => {
  const metadata = fixture();
  metadata.consumer.dependencies.push({...metadata.consumer.dependencies[0], kind: 'dev', features: ['test-support']});
  metadata.node.deps[0].dep_kinds.push({kind: 'dev', target: null});
  assert.throws(() => checkTestSupport(metadata), /must forward/);
  assert.equal(checkTestSupport(fixture({kind: 'dev'})), 0);
});

test('checks only workspace features, and resolves the exact package version behind an alias', () => {
  const metadata = fixture({alias: 'renamed_backend'});
  metadata.packages.push({id: 'backend@2', name: 'backend', features: {}});
  assert.throws(() => checkTestSupport(metadata), /renamed_backend\/test-support/);
  delete metadata.dependency.features['test-support'];
  assert.equal(checkTestSupport(metadata), 0);
  delete metadata.consumer.features['test-support'];
  assert.equal(checkTestSupport(metadata), 0);
  assert.throws(() => checkTestSupport({...metadata, resolve: null}), /resolved Cargo metadata/);
});

test('deserialized request members never exist only under test-support', () => {
  const request = member => ['request.rs', [
    '#[derive(Clone, Debug, Serialize,', '    Deserialize)]', '#[serde(tag = "kind")]', 'pub enum Request {',
    '    Shipped { task: String },', ...member, '    Last {', '        nested: Option<u64>,', '    },', '}',
    '#[cfg(feature = "test-support")]', 'pub fn helper() {}'].join('\n')];
  assert.equal(checkDeserializedTestSupport([request([])]), 1);
  for (const attribute of ['#[cfg(feature = "test-support")]', '#[cfg(any(test, feature = "test-support"))]',
    '#[cfg_attr(not(feature = "test-support"), serde(skip))]'])
    assert.throws(() => checkDeserializedTestSupport([request([attribute, '    Probe { agents: u64 },'])]),
      /request\.rs:6: Request has a test-support-only member/);
  // A field of a struct variant is a member too.
  assert.throws(() => checkDeserializedTestSupport([request(['    Probe {', '        #[cfg(feature = "test-support")]',
    '        agents: u64,', '    },'])]), /request\.rs:7: Request has a test-support-only member/);
  // Serialize-only types and items outside a deserialized body are not requests.
  const output = ['#[derive(Serialize)]', 'pub struct Reply {', '    #[cfg(feature = "test-support")]',
    '    probe: u64,', '}'].join('\n');
  assert.equal(checkDeserializedTestSupport([['reply.rs', output]]), 1);
});

test('test-visible re-exports require test-visible targets, including gated parent modules', () => {
  const fixture = (target, exported = '') => [
    ['demo/src/lib.rs', '#[cfg(any(test, feature = "test-support"))]\npub mod test_support;\nmod answer;'],
    ['demo/src/answer.rs', `${target}\npub mod validation;`],
    ['demo/src/test_support/mod.rs', `${exported}\npub use crate::answer::{validation as native_answer};`],
  ];
  for (const cfg of ['#[cfg(feature = "test-support")]',
    '#[cfg(all(\n feature = "test-support", not(target_arch = "wasm32")\n))]',
    '#[cfg(any(not(test), feature = "test-support"))]']) {
    assert.throws(() => checkTestSupportExports(fixture(cfg)), /demo\/src\/answer.rs::validation.*requires test-support/);
    assert.equal(checkTestSupportExports(fixture(cfg, '#[cfg(feature = "test-support")]')), 0);
  }
  assert.equal(checkTestSupportExports(fixture('#[cfg(any(test, feature = "test-support"))]')), 1);
  // A platform predicate on the export must not conceal a feature-only target.
  for (const cfg of ['#[cfg(windows)]', '#[cfg(not(test = "other"))]', '#[cfg(not(all(test, windows)))]'])
    assert.throws(() => checkTestSupportExports(fixture('#[cfg(feature = "test-support")]', cfg)), /requires test-support/);
  const sources = fixture('');
  sources[0][1] = '#[cfg(any(test, feature = "test-support"))]\npub mod test_support;\n#[cfg(feature = "test-support")]\nmod answer;';
  assert.throws(() => checkTestSupportExports(sources), /demo\/src\/lib.rs::answer.*requires test-support/);
  sources[0][1] = '#[cfg(feature = "test-support")]\npub mod test_support;\nmod answer;';
  sources[1][1] = '#[cfg(feature = "test-support")]\npub mod validation;';
  assert.equal(checkTestSupportExports(sources), 1);
});

test('workspace exports pass and restoring the original answer cfg is rejected', () => {
  const files = execFileSync('git', ['ls-files', '--', '*.rs'], {encoding: 'utf8'}).split('\n').filter(Boolean);
  const sources = files.map(file => [file, readFileSync(file, 'utf8')]);
  assert.ok(checkTestSupportExports(sources) > 0, 'workspace re-export inventory must not be empty');
  const answer = sources.find(([file]) => file === 'rust/crates/harness-local/src/local/host_process/children/answer.rs');
  assert.ok(answer);
  assert.match(answer[1], /#\[cfg\(any\(test, feature = "test-support"\)\)\]/);
  answer[1] = answer[1].replace('#[cfg(any(test, feature = "test-support"))]', '#[cfg(feature = "test-support")]');
  assert.throws(() => checkTestSupportExports(sources), /answer.rs::validation.*requires test-support/);
});
