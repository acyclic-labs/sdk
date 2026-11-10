import assert from 'node:assert/strict';
import test from 'node:test';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {checkDeserializedTestSupport, checkTestSupport, checkTestSupportExports} from './check-test-support.mjs';

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

test('local feature chains forward, including cycles, but unrelated enabled features do not', () => {
  const metadata = fixture({optional: true});
  metadata.consumer.features['test-support'] = ['fixtures'];
  metadata.consumer.features.fixtures = ['test-support', 'backend-alias?/test-support'];
  assert.equal(checkTestSupport(metadata), 1);
  metadata.consumer.features['test-support'] = [];
  metadata.node.features = ['test-support', 'fixtures'];
  assert.throws(() => checkTestSupport(metadata), /must forward/);
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
  assert.throws(() => checkTestSupportExports(fixture('#[cfg(feature = "test-support")]', '#[cfg(windows)]')),
    /requires test-support/);
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
  const answer = sources.find(([file]) => file === 'rust/crates/harness/src/local/host_process/children/answer.rs');
  assert.ok(answer);
  assert.match(answer[1], /#\[cfg\(any\(test, feature = "test-support"\)\)\]/);
  answer[1] = answer[1].replace('#[cfg(any(test, feature = "test-support"))]', '#[cfg(feature = "test-support")]');
  assert.throws(() => checkTestSupportExports(sources), /answer.rs::validation.*requires test-support/);
});
