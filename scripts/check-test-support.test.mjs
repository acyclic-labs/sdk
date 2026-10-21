import assert from 'node:assert/strict';
import test from 'node:test';
import {checkTestSupport} from './check-test-support.mjs';

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
