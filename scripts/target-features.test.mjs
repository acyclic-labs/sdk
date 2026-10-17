import assert from 'node:assert/strict';
import test from 'node:test';
import {checks} from './target-features.mjs';

function metadata(targets, overrides = {}) {
  return {workspace_members: ['root'], packages: [{id: 'root', name: 'root', targets, ...overrides}]};
}
const lib = {name: 'root', kind: ['lib']};
const target = (name, kind, features = []) => ({name, kind: [kind], 'required-features': features});

test('Cargo checks the default lib/test closure and each distinct declared requirement', () => {
  const plan = checks(metadata([lib, target('plain', 'test'), target('native', 'test', ['grpc']), target('other', 'test', ['grpc'])]));
  assert.deepEqual(plan.map(item => item.args), [
    ['check', '--locked', '-p', 'root', '--lib', '--tests'],
    ['check', '--locked', '-p', 'root', '--lib', '--tests', '--features', 'grpc'],
  ]);
});

test('examples, benches and bins retain their exact target selectors', () => {
  const plan = checks(metadata([target('sample', 'example', ['grpc']), target('speed', 'bench', ['grpc']), target('tool', 'bin', ['grpc'])]));
  assert.deepEqual(plan.map(item => item.args), [[
    'check', '--locked', '-p', 'root', '--example', 'sample', '--bench', 'speed', '--bin', 'tool', '--features', 'grpc',
  ]]);
});

test('feature grouping is order independent and never enables an undeclared feature', () => {
  const plan = checks(metadata([lib, target('first', 'test', ['b', 'a', 'a']), target('second', 'test', ['a', 'b'])]));
  assert.deepEqual(plan.map(item => item.features), [[], ['a', 'b']]);
  assert.ok(plan.every(item => !item.args.includes('--all-features') && !item.args.includes('--no-default-features')));
});

test('only workspace members are selected and direct dependencies are left to Cargo', () => {
  const input = metadata([lib]);
  input.packages.push({id: 'external', name: 'external', targets: [lib]});
  input.packages[0].dependencies = [{name: 'helper', optional: true}, {name: 'bridge', optional: false}];
  input.resolve = {nodes: [{id: 'bridge', dependencies: ['helper']}]};
  assert.deepEqual(checks(input).map(item => item.package), ['root']);
  assert.deepEqual(checks(input, []).map(item => item.package), []);
});
