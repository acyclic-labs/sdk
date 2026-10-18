import assert from 'node:assert/strict';
import test from 'node:test';
import {checks} from './target-features.mjs';

function metadata(targets, overrides = {}) {
  return {workspace_members: ['root'], packages: [{id: 'root', name: 'root', targets, ...overrides}]};
}
const lib = {name: 'root', kind: ['lib']};
const target = (name, kind, features = []) => ({name, kind: [kind], 'required-features': features});

test('production and dev-dependency closures stay separate for each declared feature group', () => {
  const plan = checks(metadata([lib, target('plain', 'test'), target('native', 'test', ['grpc']), target('other', 'test', ['grpc'])]));
  assert.deepEqual(plan.map(item => item.args), [
    ['check', '--locked', '-p', 'root', '--lib'],
    ['check', '--locked', '-p', 'root', '--lib', '--tests', '--test', 'plain'],
    ['check', '--locked', '-p', 'root', '--lib', '--features', 'grpc'],
    ['check', '--locked', '-p', 'root', '--lib', '--tests', '--test', 'native', '--test', 'other', '--features', 'grpc'],
  ]);
});

test('binary selectors never share a command with dev-dependency targets', () => {
  const plan = checks(metadata([target('sample', 'example', ['grpc']), target('speed', 'bench', ['grpc']), target('tool', 'bin', ['grpc'])]));
  assert.deepEqual(plan.map(item => item.args), [
    ['check', '--locked', '-p', 'root', '--bin', 'tool', '--features', 'grpc'],
    ['check', '--locked', '-p', 'root', '--tests', '--example', 'sample', '--bench', 'speed', '--features', 'grpc'],
  ]);
  assert.ok(plan.filter(item => item.stage === 'production').every(item => !item.args.some(arg => ['--tests', '--test', '--bench', '--example'].includes(arg))));
});

test('every Cargo library kind receives both normal and test qualification', () => {
  for (const kind of ['lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro']) {
    const plan = checks(metadata([{name: 'root', kind: [kind]}]));
    assert.deepEqual(plan.map(item => item.args.slice(4)), [['--lib'], ['--lib', '--tests']], kind);
  }
  assert.equal(checks(metadata([{name: 'root', kind: ['rlib', 'cdylib']}])).length, 2);
});

test('unrecognized targets, uncovered build scripts and missing selected packages fail closed', () => {
  assert.throws(() => checks(metadata([target('new', 'future-library')])), /unrecognized Cargo target/);
  assert.throws(() => checks(metadata([target('build', 'custom-build')])), /no compiler coverage/);
  assert.throws(() => checks(metadata([lib]), ['missing']), /missing from metadata/);
  const plan = checks(metadata([lib, target('build', 'custom-build')]));
  assert.equal(plan.length, 2);
});

test('feature grouping is order independent and never enables an undeclared feature', () => {
  const plan = checks(metadata([lib, target('first', 'test', ['b', 'a', 'a']), target('second', 'test', ['a', 'b'])]));
  assert.deepEqual(plan.map(item => item.features), [[], [], ['a', 'b'], ['a', 'b']]);
  assert.ok(plan.every(item => !item.args.includes('--all-features') && !item.args.includes('--no-default-features')));
});

test('only workspace members are selected and direct dependencies are left to Cargo', () => {
  const input = metadata([lib]);
  input.packages.push({id: 'external', name: 'external', targets: [lib]});
  input.packages[0].dependencies = [{name: 'helper', optional: true}, {name: 'bridge', optional: false}];
  input.resolve = {nodes: [{id: 'bridge', dependencies: ['helper']}]};
  assert.deepEqual([...new Set(checks(input).map(item => item.package))], ['root']);
  assert.deepEqual(checks(input, []).map(item => item.package), []);
});

test('explicit selectors cover targets disabled from Cargo default test selection', () => {
  const disabled = {...target('disabled', 'test'), test: false};
  const bin = {...target('tool', 'bin'), test: false};
  const plan = checks(metadata([disabled, bin]));
  assert.ok(plan.find(item => item.stage === 'production').args.includes('--bin'));
  const tests = plan.find(item => item.stage === 'tests').args;
  assert.equal(tests[tests.indexOf('--test') + 1], 'disabled');
});
