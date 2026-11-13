import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import test from 'node:test';
import {workflowExecution} from './static-policy.mjs';

const workflow = job => 'jobs:\n  build:\n' + job.split('\n').map(line => '    ' + line).join('\n');

test('parsed runner selections include aliases, arbitrary matrix keys and nested axes', () => {
  for (const [source, expected] of [
    [workflow('"runs-on": &host ubuntu-24.04\nsteps: [{run: "echo self-hosted runner: macos-unapproved"}]'), ['ubuntu-24.04']],
    ['jobs: {a: {runs-on: &host ubuntu-24.04}, b: {runs-on: *host}}', ['ubuntu-24.04']],
    [workflow('runs-on: ${{ matrix.os }}\nstrategy: {matrix: {os: [ubuntu-24.04, macos-15]}}'), ['ubuntu-24.04', 'macos-15']],
    [workflow('runs-on: ${{ matrix.platform.runner }}\nstrategy: {matrix: {platform: [{runner: windows-2025}, {runner: kaizen-linux}]}}'), ['windows-2025', 'kaizen-linux']],
    [workflow('runs-on: ${{ matrix.target }}\nstrategy: {matrix: {include: [{target: ubuntu-24.04}, {target: macos-15}]}}'), ['ubuntu-24.04', 'macos-15']],
    [workflow('runs-on: {labels: [kaizen-linux]}'), ['kaizen-linux']],
  ]) assert.deepEqual([...workflowExecution('workflow.yml', source)], expected);
  const source = workflow('runs-on: ${{ matrix.host }}\nstrategy: {matrix: {include: "${{ fromJSON(needs.build.outputs.hosts) }}"}}');
  const rows = new Map([['${{ fromJSON(needs.build.outputs.hosts) }}', [{host: 'macos-15'}]]]);
  assert.deepEqual([...workflowExecution('workflow.yml', source, rows)], ['macos-15']);
  assert.throws(() => workflowExecution('workflow.yml', source), /unresolved matrix include/);
});

test('unresolved selectors and real dynamic shells and permissions fail closed', () => {
  for (const job of [
    'runs-on: ${{ inputs.runner }}',
    'runs-on: ${{ matrix.absent }}\nstrategy: {matrix: {os: [ubuntu-24.04]}}',
    'runs-on: ${{ matrix.os }}\nstrategy: {matrix: {os: ["${{ inputs.runner }}"]}}',
    'runs-on: {group: any, labels: ubuntu-24.04}',
    'runs-on: []',
    'runs-on: ubuntu-24.04\ndefaults: {run: {shell: "${{ matrix.shell }}"}}',
    'runs-on: ubuntu-24.04\nsteps: [{shell: "${{ inputs.shell }}", run: build}]',
    'runs-on: ubuntu-24.04\npermissions: {unknown: read}',
    'runs-on: ubuntu-24.04\npermissions: {contents: arbitrary}',
  ]) assert.throws(() => workflowExecution('workflow.yml', workflow(job)), /workflow.yml:/);
  assert.throws(() => workflowExecution('workflow.yml', 'jobs: {a: {runs-on: ubuntu-24.04, runs-on: macos-15}}'));
  assert.deepEqual([...workflowExecution('workflow.yml', workflow('runs-on: ubuntu-24.04\nsteps: [{run: "echo shell: ${{ inputs.shell }}; unknown: write"}]'))], ['ubuntu-24.04']);
});

test('the shipping checker refuses an unapproved runner regardless of YAML spelling', () => {
  const root = mkdtempSync(join(tmpdir(), 'sdk-workflow-policy-'));
  const directory = join(root, '.github/workflows');
  mkdirSync(directory, {recursive: true});
  writeFileSync(join(root, '.github/qualification-lanes.json'), '[{"lane":"example","runner":"kaizen-linux","workers":8}]');
  const check = source => {
    writeFileSync(join(directory, 'example.yml'), source);
    const result = spawnSync(process.execPath, [resolve('scripts/check-workflow-runners.mjs')], {cwd: root, encoding: 'utf8'});
    assert.ifError(result.error);
    return result;
  };
  try {
    assert.equal(check(workflow('runs-on: ubuntu-24.04')).status, 0);
    for (const source of [
      workflow('runs-on: unexpected-linux'),
      workflow('runs-on: [self-hosted, linux]'),
      workflow('runs-on: ${{ matrix.os }}\nstrategy: {matrix: {os: [ubuntu-24.04, unexpected-linux]}}'),
      workflow('runs-on: ${{ matrix.platform.runner }}\nstrategy: {matrix: {platform: [{runner: unexpected-linux}]}}'),
      workflow('runs-on: ${{ matrix.target }}\nstrategy: {matrix: {include: [{target: unexpected-linux}]}}'),
    ]) {
      const result = check(source);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /uses unapproved runner/);
    }
  } finally { rmSync(root, {recursive: true, force: true}); }
});
