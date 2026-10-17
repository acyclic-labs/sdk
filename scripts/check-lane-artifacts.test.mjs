import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import test from 'node:test';
import YAML from './vendor/yaml.cjs';

const checker = resolve('scripts/check-lane-artifacts.mjs');

test('every PR and main push runs the real changed-path gate with event-bound commits', () => {
  const workflow = YAML.parse(readFileSync('.github/workflows/static.yml', 'utf8'));
  assert.equal(workflow.on.pull_request, null);
  assert.deepEqual(workflow.on.push, {branches: ['main']});
  const job = workflow.jobs.static;
  assert.equal(job.if, undefined);
  assert.equal(job.steps[0].with['fetch-depth'] ?? 1, 1);
  const gate = job.steps.find(step => step.run?.includes('node scripts/check-lane-artifacts.mjs'));
  assert.ok(gate);
  assert.equal(gate.if, undefined);
  assert.equal(gate['continue-on-error'], undefined);
  assert.deepEqual(gate.env, {
    ARTIFACT_BASE: '${{ github.event.pull_request.base.sha || github.event.before }}',
    ARTIFACT_HEAD: '${{ github.sha }}',
  });
  assert.equal(gate.run, [
    'if [[ -n "$ARTIFACT_BASE" && ! "$ARTIFACT_BASE" =~ ^0+$ ]]; then',
    '  git fetch --no-tags --depth=1 origin "$ARTIFACT_BASE"',
    'fi',
    'node scripts/check-lane-artifacts.mjs "$ARTIFACT_BASE" "$ARTIFACT_HEAD"',
    '',
  ].join('\n'));
});

test('real Git changed-path gate rejects committed artifacts and permits their removal', () => {
  const root = mkdtempSync(join(tmpdir(), 'sdk-artifact-policy-'));
  const git = (...args) => {
    const result = spawnSync('git', args, {cwd: root, encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  };
  const write = (path, text = 'fixture\n') => {
    mkdirSync(dirname(join(root, path)), {recursive: true});
    writeFileSync(join(root, path), text);
  };
  const commit = () => {
    git('add', '-A'); git('-c', 'commit.gpgSign=false', 'commit', '-qm', 'fixture');
    return git('rev-parse', 'HEAD');
  };
  const check = base => spawnSync(process.execPath, [checker, base, 'HEAD'], {cwd: root, encoding: 'utf8'});
  try {
    git('init', '-q'); git('config', 'user.name', 'Fixture'); git('config', 'user.email', 'fixture@example.invalid');
    write('README.md'); write('inherited.md'); const baseline = commit();
    write('docs/ci-releases.md'); const healthy = commit();
    assert.equal(check(baseline).status, 0);
    for (const path of ['docs/approvals-map.md', 'docs/scratch-plan.md', 'docs/new-product-guide.md']) {
      write(path); commit();
      const result = check(healthy);
      assert.equal(result.status, 1, path + ': ' + result.stderr);
      assert.ok(result.stderr.includes(path), result.stderr);
      git('rm', path); commit();
    }
    // Negative control: introduce the real forbidden path through Git, then
    // run exactly the CLI used by CI. A detached policy-only oracle cannot pass.
    write('docs/rev-839-verdict2.md'); commit();
    const rejected = check(baseline);
    assert.equal(rejected.status, 1, rejected.stderr);
    assert.match(rejected.stderr, /docs\/rev-839-verdict2\.md/);
    git('rm', 'docs/rev-839-verdict2.md'); commit();
    assert.equal(check(healthy).status, 0);
    git('mv', 'docs/ci-releases.md', 'lane-log.txt'); commit();
    assert.equal(check(healthy).status, 1);
    git('rm', 'lane-log.txt'); write('inherited.md', 'edited\n'); commit();
    assert.equal(check(healthy).status, 1, 'modifying inherited disallowed docs is gated');
    assert.equal(check('').status, 1, 'dispatch and first push audit the tree');
    assert.equal(check('0'.repeat(40)).status, 1);
    assert.notEqual(check('missing-commit').status, 0, 'missing history fails closed');
  } finally {
    rmSync(root, {recursive: true, force: true});
  }
});
