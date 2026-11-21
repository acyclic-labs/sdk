import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync, rmdirSync, unlinkSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test, {after} from 'node:test';

const reporter = fileURLToPath(new URL('./test-selection-reporter.mjs', import.meta.url));
const directory = mkdtempSync(join(tmpdir(), 'acyclic-node-selection-'));
const fixture = join(directory, 'fixture.mjs');
after(() => {
    unlinkSync(fixture);
    rmdirSync(directory);
});
writeFileSync(fixture, `import assert from 'node:assert/strict';
import test from 'node:test';
test('existing case', () => {});
test('named failure', () => assert.fail('fixture failure'));
`);
const run = pattern => spawnSync(process.execPath, [
    '--test',
    '--test-reporter=tap',
    '--test-reporter-destination=stdout',
    '--test-reporter=' + reporter,
    '--test-reporter-destination=stderr',
    '--test-name-pattern=' + pattern,
    fixture,
], {encoding: 'utf8', env: {...process.env, NODE_TEST_CONTEXT: undefined}});

test('the shared Node runner refuses a pattern with no cases', () => {
    const result = run('missing case');
    assert.ifError(result.error);
    assert.equal(result.status, 1, result.stdout);
    assert.match(result.stderr, /test name pattern selected no cases/);
});

test('the shared Node runner reports selected cases by name', () => {
    const success = run('existing case');
    assert.ifError(success.error);
    assert.equal(success.status, 0, success.stderr);
    assert.match(success.stdout, /ok 1 - existing case/);
    const failure = run('named failure');
    assert.ifError(failure.error);
    assert.equal(failure.status, 1, failure.stdout);
    assert.match(failure.stdout, /not ok 1 - named failure/);
});
