import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { verifyExactTestListing, verifyExactTests } from '@acyclic-labs/test-support/libtest';
import { verify as verifySdk } from '../qualify-main-green-debts.mjs';

const original = await readFile(new URL('./fixtures/libtest.txt', import.meta.url), 'utf8');
const originalName = /^test (\S+) \.\.\. ok$/m.exec(original)[1];

// All adversarial operands derive from the retained actual libtest transcript.
function check({ verifyExactTests: verify, verifyExactTestListing: listing }) {
  for (const count of [1, 2, 4]) {
    const names = Array.from({ length: count }, (_, i) => `vmm_product::qualification::case_${i}`);
    const cases = names.map(name => `test ${name} ... ok`).join('\n');
    const body = original.replace(/^test .* \.\.\. ok$/m, cases)
      .replace('running 1 test', `running ${count} tests`).replace('1 passed', `${count} passed`);
    const discovered = names.map(name => `${name}: test`).join('\n');
    for (const newline of ['\n', '\r\n']) {
      const output = body.replaceAll('\n', newline);
      assert.deepEqual(verify(names, { status: 0, stdout: output }).tests, names);
      listing(names, discovered.replaceAll('\n', newline));
    }
    // Duration policy belongs to the caller, including native tests over ten seconds.
    assert.equal(verify(names, { status: 0, stdout: body.replace('0.00s', '12.50s') }).seconds, 12.5);
    for (const changed of [
      body.replace(names[0], 'foreign::case'),
      body.replace(`test ${names[0]} ... ok`, `test ${names[0]} ... ignored`),
      body.replace(`${count} passed`, '0 passed'),
      body.replace('0 ignored', '1 ignored'),
      body.replace('0 measured', '1 measured'),
      body.replace('test result:', 'incomplete result:'),
      body + `\ntest ${names[0]} ... FAILED\n`,
      body + body,
      body + '\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n',
    ]) assert.throws(() => verify(names, { status: 0, stdout: changed }));
    for (const terminal of [{ status: null }, { status: 101 }, { status: 0, signal: 'SIGTERM' },
      { status: 0, error: new Error('original process failed') }]) {
      assert.throws(() => verify(names, { ...terminal, stdout: body }));
    }
    for (const changed of ['', discovered + '\n' + discovered,
      discovered.replace(names[0], 'foreign::case'), discovered.replace(': test', ': benchmark')]) {
      assert.throws(() => listing(names, changed));
    }
    for (const invalid of [[], [...names, names[0]], ['--skip'], [null]]) {
      assert.throws(() => verify(invalid, { status: 0, stdout: body }));
      assert.throws(() => listing(invalid, discovered));
    }
  }
  assert.equal(verify([originalName], { status: 0, stdout: original }).tests[0], originalName);
}

test('one shared exact-case property detects actual verifier guard omissions', async () => {
  check({ verifyExactTests, verifyExactTestListing });
  const entry = { package: 'acyclic-harness-core', target: 'lib', regression: originalName, controls: [originalName] };
  assert.deepEqual(verifySdk(entry, 0, original).tests, [originalName]);
  for (const seconds of [10, 30, 1e6]) {
    assert.equal(verifySdk(entry, 0, original.replace('0.00s', `${seconds.toFixed(2)}s`)).seconds, seconds);
  }
  assert.throws(() => verifySdk(entry, 0, original + '\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n'));
  const source = await readFile(new URL('./libtest.mjs', import.meta.url), 'utf8');
  for (const [before, after] of [
    ['summaries.length !== 1', 'summaries.length < 1'],
    ['sameCases(expected, actual);', ''],
    ['status !== 0 || signal != null || error != null', 'false'],
    ['sameCases(expected, cases.map(match => match[1]));', ''],
  ]) {
    assert.equal(source.split(before).length, 2, 'actual mutation source must be unique');
    const mutant = await import('data:text/javascript;base64,' + Buffer.from(source.replace(before, after)).toString('base64'));
    assert.throws(() => check(mutant), assert.AssertionError);
  }
});
