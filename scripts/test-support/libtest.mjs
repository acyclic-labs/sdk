/** @returns {never} */
function refuse(detail) {
  throw new Error(`step=verify exact libtest evidence: ${detail}; recovery command: node --test scripts/test-support/libtest.test.mjs`);
}

function expectedCases(names) {
  if (!Array.isArray(names) || names.length === 0
    || names.some(name => typeof name !== 'string' || !/^[a-zA-Z0-9_:]+$/.test(name))) {
    refuse('distinct literal exact test names required');
  }
  const expected = new Set(names);
  if (expected.size !== names.length) refuse('distinct literal exact test names required');
  return expected;
}

function sameCases(expected, actual) {
  if (actual.length !== expected.size || new Set(actual).size !== actual.length
    || actual.some(name => !expected.has(name))) {
    refuse(`expected every exact test once: ${JSON.stringify({ expected: [...expected], actual })}`);
  }
}

/**
 * Verify libtest discovery before invoking an exact selection, including ignored cases.
 * @param {string[]} names
 * @param {string} output
 */
export function verifyExactTestListing(names, output) {
  const expected = expectedCases(names);
  if (typeof output !== 'string') refuse('libtest listing stdout required');
  const cases = [...output.matchAll(/^(\S+): (test|benchmark)\r?$/gm)];
  if (cases.some(match => match[2] !== 'test')) refuse('a benchmark is not an exact test');
  sameCases(expected, cases.map(match => match[1]));
}

/**
 * Verify the original process terminal and stdout, not its exit code alone.
 * Package selection, deadlines, cancellation and process cleanup belong to the caller.
 * @param {string[]} names Distinct exact case names selected by the caller.
 * @param {{status: number|null, stdout: string, signal?: string|null, error?: unknown}} result
 */
export function verifyExactTests(names, { status, stdout, signal, error }) {
  const expected = expectedCases(names);
  if (status !== 0 || signal != null || error != null) refuse(`test execution failed: ${error ?? signal ?? status}`);
  if (typeof stdout !== 'string') refuse('libtest execution stdout required');
  const cases = [...stdout.matchAll(/^test (\S+)(?: - should panic)? \.\.\. ([^\r\n]+)\r?$/gm)];
  if (cases.some(match => match[2] !== 'ok')) refuse('every reported exact test must pass');
  const actual = cases.map(match => match[1]);
  sameCases(expected, actual);
  const summaries = stdout.split(/\r?\n/).filter(line => line.startsWith('test result:'));
  if (summaries.length !== 1) refuse('exactly one libtest summary required');
  const summary = /^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; \d+ filtered out; finished in ([\d.]+)s$/.exec(summaries[0]);
  if (!summary) refuse('one complete successful libtest summary required');
  const [, passed, failed, ignored, measured, seconds] = summary;
  if (Number(passed) !== expected.size || [failed, ignored, measured].some(value => Number(value) !== 0)) {
    refuse('missing, ignored, failed or measured tests cannot qualify');
  }
  if (!Number.isFinite(Number(seconds))) refuse('finite libtest elapsed time required');
  return { tests: actual, seconds: Number(seconds) };
}
