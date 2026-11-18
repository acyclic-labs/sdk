#!/usr/bin/env node
// A successful test exit with no matching tests cannot discharge a main debt.
import { execFileSync, spawn } from 'node:child_process';
import { isAbsolute, resolve } from 'node:path';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { verifyExactTests } from './test-support/libtest.mjs';

export function testArguments(entry) {
  if (typeof entry.target !== 'string' || !/^\/\/rust\/crates\/[a-z0-9-]+:[a-zA-Z0-9_-]+_test_cases$/.test(entry.target)) {
    throw new Error('invalid test target');
  }
  if (!entry.regression || !Array.isArray(entry.controls) || !entry.controls.length) throw new Error('regression and negative control required');
  const names = [...new Set([entry.regression, ...entry.controls])];
  if (names.some(name => typeof name !== 'string' || !/^[a-zA-Z0-9_:]+$/.test(name))) throw new Error('invalid exact test name');
  return [...names, '--exact', '--test-threads=4', '--color=never'];
}

export function verify(entry, status, output) {
  testArguments(entry);
  const expected = [...new Set([entry.regression, ...entry.controls])];
  const result = verifyExactTests(expected, { status, stdout: output });
  return { regression: entry.regression, ...result };
}

export function testExecutable(root, output) {
  const files = output.trim().split(/\r?\n/u);
  if (!isAbsolute(root) || files.length !== 1 || !files[0].startsWith('bazel-out/') || files[0].split('/').includes('..')) {
    throw new Error('exactly one selected test executable required');
  }
  return resolve(root, files[0]);
}

// The original leader exit retires its process group before inherited output
// can keep pipe closure pending. Qualification still joins the actual output.
/**
 * @typedef {{pid?: number, stdout: import('node:stream').Readable,
 * stderr: import('node:stream').Readable,
 * on(event: string, listener: (...args: any[]) => void): unknown}} QualificationChild
 * @param {string} executable
 * @param {string[]} args
 * @param {{launch?: (command: string, args: string[], options:
 * import('node:child_process').SpawnOptionsWithStdioTuple<import('node:child_process').StdioNull,
 * import('node:child_process').StdioPipe, import('node:child_process').StdioPipe>) => QualificationChild,
 * terminate?: (child: QualificationChild) => void}} options
 */
export function executeTest(executable, args, {
  launch = spawn,
  terminate = child => { process.kill(-/** @type {number} */ (child.pid), 'SIGKILL'); },
} = {}) {
  return new Promise((accept, reject) => {
    const child = launch(executable, args, {
      detached: true, stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, RUST_TEST_THREADS: '4' },
    });
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', chunk => { output += chunk; process.stdout.write(chunk); });
    child.stderr.pipe(process.stderr);
    let settled = false;
    let retired = false;
    const retire = () => {
      if (retired) return;
      retired = true;
      if (child.pid !== undefined) terminate(child);
    };
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      try {
        // The group belongs to this execution even after its leader exits
        // and descendants close their output pipes. A failed spawn has no PID.
        retire();
      } catch (cleanupError) {
        if (cleanupError.code !== 'ESRCH') error = cleanupError;
      } finally {
        child.stdout.destroy();
        child.stderr.destroy();
      }
      if (error) reject(error);
      else accept(result);
    };
    child.on('error', error => finish(error));
    child.on('exit', () => {
      try { retire(); }
      catch (error) {
        if (error.code !== 'ESRCH') finish(new Error(
          `step=retire original test group ${child.pid}: ${error.message}; recovery command: node scripts/qualify-main-green-debts.mjs`,
          { cause: error }));
      }
    });
    child.on('close', status => finish(null, { status, output }));
  });
}

async function run(entry) {
  const testArgs = testArguments(entry);
  const download = '--remote_download_toplevel';
  console.log(`MAIN_GREEN_BUILD bazel build ${download} ${entry.target}`);
  execFileSync('bazel', ['build', download, entry.target], { stdio: 'inherit' });
  const query = (...args) => execFileSync('bazel', args, { encoding: 'utf8' }).trim();
  const executable = testExecutable(query('info', 'execution_root'),
    query('cquery', download, '--output=files', entry.target));
  console.log(`MAIN_GREEN_EXECUTE ${executable} ${testArgs.join(' ')}`);
  const execution = await executeTest(executable, testArgs);
  const result = verify(entry, execution.status, execution.output);
  console.log(`MAIN_GREEN_QUALIFIED ${JSON.stringify(result)}`);
  return result;
}

export async function main(selected = process.argv.slice(2)) {
  if (process.platform !== 'linux') throw new Error('step=admit process-group qualification: Linux is required; recovery command: node scripts/qualify-main-green-debts.mjs');
  process.chdir(fileURLToPath(new URL('../', import.meta.url)));
  const entries = JSON.parse(await readFile(new URL('./main-green-debts.json', import.meta.url), 'utf8'));
  const names = new Set(entries.map(entry => entry.regression));
  if (names.size !== entries.length) throw new Error('duplicate debt');
  for (const name of selected) if (!names.has(name)) throw new Error(`unknown debt: ${name}`);
  // Validate the complete plan before starting any build.
  for (const entry of entries) testArguments(entry);
  const results = [];
  const failures = [];
  for (const entry of entries) {
    if (!selected.length || selected.includes(entry.regression)) {
      try {
        results.push(await run(entry));
      } catch (error) {
        const failure = { regression: entry.regression, error: String(error) };
        failures.push(failure);
        console.error(`MAIN_GREEN_FAILED ${JSON.stringify(failure)}`);
      }
    }
  }
  console.log(`MAIN_GREEN_DONE ${JSON.stringify({ qualified: results.length, total: entries.length, results, failures })}`);
  if (failures.length) throw new Error(`${failures.length} debt qualifications failed`);
}

if (import.meta.main) {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}
