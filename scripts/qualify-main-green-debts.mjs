#!/usr/bin/env node
// A successful Cargo exit with no matching tests cannot discharge a main debt.
import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

export function cargoArguments(entry) {
  if (!/^acyclic-[a-z0-9-]+$/.test(entry.package)) throw new Error('invalid package');
  if (typeof entry.target !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]*$/.test(entry.target)) throw new Error('invalid test target');
  if (!entry.regression || !Array.isArray(entry.controls) || !entry.controls.length) throw new Error('regression and negative control required');
  const names = [...new Set([entry.regression, ...entry.controls])];
  if (names.some(name => typeof name !== 'string' || !/^[a-zA-Z0-9_:]+$/.test(name))) throw new Error('invalid exact test name');
  const target = entry.target === 'lib' ? ['--lib'] : ['--test', entry.target];
  return ['test', '--locked', '-p', entry.package, '--all-features', ...target,
    '--', ...names, '--exact', '--test-threads=4', '--color=never'];
}

export function verify(entry, status, output) {
  cargoArguments(entry);
  if (status !== 0) throw new Error(`Cargo failed: ${status}`);
  const expected = [...new Set([entry.regression, ...entry.controls])];
  const actual = [...output.matchAll(/^test (\S+)(?: - should panic)? \.\.\. ok\r?$/gm)].map(match => match[1]);
  if (actual.length !== expected.length || new Set(actual).size !== actual.length
    || expected.some(name => !actual.includes(name))) {
    throw new Error(`expected every regression and negative control: ${JSON.stringify({ expected, actual })}`);
  }
  const summaries = [...output.matchAll(/^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; \d+ filtered out; finished in ([\d.]+)s\r?$/gm)];
  if (summaries.length !== 1) throw new Error('exactly one libtest summary required');
  const [, passed, failed, ignored, measured, seconds] = summaries[0];
  if (Number(passed) !== expected.length || [failed, ignored, measured].some(value => Number(value) !== 0)) {
    throw new Error('missing, ignored, failed or measured tests cannot discharge a debt');
  }
  if (!Number.isFinite(Number(seconds)) || Number(seconds) >= 10) throw new Error('suite must finish in under 10 seconds');
  return { regression: entry.regression, tests: actual, seconds: Number(seconds) };
}

export function testExecutable(entry, output) {
  cargoArguments(entry);
  const libraryKinds = new Set(['lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro']);
  const selectedKind = kinds => Array.isArray(kinds) && kinds.length > 0
    && (entry.target === 'lib' ? kinds.every(kind => libraryKinds.has(kind))
      : kinds.length === 1 && kinds.includes('test'));
  const name = entry.target === 'lib' ? entry.package.replaceAll('-', '_') : entry.target;
  const artifacts = output.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
    .filter(message => message.reason === 'compiler-artifact' && message.profile?.test
      && message.target?.name === name && selectedKind(message.target.kind) && message.executable);
  if (artifacts.length !== 1 || typeof artifacts[0].executable !== 'string') {
    throw new Error('exactly one selected test executable required');
  }
  return artifacts[0].executable;
}

// Compilation owns no test budget. Once a binary starts, its process group and
// output pipes belong to this invocation, including when the test never exits.
export function executeTest(executable, args, {
  launch = spawn, arm = setTimeout, disarm = clearTimeout,
  terminate = child => process.kill(-child.pid, 'SIGKILL'),
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
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      disarm(timer);
      try {
        // The group belongs to this execution even after its leader exits
        // and descendants close their output pipes. A failed spawn has no PID.
        if (child.pid !== undefined) terminate(child);
      } catch (cleanupError) {
        if (cleanupError.code !== 'ESRCH') error = cleanupError;
      } finally {
        child.stdout.destroy();
        child.stderr.destroy();
      }
      if (error) reject(error);
      else accept(result);
    };
    const timer = arm(() => {
      // Finish independently of close: an inherited pipe cannot stall the
      // debt queue after its owning test has exceeded the execution budget.
      finish(new Error('test execution exceeded the fixed 10-second budget; debt remains unqualified'));
    }, 10_000);
    child.on('error', error => finish(error));
    child.on('close', status => finish(null, { status, output }));
  });
}

async function run(entry) {
  const args = cargoArguments(entry);
  const separator = args.indexOf('--');
  const build = [...args.slice(0, separator), '--no-run', '--message-format=json-render-diagnostics'];
  console.log(`MAIN_GREEN_BUILD cargo ${build.join(' ')}`);
  const child = spawn('cargo', build, { stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => { output += chunk; });
  child.stderr.pipe(process.stderr);
  const status = await new Promise((accept, reject) => {
    child.on('error', reject);
    child.on('close', accept);
  });
  if (status !== 0) throw new Error(`Cargo build failed: ${status}`);
  const executable = testExecutable(entry, output);
  const testArgs = args.slice(separator + 1);
  console.log(`MAIN_GREEN_EXECUTE ${executable} ${testArgs.join(' ')}`);
  const execution = await executeTest(executable, testArgs);
  const result = verify(entry, execution.status, execution.output);
  console.log(`MAIN_GREEN_QUALIFIED ${JSON.stringify(result)}`);
  return result;
}

export async function main(selected = process.argv.slice(2)) {
  if (process.platform !== 'linux') throw new Error('run main debt qualification on bb2 Linux');
  process.chdir(fileURLToPath(new URL('../', import.meta.url)));
  const entries = JSON.parse(await readFile(new URL('./main-green-debts.json', import.meta.url), 'utf8'));
  const names = new Set(entries.map(entry => entry.regression));
  if (names.size !== entries.length) throw new Error('duplicate debt');
  for (const name of selected) if (!names.has(name)) throw new Error(`unknown debt: ${name}`);
  // Validate the complete plan before starting any build.
  for (const entry of entries) cargoArguments(entry);
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

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}
