import {copyFileSync, existsSync, globSync, mkdirSync, readFileSync, rmSync} from 'node:fs';
import {dirname, join, relative, resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import ts from 'typescript-parser';
import {SaxesParser} from 'saxes';
import {Processes} from './graphcoder-e2e/process.mjs';
import {environment, scratchDirectory} from './graphcoder-e2e/scratch.mjs';

const ROOT = fileURLToPath(new URL('../', import.meta.url));
const ordinaryTest = file => /(?:^|\/)(?:test-[^/]+|[^/]+[._](?:test|spec))\.[cm]?[jt]sx?$/.test(file);
const suffixedTest = file => /[._](?:test|spec)\.[cm]?[jt]sx?$/.test(file);
const sourceFile = file => /\.[cm]?[jt]sx?$/.test(file);
const excluded = file => file.replaceAll('\\', '/').split('/').some(part =>
 part.startsWith('.') || ['node_modules', 'target', 'dist', 'coverage', 'output'].includes(part));
const stable = value => String(value).replaceAll('\r', '\\r').replaceAll('\n', '\\n');
const fail = (file, name) => console.log(`NODE_TEST_FAIL ${stable(file)}::${stable(name)}`);

// Use the repository's TypeScript parser, including for JavaScript. Strings and
// comments containing program fixtures cannot register a test or an import.
export function discoverTests(root = ROOT) {
 const files = globSync('**/*.{js,mjs,cjs,ts,mts,cts,tsx,jsx}', {cwd: root, exclude: excluded})
  .map(file => file.replaceAll('\\', '/')).sort();
 return files.filter(file => ordinaryTest(file)).map(file => {
  const imports = ts.preProcessFile(readFileSync(join(root, file), 'utf8'), true, true).importedFiles.map(input => input.fileName);
  return {file, runtime: imports.includes('bun:test') || /\.(?:[cm]?tsx?|jsx)$/.test(file) ? 'bun' : 'node', imports};
 // Prefix-named CLI scenarios have their own executable arguments and owners.
 // Suffix-named test modules remain selected even if import failure prevents
 // registration; native engine failure must never disappear from inventory.
 }).filter(test => suffixedTest(test.file) || test.imports.some(input => input === 'node:test' || input === 'bun:test'));
}

// Like the Cloud entry point, only test-only changes can narrow qualification.
// Source changes may affect spawned programs, generated inputs and dynamic reads.
export function affectedTests(tests, changed, root = ROOT) {
 if (!changed.length) return [];
 const paths = changed.map(file => relative(root, resolve(root, file)).replaceAll('\\', '/'));
 if (paths.some(file => !ordinaryTest(file) || !existsSync(join(root, file)))) return tests;
 const programs = new Map(tests.map(test => [test.file, test.imports]));
 const options = {moduleResolution: ts.ModuleResolutionKind.Bundler, allowJs: true};
 const cache = ts.createModuleResolutionCache(root, file => ts.sys.useCaseSensitiveFileNames ? file : file.toLowerCase(), options);
 const consumers = new Map(), pending = tests.map(test => test.file), seen = new Set();
  while (pending.length) {
   const file = pending.pop();
   if (seen.has(file)) continue;
   seen.add(file);
   if (!existsSync(join(root, file)) || !sourceFile(file)) continue;
   if (!programs.get(file)) programs.set(file, ts.preProcessFile(readFileSync(join(root, file), 'utf8'), true, true).importedFiles.map(input => input.fileName));
   for (const input of programs.get(file)) if (input.startsWith('.')) {
    // Runtime paths and TypeScript's .js-to-.ts aliases both contribute.
    const dependencies = [relative(root, resolve(root, dirname(file), input)).replaceAll('\\', '/')];
    const resolved = ts.resolveModuleName(input, join(root, file), options, ts.sys, cache).resolvedModule;
    if (resolved) dependencies.push(relative(root, resolved.resolvedFileName).replaceAll('\\', '/'));
    for (const dependency of dependencies) {
     if (!consumers.has(dependency)) consumers.set(dependency, new Set());
     consumers.get(dependency).add(file);
     pending.push(dependency);
    }
   }
  }
 const affected = new Set(paths);
 pending.push(...paths);
 while (pending.length) for (const file of consumers.get(pending.pop()) ?? []) if (!affected.has(file)) {
  affected.add(file); pending.push(file);
 }
 return tests.filter(test => affected.has(test.file));
}

/** Decode native Bun JUnit and reconcile files omitted after import failures. */
export function bunResults(xml, reportFailure = fail, expectedFiles = []) {
 const parser = new SaxesParser(), result = {tests: 0, passed: 0, failed: 0, skipped: 0};
 const suites = [], observed = new Set();
 let current;
 parser.on('doctype', () => { throw new Error('JUnit must not declare external entities'); });
 parser.on('opentag', tag => {
  if (tag.name === 'testsuite') suites.push({file: tag.attributes.file, name: tag.attributes.name});
  if (tag.name === 'testcase') {
   const file = tag.attributes.file ?? suites[0]?.file ?? suites[0]?.name;
   if (typeof file !== 'string' || typeof tag.attributes.name !== 'string') throw new Error('JUnit test identity missing');
   const parents = suites.slice(1).map(suite => suite.name);
   if (!parents.length && tag.attributes.classname && tag.attributes.classname !== file) parents.push(tag.attributes.classname);
   current = {file, name: [...parents, tag.attributes.name].join(' > '), failed: false, skipped: false};
  }
  if (current && ['failure', 'error', 'flakyFailure', 'rerunFailure'].includes(tag.name)) current.failed = true;
  if (current && tag.name === 'skipped') current.skipped = true;
 });
 parser.on('closetag', tag => {
  if (tag.name === 'testsuite') suites.pop();
  if (tag.name !== 'testcase') return;
  if (!current || typeof current.file !== 'string' || typeof current.name !== 'string') throw new Error('JUnit test identity missing');
  observed.add(current.file.replaceAll('\\', '/'));
  result.tests++;
  if (current.failed) { result.failed++; reportFailure(current.file, current.name); }
  else if (current.skipped) result.skipped++;
  else result.passed++;
  current = undefined;
 });
 parser.write(xml).close();
 // Bun 1.4's JUnit omits import failures altogether, even when other cases
 // failed. Reconcile the selected inventory against original case identities;
 // a file that registered nothing can never silently qualify.
 for (const file of expectedFiles) if (!observed.has(file.replaceAll('\\', '/'))) {
  result.tests++; result.failed++; reportFailure(file, '(no tests registered)');
 }
 return result;
}

// Cargo's original artifact messages own executable locations, including a
// relocated target directory. Never derive a binary path from this checkout.
async function nativeInputs(tests, processes, env, publish) {
 if (!tests.length) return;
 env.E2E_PROCESS_HOST ??= env.ACYCLIC_PROCESS_HOST;
 env.ACYCLIC_PROCESS_HOST ??= env.E2E_PROCESS_HOST;
 // A custodian cannot compile itself under its own custody. Windows callers
 // must admit the canonical host produced by their native build before any
 // compiler or engine starts; an unsafe parent-only bootstrap is forbidden.
 if (process.platform === 'win32' && !env.E2E_PROCESS_HOST) throw new Error(
  "step=admit native process owner; recovery command: $env:E2E_PROCESS_HOST='ABSOLUTE_INSTALLED_ACYCLIC_PROCESS_HOST'; node scripts/node-tests.mjs --all");
 const report = tests.some(test => test.file.endsWith('/termination.test.mjs')) && !env.E2E_REPORT_TEST_BINARY;
 const host = tests.some(test => test.file.endsWith('/sdk/test/process.test.mjs')) && !env.E2E_PROCESS_HOST;
 for (const kind of [...(host ? ['host'] : []), ...(report ? ['report'] : [])]) {
  const args = kind === 'host'
   ? ['build', '--bin', 'acyclic-process-host'] : ['test', '--no-run', '--lib'];
  args.push('--locked', '-p', 'acyclic-native-runtime', '--message-format=json');
  const outcome = await processes.runOwned('cargo', args, {cwd: ROOT, env});
  publish(outcome);
  if (outcome.code !== 0) throw new Error(`step=prepare ${kind}; recovery command: cargo ${args.join(' ')}`);
  const artifacts = outcome.stdout.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line))
   .filter(row => row.reason === 'compiler-artifact' && row.executable
    && (kind === 'host' ? row.target.name === 'acyclic-process-host' : row.target.name === 'acyclic_native_runtime' && row.profile.test));
  if (artifacts.length !== 1) throw new Error(`step=locate ${kind} artifact; recovery command: cargo ${args.join(' ')}`);
  if (kind === 'host') env.E2E_PROCESS_HOST = env.ACYCLIC_PROCESS_HOST = artifacts[0].executable;
  else env.E2E_REPORT_TEST_BINARY = artifacts[0].executable;
 }
}

/** @param {Array<{file: string, runtime: string}>} tests
 * @param {{root?: string, selected?: string, signal?: AbortSignal, processes?: Processes, publish?: (result: any) => void}} [options] */
export async function runTests(tests, {root = ROOT, selected = 'all', signal,
 processes = new Processes(), publish = result => {
  process.stdout.write(result.stdout); process.stderr.write(result.stderr);
 }} = {}) {
 const started = performance.now();
 const totals = {files: tests.length, tests: 0, passed: 0, failed: 0, skipped: 0, selected, seconds: 0};
 const temporary = scratchDirectory('node-tests-', {...process.env, E2E_SCRATCH_ROOT: process.env.E2E_SCRATCH_ROOT ?? tmpdir()});
 const env = /** @type {NodeJS.ProcessEnv} */ ({...environment(join(temporary, 'profile')), E2E_SCRATCH_ROOT: temporary, FORCE_COLOR: '0', NO_COLOR: '1'});
 delete env.NODE_TEST_CONTEXT;
 let draining;
 const cancel = () => { draining ??= processes.close(); draining.catch(() => {}); };
 signal?.addEventListener('abort', cancel, {once: true});
 try {
  signal?.throwIfAborted();
  await nativeInputs(tests, processes, env, publish);
  signal?.throwIfAborted();
  for (const runtime of ['node', 'bun']) {
   const files = tests.filter(test => test.runtime === runtime).map(test => test.file);
   if (!files.length) continue;
   const report = join(temporary, runtime === 'bun' ? 'bun.xml' : 'node.json');
   const args = runtime === 'bun'
    ? ['test', '--isolate', '--reporter=junit', '--reporter-outfile=' + report, ...files.map(file => resolve(root, file))]
    : ['--test', '--experimental-vm-modules', '--test-concurrency=4',
     '--test-reporter=' + fileURLToPath(new URL('./node-test-reporter.mjs', import.meta.url)), ...files];
   const outcome = await processes.runOwned(runtime, args, {cwd: root, env: {...env, SDK_NODE_TEST_REPORT: report}});
   publish(outcome);
   signal?.throwIfAborted();
   try {
    if (runtime === 'bun' && existsSync(report)) {
     const retained = process.env.SDK_TEMP_DIR ? join(process.env.SDK_TEMP_DIR, 'observability') : join(root, 'target');
     mkdirSync(retained, {recursive: true});
     copyFileSync(report, join(retained, 'bun-test.xml'));
    }
    const counts = runtime === 'bun' ? bunResults(readFileSync(report, 'utf8'), fail, files) : JSON.parse(readFileSync(report, 'utf8'));
    for (const key of ['tests', 'passed', 'failed', 'skipped']) totals[key] += counts[key];
    if ((outcome.code !== 0 && counts.failed === 0) || counts.tests === 0)
     throw new Error(`${runtime} exited ${outcome.code}, signal=${outcome.signal}, tests=${counts.tests}`);
   } catch (cause) {
    fail(runtime, '(runner)'); totals.failed++;
    console.error(`step=collect ${runtime} test results: ${cause.message}; recovery command: node scripts/node-tests.mjs --all`);
   }
  }
 } finally {
  signal?.removeEventListener('abort', cancel);
  await (draining ?? processes.close());
  rmSync(temporary, {recursive: true, force: true});
 }
 totals.seconds = (performance.now() - started) / 1000;
 console.log('NODE_TESTS_RESULT ' + JSON.stringify(totals));
 return totals.failed ? 1 : 0;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
 try {
  const args = process.argv.slice(2);
  const rootAt = args.indexOf('--root');
  if (rootAt >= 0 && (!args[rootAt + 1] || args[rootAt + 1].startsWith('-'))) throw new Error('--root requires a directory');
  const root = rootAt < 0 ? ROOT : resolve(args.splice(rootAt, 2)[1]);
  const onlyAt = args.indexOf('--only');
  if (onlyAt >= 0 && (!args[onlyAt + 1] || args[onlyAt + 1].startsWith('-'))) throw new Error('--only requires a test file or directory');
  const only = onlyAt < 0 ? undefined : relative(root, resolve(args.splice(onlyAt, 2)[1])).replaceAll('\\', '/');
  const all = args.includes('--all');
  if ((all && (args.length !== 1 || only !== undefined)) || (only !== undefined && args.length) || (!all && args.some(arg => arg.startsWith('-'))))
   throw new Error('use --all, --only FILE_OR_DIRECTORY, or changed repository paths');
  const tests = discoverTests(root);
  const selected = all ? tests : only === undefined ? affectedTests(tests, args, root)
   : tests.filter(test => test.file === only || test.file.startsWith(only + '/'));
  if ((only !== undefined || all) && !selected.length) throw new Error('selection contains no tests');
  process.exitCode = await runTests(selected, {root, selected: all ? 'all' : only === undefined ? 'affected' : 'explicit'});
 } catch (cause) {
  fail('discovery', '(runner)');
  console.error(`step=discover Node/Bun tests: ${cause.message}; recovery command: bun install --frozen-lockfile && node scripts/node-tests.mjs --all`);
  console.log('NODE_TESTS_RESULT ' + JSON.stringify({files: 0, tests: 0, passed: 0, failed: 1, skipped: 0, selected: 'error'}));
  process.exitCode = 2;
 }
}
