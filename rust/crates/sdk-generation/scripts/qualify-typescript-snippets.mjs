#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const [bundleArg, packagesArg, workArg, npmCliArg] = process.argv.slice(2);
if (!npmCliArg || process.argv.length !== 6) throw new Error('usage: qualify-typescript-snippets.mjs BUNDLE PACKAGE_DIRECTORY FRESH_WORK_DIRECTORY NPM_CLI_JS');
const bundle = resolve(bundleArg), packages = resolve(packagesArg), work = resolve(workArg);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const json = path => JSON.parse(readFileSync(path, 'utf8'));
const contained = (root, name) => {
  const path = resolve(root, name), rel = relative(root, path);
  if (isAbsolute(name) || rel.startsWith('..') || isAbsolute(rel)) throw new Error(`escaping artifact path: ${name}`);
  return path;
};
const run = (args, label) => {
  const result = spawnSync(process.execPath, args, { cwd: work, encoding: 'utf8', timeout: 120_000 });
  writeFileSync(join(work, `${label}.stdout.log`), result.stdout ?? '');
  writeFileSync(join(work, `${label}.stderr.log`), result.stderr ?? '');
  if (result.error || result.status !== 0) throw new Error(`${label} failed: ${result.error ?? result.stderr}`);
  return result.stdout.trim();
};
const manifestBytes = readFileSync(join(bundle, 'generation-manifest.v1.json'));
if (`sha256:${hash(manifestBytes)}` !== readFileSync(join(bundle, 'generation-manifest.v1.sha256'), 'utf8').trim()) throw new Error('generation manifest digest differs');
const manifest = JSON.parse(manifestBytes);
const receipt = json(join(packages, 'QUALIFICATION.json'));
if (manifest.sourceState !== 'captured-snapshot' || !/^[0-9a-f]{40}$/.test(manifest.revision) || receipt.revision !== 1 || receipt.source_commit !== manifest.revision) throw new Error('installed packages must match the captured documentation source commit');
const projections = json(join(bundle, 'sdk-docs-scenario-projections.v1.json')).projections;
const expected = ['actors', 'stream', 'machines', 'workers', 'objects', 'inference'].map(name => `@acyclic-labs/${name}`);
if (projections.length !== expected.length || new Set(projections.map(row => row.package)).size !== expected.length || expected.some(name => !projections.some(row => row.package === name && row.language === 'typescript'))) throw new Error('expected exactly the six registered TypeScript projections');
const archives = receipt.packages.map(row => {
  if (!/^@acyclic-labs\/[a-z][a-z0-9-]*$/.test(row.name) || !/^acyclic-labs-[a-z0-9.-]+\.tgz$/.test(row.asset)) throw new Error('invalid package receipt identity');
  const path = contained(packages, row.asset), bytes = readFileSync(path);
  if (hash(bytes) !== row.sha256 || bytes.length !== row.size) throw new Error(`package archive differs: ${row.asset}`);
  return { ...row, path };
});
if (expected.some(name => !archives.some(row => row.name === name))) throw new Error('package receipt omits a snippet package');
if (existsSync(work)) throw new Error('qualification work directory must be fresh');
mkdirSync(work, { recursive: true });
writeFileSync(join(work, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
run([resolve(npmCliArg), 'install', '--no-audit', '--no-fund', '--ignore-scripts', 'typescript@7.0.2', '@types/node@26.4.1', ...archives.map(row => row.path)], 'install');
for (const row of archives) {
  const installed = json(join(work, 'node_modules', row.name, 'package.json'));
  if (installed.name !== row.name || installed.version !== row.version) throw new Error(`installed identity differs: ${row.name}`);
}
const files = projections.map(row => {
  const source = contained(bundle, row.path), bytes = readFileSync(source);
  if (`sha256:${hash(bytes)}` !== row.snippetSha256 || manifest.artifacts[row.path] !== row.snippetSha256) throw new Error(`generated snippet digest differs: ${row.path}`);
  const target = contained(work, row.path);
  mkdirSync(dirname(target), { recursive: true });
  copyFileSync(source, target);
  return target;
});
const compiler = join(work, 'node_modules/typescript/bin/tsc');
const compilerVersion = run([compiler, '--version'], 'compiler-version');
const flags = ['--ignoreConfig', '--strict', '--skipLibCheck', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--target', 'ES2022', '--rootDir', work, '--outDir', join(work, 'compiled')];
run([compiler, ...flags, ...files], 'compile');
const executions = projections.map((row, index) => ({ ...row, stdout: run([join(work, 'compiled', row.path.replace(/\.ts$/, '.js'))], `execute-${index + 1}`) }));
const qualification = {
  schema: 'acyclic.generated-snippet-qualification.v2', revision: manifest.revision,
  sourceSha256: manifest.sourceSha256, manifestSha256: `sha256:${hash(manifestBytes)}`,
  packageReceiptSha256: `sha256:${hash(readFileSync(join(packages, 'QUALIFICATION.json')))}`,
  runnerSha256: `sha256:${hash(readFileSync(fileURLToPath(import.meta.url)))}`,
  compiler: compilerVersion, compilerFlags: flags, runtime: process.version,
  packages: archives.map(({ path, ...row }) => row), executions,
};
writeFileSync(join(work, 'receipt.json'), `${JSON.stringify(qualification, null, 2)}\n`);
console.log(JSON.stringify({ revision: manifest.revision, compiled: executions.length, executed: executions.length, receipt: join(work, 'receipt.json') }));
