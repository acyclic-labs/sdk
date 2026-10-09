#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, posix, relative, resolve } from 'node:path';
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
if (receipt.scope !== 'typescript-compiler' || receipt.rust_producers_qualified !== false || !/^sha256:[0-9a-f]{64}$/.test(receipt.source_sha256) || !/^sha256:[0-9a-f]{64}$/.test(receipt.compiler_build_receipt_sha256)) throw new Error('installed packages require canonical compiler provenance');
const buildBytes = readFileSync(join(packages, 'BUILD.json'));
if (`sha256:${hash(buildBytes)}` !== receipt.compiler_build_receipt_sha256) throw new Error('package compiler receipt digest differs');
const build = JSON.parse(buildBytes);
if (build.schema !== 'acyclic.typescript-build-receipt.v1' || build.scope !== receipt.scope || build.source_commit !== receipt.source_commit || build.source_sha256 !== receipt.source_sha256 || build.rust_producers_qualified !== false) throw new Error('package compiler source identity differs');
const boundJson = name => {
  const bytes = readFileSync(contained(bundle, name));
  if (manifest.artifacts[name] !== `sha256:${hash(bytes)}`) throw new Error(`scenario sidecar digest differs: ${name}`);
  return JSON.parse(bytes);
};
const projections = boundJson('sdk-docs-scenario-projections.v1.json').projections;
const scenarios = boundJson('sdk-docs-scenarios.v1.json').scenarios;
const rustExecutions = boundJson('sdk-docs-scenario-executions.v1.json');
if (!projections.length || new Set(projections.map(row => row.scenarioId)).size !== projections.length || new Set(projections.map(row => row.package)).size !== projections.length) throw new Error('duplicate or absent registered TypeScript projections');
for (const row of projections) {
  const scenario = scenarios.find(source => source.id === row.scenarioId);
  const execution = rustExecutions.find(result => result.id === row.scenarioId);
  if (!scenario || !execution || row.language !== 'typescript' || row.package !== `@acyclic-labs/${scenario.family}` || scenario.sourceSha256 !== row.sourceSha256 || execution.sourceSha256 !== row.sourceSha256 || execution.stdoutSha256 !== row.rustOutputSha256 || execution.status !== 'passed') throw new Error(`projection differs from registered Rust scenario execution: ${row.scenarioId}`);
  const bytes = readFileSync(contained(bundle, row.path));
  if (`sha256:${hash(bytes)}` !== row.snippetSha256 || manifest.artifacts[row.path] !== row.snippetSha256) throw new Error(`generated snippet digest differs: ${row.path}`);
}
if (scenarios.some(row => typeof row.typescriptProjection !== 'boolean') || new Set(scenarios.map(row => row.id)).size !== scenarios.length) throw new Error('invalid Rust-owned projection eligibility');
const eligible = new Set(scenarios.filter(row => row.typescriptProjection).map(row => row.id));
if (eligible.size !== projections.length || projections.some(row => !eligible.has(row.scenarioId))) throw new Error('projections do not exactly cover Rust-owned TypeScript eligibility');
const expected = projections.map(row => row.package);
const docs = boundJson(posix.join(posix.dirname(manifest.releaseManifest), 'sdk-docs-data.v1.json'));
const release = boundJson(manifest.releaseManifest);
if (docs.version !== manifest.version || release.version !== manifest.version || docs.source.revision !== manifest.revision || release.revision !== manifest.revision || release.sourceSha256 !== manifest.sourceSha256) throw new Error('Rust package catalog and release identity differ');
if (new Set(receipt.packages.map(row => row.name)).size !== receipt.packages.length) throw new Error('duplicate package receipt identity');
for (const projection of projections) {
  const scenario = scenarios.find(row => row.id === projection.scenarioId);
  const catalog = docs.packages.entries.filter(row => row.packageName === scenario.package);
  const released = release.scenarios.filter(row => row.id === scenario.id && row.package === scenario.package);
  const installed = receipt.packages.filter(row => row.name === projection.package);
  if (catalog.length !== 1 || released.length !== 1 || installed.length !== 1 || catalog[0].version !== released[0].cargoVersion || installed[0].version !== catalog[0].version) throw new Error(`package version differs from Rust catalog: ${projection.package}`);
}
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
  schema: 'acyclic.generated-snippet-qualification.v1', revision: manifest.revision,
  sourceSha256: manifest.sourceSha256, manifestSha256: `sha256:${hash(manifestBytes)}`,
  packageReceiptSha256: `sha256:${hash(readFileSync(join(packages, 'QUALIFICATION.json')))}`,
  compilerBuildReceiptSha256: receipt.compiler_build_receipt_sha256,
  compilerSourceSha256: receipt.source_sha256, rustProducersQualified: false,
  runnerSha256: `sha256:${hash(readFileSync(fileURLToPath(import.meta.url)))}`,
  compiler: compilerVersion, compilerFlags: flags, runtime: process.version,
  packages: archives.map(({ path, ...row }) => row), executions,
};
writeFileSync(join(work, 'receipt.json'), `${JSON.stringify(qualification, null, 2)}\n`);
console.log(JSON.stringify({ revision: manifest.revision, compiled: executions.length, executed: executions.length, receipt: join(work, 'receipt.json') }));
