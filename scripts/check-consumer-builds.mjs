#!/usr/bin/env node
// Code generation is a separate boundary from `cargo check`: async layout
// queries can overflow only when a downstream executable is monomorphized.
// One package per invocation keeps unrelated workspace features out of it.
import { spawn, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { selectionWorker, workerEnv } from './check-feature-matrix.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const libraryKinds = new Set(['lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro']);

export function consumers(metadata) {
  const members = new Set(metadata.workspace_default_members);
  return metadata.packages.filter(item => members.has(item.id)
    && !item.metadata?.['cargo-fuzz']
    && (item.publish === null || item.publish?.length > 0
      || item.targets.some(target => target.kind.some(kind => libraryKinds.has(kind)))))
    .sort((a, b) => a.name.localeCompare(b.name));
}

export function commands(item) {
  if (item.metadata?.acyclic?.['consumer-examples']
    && !item.targets.some(target => target.kind.includes('example') && !target['required-features']?.length)) {
    throw new Error(`consumer examples require an unconditional executable: ${item.name}`);
  }
  // One selection per crate, --all-features (2026-10-26 bb2 compute policy).
  return [['--all-features']].flatMap(features => {
    const base = ['build', '--locked', '--package', item.name, ...features];
    // Examples are downstream crates. Opted-in provider probes must also be
    // monomorphized, after the library build without dev-dependency unification.
    return [base, ...(item.metadata?.acyclic?.['consumer-examples'] ? [[...base, '--examples']] : [])];
  });
}

export async function main(args = process.argv.slice(2)) {
  const scope = args.filter(arg => arg.startsWith('--scope-file='));
  const names = args.filter(arg => arg.startsWith('--package=')).map(arg => arg.slice(10));
  const workerArgs = args.filter(arg => arg.startsWith('--workers='));
  const workers = workerArgs.length ? Number(workerArgs[0].slice(10)) : 1;
  if (scope.length > 1 || workerArgs.length > 1 || !Number.isSafeInteger(workers) || workers < 1
    || args.some(arg => !arg.startsWith('--scope-file=') && !arg.startsWith('--workers=')
      && !arg.startsWith('--package=') && arg !== '--list')) throw new Error('invalid consumer build arguments');
  const result = spawnSync('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1'],
    { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(result.stderr || 'consumer metadata failed');
  const all = consumers(JSON.parse(result.stdout));
  for (const name of names) if (!all.some(item => item.name === name)) throw new Error(`unknown consumer: ${name}`);
  const allowed = scope.length ? new Set(readFileSync(resolve(root, scope[0].slice(13)), 'utf8')
    .split(/\r?\n/).filter(Boolean)) : null;
  const selected = all.filter(item => (!names.length || names.includes(item.name))
    && (!allowed || allowed.has(item.name)));
  const jobs = selected.flatMap(item => commands(item));
  if (args.includes('--list')) {
    for (const command of jobs) console.log(`cargo ${command.join(' ')}`);
    return;
  }
  // Each crate's unit runs on the matrix worker that checks the same
  // selection, in that worker's own target: one shared target let Cargo's
  // build lock serialize every build (1,225 s for 82 sets, 2026-10-26).
  let passed = 0;
  const failures = [];
  const queues = Array.from({ length: workers }, () => []);
  for (const item of selected) queues[selectionWorker(item.name, ['--all-features'], workers)].push(commands(item));
  async function worker(index) {
    const env = workerEnv(index);
    for (const unit of queues[index]) for (const command of unit) {
      const started = performance.now();
      // Separate Cargo invocations prevent feature unification while
      // retaining compiled dependencies; the library precedes its examples.
      const child = spawn('cargo', command, { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] });
      let output = '';
      child.stdout.on('data', chunk => { output += chunk; });
      child.stderr.on('data', chunk => { output += chunk; });
      const status = await new Promise((accept, reject) => child.on('error', reject).on('close', accept));
      if (status === 0) passed++;
      else {
        failures.push(command);
        process.stderr.write(`FAILED cargo ${command.join(' ')}\n${output}\n`);
      }
      console.log(`consumer build: worker=${index} seconds=${((performance.now() - started) / 1000).toFixed(3)} rc=${status} cargo ${command.join(' ')}`);
    }
  }
  const started = performance.now();
  await Promise.all(Array.from({ length: workers }, (_, index) => worker(index)));
  console.log(`consumer builds wall: seconds=${((performance.now() - started) / 1000).toFixed(3)} workers=${workers}`);
  console.log(`consumer builds: ${passed}/${jobs.length} sets ok`);
  if (failures.length) process.exitCode = 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
