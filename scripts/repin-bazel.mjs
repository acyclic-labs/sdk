// Feed current Cargo declarations to crate_universe before asking it to repin.
import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {bazelCommand} from './bazel-executables.mjs';
import {repositoryRoot, toml} from './policy-core.mjs';
import {registryInputs} from './workspace-model.mjs';

const root = repositoryRoot;
const hub = toml(root, 'Cargo.toml').workspace?.metadata?.bazel?.['crate-hub'] ?? 'sdk_crates';
const inputs = registryInputs(root).inputs.sort((a, b) => a.name.localeCompare(b.name, 'en') || a.version.localeCompare(b.version, 'en'));
const groups = Object.groupBy(inputs, p => p.name);
const quote = JSON.stringify;
const specs = inputs.map(p => {
  const alias = groups[p.name].length > 1 ? `    package_alias = ${quote(`${p.name}-${p.version.replace(/[^A-Za-z0-9_-]/g, '_')}`)},\n` : '';
  const features = [...p.features].sort().map(f => `        ${quote(f)},\n`).join('');
  return `crate.spec(\n    package = ${quote(p.name)},\n    repositories = [\n        ${quote(hub)},\n    ],\n    version = ${quote(`=${p.version}`)},\n${alias}    default_features = ${p.defaultFeatures ? 'True' : 'False'},\n${features ? `    features = [\n${features}    ],\n` : ''})\n`;
}).join('\n');
const moduleFile = path.join(root, 'cargo.MODULE.bazel');
const original = fs.readFileSync(moduleFile, 'utf8');
// Replace only the native spec run: the browser specs after the native crate.from_specs
// are the generator's, and the generator write below refreshes them from the Cargo lock.
const pattern = /^(?:crate\.spec\(\n[\s\S]*?^\)\n\n?)+/m;
let matched = false;
const updated = original.replace(pattern, () => { matched = true; return `${specs}\n`; });
if (!matched) throw Error('cargo.MODULE.bazel has no crate.spec entries');
fs.writeFileSync(moduleFile, updated);
const output = path.join(os.tmpdir(), `repin-bazel-${process.pid}.json`);
try {
  // The generator owns the browser specs. Write them without a repin env: crate_universe
  // digest-checks every hub, so a native-only repin cannot pass while the browser hub is
  // stale. The output mode is read-only, so it writes the specs without evaluating them.
  execFileSync(process.execPath, [path.join(root, 'scripts/bazel-build-files.mjs'), 'write', '--output', output], {
    cwd: root, stdio: 'inherit',
  });
  fs.writeFileSync(moduleFile, JSON.parse(fs.readFileSync(output, 'utf8'))['cargo.MODULE.bazel']);
  // One repin for both hubs: crate_universe digest-checks every hub, so they must repin
  // together.
  execFileSync(bazelCommand(), ['mod', 'deps', '--lockfile_mode=update'], {
    cwd: root, stdio: 'inherit', env: {...process.env, CARGO_BAZEL_REPIN: '1'},
  });
  // The BUILD files need the pinned hubs, so write them after the repin.
  execFileSync(process.execPath, [path.join(root, 'scripts/bazel-build-files.mjs'), 'write'], {
    cwd: root, stdio: 'inherit',
  });
} catch (error) {
  fs.writeFileSync(moduleFile, original);
  throw error;
} finally {
  fs.rmSync(output, {force: true});
}
