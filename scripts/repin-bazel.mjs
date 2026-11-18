// Feed current Cargo declarations to crate_universe before asking it to repin.
import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {bazelCommand} from './bazel-executables.mjs';
import {repositoryRoot} from './policy-core.mjs';
import {registryInputs} from './workspace-model.mjs';

const root = repositoryRoot;
const inputs = registryInputs(root).inputs.sort((a, b) => a.name.localeCompare(b.name, 'en') || a.version.localeCompare(b.version, 'en'));
const groups = Object.groupBy(inputs, p => p.name);
const quote = JSON.stringify;
const specs = inputs.map(p => {
  const alias = groups[p.name].length > 1 ? `    package_alias = ${quote(`${p.name}-${p.version.replace(/[^A-Za-z0-9_-]/g, '_')}`)},\n` : '';
  const features = [...p.features].sort().map(f => `        ${quote(f)},\n`).join('');
  return `crate.spec(\n    package = ${quote(p.name)},\n    version = ${quote(`=${p.version}`)},\n${alias}    default_features = ${p.defaultFeatures ? 'True' : 'False'},\n${features ? `    features = [\n${features}    ],\n` : ''})\n`;
}).join('\n');
const moduleFile = path.join(root, 'cargo.MODULE.bazel');
const original = fs.readFileSync(moduleFile, 'utf8');
const pattern = /^crate\.spec\(\n[\s\S]*?^\)\n\n?/gm;
let first = true;
const updated = original.replace(pattern, () => {if (!first) return ''; first = false; return `${specs}\n`;});
if (first) throw Error('cargo.MODULE.bazel has no crate.spec entries');
fs.writeFileSync(moduleFile, updated);
try {
  execFileSync(bazelCommand(), ['mod', 'deps', '--lockfile_mode=update'], {
    cwd: root, stdio: 'inherit', env: {...process.env, CARGO_BAZEL_REPIN: '1'},
  });
} catch (error) {
  fs.writeFileSync(moduleFile, original);
  throw error;
}
