import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';

const metadata = JSON.parse(execFileSync('cargo', ['metadata', '--locked', '--no-deps', '--format-version=1'], {encoding: 'utf8'}));
const members = new Set(metadata.workspace_members);
const packages = new Map(metadata.packages.filter(pkg => members.has(pkg.id)).map(pkg => [pkg.name, pkg]));
const boundaries = new Map([
  ['acyclic-fs-wire', new Set(['acyclic-fs', 'acyclic-harness', 'acyclic-harness-core', 'acyclic-plugin'])],
  ['acyclic-plugin-identity', new Set(['acyclic-fs', 'acyclic-harness', 'acyclic-harness-core', 'acyclic-plugin'])],
]);
for (const [name, forbidden] of boundaries) {
  assert.ok(packages.has(name), `${name} must own its independent build boundary`);
  const pending = [[name]];
  const seen = new Set();
  while (pending.length) {
    const chain = pending.pop();
    assert.ok(chain);
    const current = chain.at(-1);
    assert.ok(current);
    assert.ok(!forbidden.has(current), `${name} must not compile its consumers: ${chain.join(' -> ')}`);
    if (seen.has(current)) continue;
    seen.add(current);
    for (const dependency of packages.get(current)?.dependencies ?? []) {
      // Root dev dependencies compile for its tests; dependencies' own tests do not.
      if (dependency.kind !== 'dev' || chain.length === 1) pending.push([...chain, dependency.name]);
    }
  }
  console.log(`${name}: independent production and test dependency closure`);
}
