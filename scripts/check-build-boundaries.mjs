import assert from 'node:assert/strict';
import {reachable} from './policy-core.mjs';
import {workspacePackages} from './workspace-model.mjs';

const packages = new Map(workspacePackages(process.cwd()).filter(pkg => pkg.workspace_member).map(pkg => [pkg.name, pkg]));
const consumers = new Set(['acyclic-fs', 'acyclic-harness', 'acyclic-harness-core', 'acyclic-plugin']);
for (const name of ['acyclic-fs-wire', 'acyclic-plugin-identity']) {
  assert.ok(packages.has(name), `${name} must own its independent build boundary`);
  const chains = new Map([[name, name]]);
  reachable([name], function* (current) {
    for (const dependency of packages.get(current)?.dependencies ?? []) {
      // Root dev dependencies compile for its tests; dependencies' own tests do not.
      if (dependency.kind === 'dev' && current !== name) continue;
      const chain = `${chains.get(current)} -> ${dependency.name}`;
      assert.ok(!consumers.has(dependency.name), `${name} must not compile its consumers: ${chain}`);
      if (!chains.has(dependency.name)) chains.set(dependency.name, chain);
      yield dependency.name;
    }
  });
  console.log(`${name}: independent production and test dependency closure`);
}
