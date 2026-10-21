import {spawn} from 'node:child_process';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

// Inspect declarations, not unified enabled features: a dev dependency or
// another workspace member must not accidentally supply the missing edge.
export function checkTestSupport(metadata) {
  if (!metadata.resolve) throw new Error('test-support check requires resolved Cargo metadata');
  const packages = new Map();
  const nodes = new Map();
  for (const pkg of metadata.packages) packages.set(pkg.id, pkg);
  for (const node of metadata.resolve.nodes) nodes.set(node.id, node);
  const failures = new Set();
  let checked = 0;
  for (const id of metadata.workspace_members) {
    const pkg = packages.get(id);
    if (!pkg) throw new Error(`missing workspace package ${id}`);
    if (!Object.hasOwn(pkg.features, 'test-support')) continue;
    const node = nodes.get(id);
    if (!node) throw new Error(`missing resolved workspace package ${id}`);
    const reachable = new Set();
    const visit = feature => {
      if (reachable.has(feature)) return;
      reachable.add(feature);
      for (const next of pkg.features[feature] ?? []) visit(next);
    };
    visit('test-support');
    for (const edge of node.deps) {
      const dependency = packages.get(edge.pkg);
      if (!dependency) throw new Error(`missing resolved dependency ${edge.pkg}`);
      if (!Object.hasOwn(dependency.features, 'test-support')) continue;
      for (const declaration of pkg.dependencies) {
        const alias = declaration.rename ?? declaration.name;
        if (declaration.name !== dependency.name || alias.replaceAll('-', '_') !== edge.name) continue;
        // Dev features are not selected by a consumer's test-support feature.
        if (declaration.kind === 'dev' || !edge.dep_kinds.some(kind =>
          kind.kind === declaration.kind && kind.target === declaration.target)) continue;
        checked++;
        if (reachable.has(`${alias}/test-support`) ||
            (declaration.optional && reachable.has(`${alias}?/test-support`))) continue;
        const forwarding = `${alias}${declaration.optional ? '?' : ''}/test-support`;
        failures.add(`${pkg.name}: test-support must forward ${forwarding}`);
      }
    }
  }
  if (failures.size) throw new Error([...failures].sort().join('\n'));
  return checked;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  // Resolve every optional and target-specific edge, including registry crates.
  const child = spawn('cargo', ['metadata', '--locked', '--all-features', '--format-version', '1'],
    {stdio: ['ignore', 'pipe', 'inherit'], windowsHide: true});
  let output = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => { output += chunk; });
  const status = await new Promise((accept, reject) => child.on('error', reject).on('close', accept));
  if (status !== 0) throw new Error(`cargo metadata failed (${status})`);
  console.log(`test-support forwarding: ${checkTestSupport(JSON.parse(output))} dependency declarations checked`);
}
