// Cargo owns Rust name resolution, cfg evaluation and dependency visibility.
// Production targets must be checked before any selector activates dev dependencies.
import assert from 'node:assert/strict';
const LIBRARIES = new Set(['lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro']);
const KINDS = new Map([['test', '--test'], ['example', '--example'], ['bench', '--bench'], ['bin', '--bin']]);

export function checks(metadata, selected = metadata.workspace_members) {
  const remaining = new Set(selected);
  const result = [];
  for (const pkg of metadata.packages) {
    if (!remaining.delete(pkg.id)) continue;
    const start = result.length;
    const covered = new Set();
    const libraries = [];
    const build = [];
    const groups = new Map([['', {features: [], targets: []}]]);
    for (const target of pkg.targets) {
      if (target.kind.length && target.kind.every(kind => LIBRARIES.has(kind))) {
        libraries.push(target);
        continue;
      }
      if (target.kind.length === 1 && target.kind[0] === 'custom-build') {
        build.push(target);
        continue;
      }
      assert.ok(target.kind.length === 1 && KINDS.has(target.kind[0]),
        `${pkg.name}/${target.name}: unrecognized Cargo target kinds ${target.kind}`);
      const features = [...new Set(target['required-features'] ?? [])].sort();
      const key = features.join(',');
      if (!groups.has(key)) groups.set(key, {features, targets: []});
      groups.get(key).targets.push(target);
    }
    assert.ok(libraries.length <= 1, `${pkg.name}: multiple Cargo library targets`);
    function emit(stage, features, flags, targets) {
      if (!flags.length) return;
      for (const target of targets) covered.add(target);
      result.push({package: pkg.name, stage, features,
        targets: targets.map(target => ({kind: target.kind[0], name: target.name})),
        args: ['check', '--locked', '-p', pkg.name, ...flags,
          ...(features.length ? ['--features', features.join(',')] : [])]});
    }
    for (const {features, targets} of groups.values()) {
      const bins = targets.filter(target => target.kind[0] === 'bin');
      // Neither examples/benches nor --tests may contaminate this production closure.
      emit('production', features,
        [...(libraries.length ? ['--lib'] : []), ...bins.flatMap(target => ['--bin', target.name])],
        [...libraries, ...bins]);
      const dev = targets.filter(target => target.kind[0] !== 'bin');
      const tests = libraries.some(target => target.test !== false)
        || targets.some(target => target.kind[0] === 'test' || (target.kind[0] === 'bin' && target.test !== false));
      emit('tests', features,
        [...(libraries.length && tests ? ['--lib'] : []), ...(tests ? ['--tests'] : []),
          ...dev.flatMap(target => [KINDS.get(target.kind[0]), target.name])],
        dev);
    }
    // Cargo runs a package's build script as part of each selected compiler command.
    if (result.length > start) for (const target of build) covered.add(target);
    for (const target of pkg.targets) {
      assert.ok(covered.has(target), `${pkg.name}/${target.name}: selected target has no compiler coverage`);
    }
    assert.ok(result.length > start, `${pkg.name}: selected package has no compiler coverage`);
  }
  assert.equal(remaining.size, 0, `selected packages missing from metadata: ${[...remaining]}`);
  return result;
}
