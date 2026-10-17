// Cargo owns Rust name resolution, cfg evaluation and dependency visibility.
// This module only groups declared target requirements into compiler commands.
const KINDS = new Map([['test', '--test'], ['example', '--example'], ['bench', '--bench'], ['bin', '--bin']]);

export function checks(metadata, selected = metadata.workspace_members) {
  const members = new Set(selected);
  const result = [];
  for (const pkg of metadata.packages) {
    if (!members.has(pkg.id)) continue;
    const groups = new Map([['', {features: [], targets: []}]]);
    const library = pkg.targets.some(target => target.kind.some(kind => ['lib', 'rlib', 'proc-macro'].includes(kind)));
    for (const target of pkg.targets) {
      const kind = target.kind.find(kind => KINDS.has(kind));
      if (!kind) continue;
      const features = [...new Set(target['required-features'] ?? [])].sort();
      const key = features.join(',');
      if (!groups.has(key)) groups.set(key, {features, targets: []});
      groups.get(key).targets.push({kind, name: target.name});
    }
    for (const {features, targets} of groups.values()) {
      if (!library && !targets.length) continue;
      const args = ['check', '--locked', '-p', pkg.name];
      if (library) args.push('--lib');
      // --tests also compiles the library's real cfg(test) closure. Cargo skips
      // integration targets whose declarations are not enabled in this group.
      if (library || targets.some(target => target.kind === 'test')) args.push('--tests');
      for (const target of targets) {
        if (target.kind !== 'test') args.push(KINDS.get(target.kind), target.name);
      }
      if (features.length) args.push('--features', features.join(','));
      result.push({package: pkg.name, features, targets, args});
    }
  }
  return result;
}
