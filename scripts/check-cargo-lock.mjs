import {lockedReference, toml} from './policy-core.mjs';
import {workspacePackages, compareVersions, versionMatches} from './workspace-model.mjs';

export function checkLock(root) {
  const lock = toml(root, 'Cargo.lock');
  if (lock.version !== 4 || !Array.isArray(lock.package)) throw Error('expected a version 4 package lock');
  const identities = new Set();
  const resolved = new Map();
  let previous;
  for (const pkg of lock.package) {
    const identity = `${pkg.name} ${pkg.version} ${pkg.source ?? ''}`;
    if (identities.has(identity)) throw Error(`duplicate locked package ${identity}`);
    identities.add(identity);
    if (previous && (pkg.name < previous.name || pkg.name === previous.name && compareVersions(pkg.version, previous.version) < 0)) throw Error(`package ordering at ${pkg.name}`);
    previous = pkg;
    if (pkg.source?.startsWith('registry+') && !/^[0-9a-f]{64}$/.test(pkg.checksum ?? '')) throw Error(`invalid checksum for ${identity}`);
    const dependencies = pkg.dependencies ?? [];
    if (new Set(dependencies).size !== dependencies.length || dependencies.some((d, n) => n && d < dependencies[n - 1]))
      throw Error(`dependency ordering or duplicate in ${identity}`);
    resolved.set(pkg, dependencies.map(dep => {
      const candidates = lock.package.filter(lockedReference(dep));
      if (candidates.length !== 1) throw Error(`ambiguous or missing dependency ${dep} in ${identity}`);
      return {ref: dep, package: candidates[0]};
    }));
  }
  for (const pkg of workspacePackages(root)) {
    const locked = lock.package.find(p => !p.source && p.name === pkg.name && p.version === pkg.version);
    if (!locked) throw Error(`missing local package ${pkg.name}@${pkg.version}`);
    const declared = pkg.dependencies.filter(dep => pkg.workspace_member || dep.kind !== 'dev');
    // A patched dependency outside the workspace may have optional features
    // no workspace member selected, so its lock need not contain those edges.
    for (const dep of declared.filter(dep => pkg.workspace_member || !dep.optional)) {
      const present = resolved.get(locked).some(({package: candidate}) => candidate.name === dep.name && versionMatches(candidate.version, dep.req));
      if (!present) throw Error(`missing locked dependency ${pkg.name} -> ${dep.name} ${dep.req}`);
    }
    for (const {ref, package: candidate} of resolved.get(locked))
      if (!declared.some(dep => dep.name === candidate.name)) throw Error(`undeclared locked dependency ${pkg.name} -> ${ref}`);
  }
  // Parsing and checking never rewrites the lockfile or acquires a package-cache lock.
  return lock.package.length;
}

if (import.meta.main) {
  try { console.log(`lockfile: ${checkLock(process.cwd())} packages checked`); }
  catch (error) {console.error(`step=cargo-lock: ${error.message}`); process.exitCode = 1;}
}
