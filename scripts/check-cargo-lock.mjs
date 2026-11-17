import {toml} from './policy-core.mjs';
import {workspacePackages, compareVersions, versionMatches} from './workspace-model.mjs';

export function checkLock(root) {
  const lock = toml(root, 'Cargo.lock');
  if (lock.version !== 4 || !Array.isArray(lock.package)) throw Error('expected a version 4 package lock');
  const identities = new Set();
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
    for (const dep of dependencies) {
      const [name, version, source] = dep.split(' ');
      const candidates = lock.package.filter(p => p.name === name && (!version || p.version === version) && (!source || `(${p.source})` === source));
      if (candidates.length !== 1) throw Error(`ambiguous or missing dependency ${dep} in ${identity}`);
    }
  }
  for (const pkg of workspacePackages(root)) {
    const locked = lock.package.find(p => !p.source && p.name === pkg.name && p.version === pkg.version);
    if (!locked) throw Error(`missing local package ${pkg.name}@${pkg.version}`);
    const declared = pkg.dependencies.filter(dep => pkg.workspace_member || dep.kind !== 'dev');
    // A patched dependency outside the workspace may have optional features
    // no workspace member selected, so its lock need not contain those edges.
    for (const dep of declared.filter(dep => pkg.workspace_member || !dep.optional)) {
      const present = (locked.dependencies ?? []).some(ref => {
        const [name, version, source] = ref.split(' ');
        if (name !== dep.name) return false;
        return lock.package.some(candidate => candidate.name === name && (!version || candidate.version === version)
          && (!source || `(${candidate.source})` === source) && versionMatches(candidate.version, dep.req));
      });
      if (!present) throw Error(`missing locked dependency ${pkg.name} -> ${dep.name} ${dep.req}`);
    }
    for (const ref of locked.dependencies ?? [])
      if (!declared.some(dep => dep.name === ref.split(' ')[0])) throw Error(`undeclared locked dependency ${pkg.name} -> ${ref}`);
  }
  // Parsing and checking never rewrites the lockfile or acquires a package-cache lock.
  return lock.package.length;
}

if (import.meta.main) {
  try { console.log(`lockfile: ${checkLock(process.cwd())} packages checked`); }
  catch (error) {console.error(`step=cargo-lock: ${error.message}`); process.exitCode = 1;}
}
