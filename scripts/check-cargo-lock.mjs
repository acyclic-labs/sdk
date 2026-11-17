import {toml} from './policy-core.mjs';
import {workspacePackages, compareVersions} from './workspace-model.mjs';

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
  for (const pkg of workspacePackages(root))
    if (!lock.package.some(p => !p.source && p.name === pkg.name && p.version === pkg.version))
      throw Error(`missing local package ${pkg.name}@${pkg.version}`);
  // Parsing and checking never rewrites the lockfile or acquires a package-cache lock.
  return lock.package.length;
}

if (import.meta.main) {
  try { console.log(`lockfile: ${checkLock(process.cwd())} packages checked`); }
  catch (error) {console.error(`step=cargo-lock: ${error.message}`); process.exitCode = 1;}
}
