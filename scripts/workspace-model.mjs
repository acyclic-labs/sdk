// Repository declarations and Bazel's committed crate resolution are the metadata
// authority. Reading them never resolves, downloads or builds a Cargo workspace.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {dependencyEnabled, evaluateCfg, forwardedFeatures, growSets, lockDependencyIndex, manifestDependencies, reachable, toml, tomlFiles} from './policy-core.mjs';

const slash = value => value.replaceAll('\\', '/');
const exists = file => fs.existsSync(file);
function bazelResolution(file, optional = false) {
  try {return JSON.parse(fs.readFileSync(file, 'utf8'));}
  catch (error) {if (!optional || error.code !== 'ENOENT') throw error; return null;}
}
const requirement = value => value === undefined ? '*' : /^\d/.test(value) ? `^${value}` : value;
const selectedEntries = (value, universe, triple) => Array.isArray(value) ? value :
  [...(value?.common ?? []), ...Object.entries(value?.selects ?? {}).flatMap(([cfg, entries]) => triple === null || (universe.conditions[cfg] ?? [cfg]).includes(triple) ? entries : [])];
export function compareVersions(a, b) {
  const parts = value => {
    const [core, ...suffix] = value.split('+')[0].split('-');
    return {core: core.split('.').map(Number), pre: suffix.length ? suffix.join('-').split('.') : null};
  };
  const aa = parts(a), bb = parts(b);
  for (let n = 0; n < 3; n++) {
    const delta = (aa.core[n] ?? 0) - (bb.core[n] ?? 0);
    if (delta) return delta;
  }
  if (!aa.pre || !bb.pre) return aa.pre ? -1 : bb.pre ? 1 : 0;
  for (let n = 0; n < Math.max(aa.pre.length, bb.pre.length); n++) {
    const x = aa.pre[n], y = bb.pre[n];
    if (x === y) continue;
    if (x === undefined || y === undefined) return x === undefined ? -1 : 1;
    const numericX = /^\d+$/.test(x), numericY = /^\d+$/.test(y);
    if (numericX !== numericY) return numericX ? -1 : 1;
    return numericX ? Number(x) - Number(y) : x < y ? -1 : 1;
  }
  return 0;
}
export function versionMatches(version, requirement) {
  const comparisons = requirement.split(',').map(part => {
    const match = /^\s*(\^|~|=|>=|<=|>|<)?\s*(\d+|\*)(?:\.(\d+|\*))?(?:\.(\d+|\*))?(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?\s*$/.exec(part);
    if (!match) throw Error(`unsupported locked version requirement ${requirement}`);
    return match;
  });
  if (version.split('+')[0].includes('-') && !comparisons.some(match => match[5] && match.slice(2, 5).join('.') === version.split('-')[0])) return false;
  return comparisons.every(match => {
    const [, operator = '^', major, minor, patch, prerelease] = match;
    const fields = [major, minor, patch];
    const count = fields.findIndex(x => x === undefined || x === '*');
    const length = count < 0 ? 3 : count;
    if (!length) return true;
    const lower = fields.map(x => x === undefined || x === '*' ? 0 : Number(x));
    const comparison = compareVersions(version, lower.join('.') + (prerelease ? `-${prerelease}` : ''));
    if (operator === '>=') return comparison >= 0;
    if (operator === '<=') return comparison <= 0;
    if (operator === '>') return comparison > 0;
    if (operator === '<') return comparison < 0;
    if (operator === '=' && length === 3) return comparison === 0;
    const upper = [...lower];
    const bump = operator === '~' ? Math.min(length - 1, 1) : fields.includes('*') || operator === '=' ? length - 1 : Math.min(lower.findIndex(x => x !== 0) < 0 ? length - 1 : lower.findIndex(x => x !== 0), length - 1);
    upper[bump]++;
    for (let n = bump + 1; n < 3; n++) upper[n] = 0;
    return comparison >= 0 && compareVersions(version, upper.join('.')) < 0;
  });
}

function lockedEdges(resolve, item) {
  return (item.dependencies ?? []).map(reference => {
    const candidates = resolve(reference);
    if (candidates.length !== 1) throw Error(`ambiguous or missing Cargo.lock dependency ${reference}`);
    return candidates[0];
  });
}

export function workspacePackages(root, {sourceRoot = root, patchConfig, lockFile} = {}) {
  root = path.resolve(root);
  sourceRoot = path.resolve(sourceRoot);
  const withinSource = directory => {
    const relative = path.relative(sourceRoot, directory);
    return relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
  };
  if (!withinSource(root)) throw Error('workspace escapes source root');
  const rootManifest = toml(root, 'Cargo.toml');
  const patches = [];
  const addPatches = (table, base) => {
    for (const [alias, entry] of Object.entries(table ?? {})) {
      if (!entry.path) continue;
      const directory = path.resolve(base, entry.path);
      if (!withinSource(directory)) throw Error(`workspace patch escapes source root: ${directory}`);
      patches.push({name: entry.package ?? alias, directory});
    }
  };
  addPatches(rootManifest.patch?.['crates-io'], root);
  if (patchConfig) {
    const config = path.resolve(sourceRoot, patchConfig);
    if (!withinSource(config)) throw Error('workspace patch config escapes source root');
    addPatches(toml(sourceRoot, patchConfig).patch?.['crates-io'], path.dirname(path.dirname(config)));
  }
  if (lockFile && !withinSource(path.resolve(sourceRoot, lockFile))) throw Error('workspace lock escapes source root');
  const lock = lockFile ? toml(sourceRoot, lockFile) : null;
  const resolveLocked = lockDependencyIndex(lock?.package ?? []);
  const manifests = new Set();
  const expand = pattern => fs.globSync(pattern.replace(/\/$/, '') + '/Cargo.toml', {cwd: root});
  const excluded = file => {
    let directory = slash(path.dirname(file));
    while (directory !== '.' && directory !== '..' && !directory.startsWith('../')) {
      if ((rootManifest.workspace?.exclude ?? []).some(pattern => path.matchesGlob(directory, pattern.replace(/\/$/, '')))) return true;
      directory = slash(path.dirname(directory));
    }
    return false;
  };
  for (const member of rootManifest.workspace?.members ?? []) for (const file of expand(member)) if (!excluded(slash(file))) manifests.add(slash(file));
  if (rootManifest.package) manifests.add('Cargo.toml');
  const members = new Set(manifests);
  // The root Bazel graph also renders patch crates that are not currently
  // reached. Retained producer graphs instead use their selected lockfile.
  if (!patchConfig && !lockFile) for (const patch of patches)
    manifests.add(slash(path.relative(root, path.join(patch.directory, 'Cargo.toml'))));
  const packages = new Map();
  const pending = [...manifests];
  const enqueueLockedPatches = (name, version) => {
    if (!lock) return;
    const selected = (lock.package ?? []).filter(item => item.name === name && item.version === version && !item.source);
    if (selected.length !== 1) throw Error(`Cargo.lock lacks local package ${name}@${version}`);
    reachable(selected, item => {
      if (!item.source) for (const patch of patches.filter(candidate => candidate.name === item.name)) {
        const file = slash(path.relative(root, path.join(patch.directory, 'Cargo.toml')));
        if (toml(root, file).package?.version !== item.version) continue;
        if (!manifests.has(file)) {manifests.add(file); pending.push(file);}
      }
      return lockedEdges(resolveLocked, item);
    });
  };
  for (const file of pending) if (!withinSource(path.dirname(path.resolve(root, file)))) throw Error(`workspace dependency escapes source root: ${file}`);
  tomlFiles(root, pending);
  while (pending.length) {
    const file = pending.shift(), manifest = toml(root, file), directory = path.dirname(path.resolve(root, file));
    if (!withinSource(directory)) throw Error(`workspace dependency escapes source root: ${file}`);
    if (!manifest.package) throw Error(`${file}: missing package`);
    let workspaceDirectory = directory;
    if (manifest.package.workspace) {
      workspaceDirectory = path.resolve(directory, manifest.package.workspace);
      if (!withinSource(workspaceDirectory)) throw Error(`${file}: workspace escapes source root`);
    }
    else {
      let cursor = directory;
      while (true) {
        const candidate = path.join(cursor, 'Cargo.toml');
        if (exists(candidate) && toml(root, slash(path.relative(root, candidate))).workspace) {workspaceDirectory = cursor; break;}
        if (cursor === sourceRoot) break;
        cursor = path.dirname(cursor);
      }
    }
    const workspace = toml(root, slash(path.relative(root, path.join(workspaceDirectory, 'Cargo.toml')))).workspace ?? {};
    const own = Object.fromEntries(Object.entries(manifest.package).map(([key, value]) =>
      [key, value?.workspace === true ? workspace.package?.[key] : value]));
    enqueueLockedPatches(own.name, own.version);
    const dependencies = [];
    for (const {target, kind, alias, value} of manifestDependencies(manifest)) {
      let declaration = typeof value === 'string' ? {version: value} : value;
      let base = directory;
      if (declaration.workspace) {
        const inherited = workspace.dependencies?.[alias];
        if (!inherited) throw Error(`${file}: missing workspace dependency ${alias}`);
        const entry = typeof inherited === 'string' ? {version: inherited} : inherited;
        declaration = {...entry, ...declaration, features: [...(entry.features ?? []), ...(declaration.features ?? [])]};
        base = workspaceDirectory;
      }
      const patched = declaration.path ? [] : patches.filter(patch => {
        if (patch.name !== (declaration.package ?? alias)) return false;
        const packageManifest = toml(root, slash(path.relative(root, path.join(patch.directory, 'Cargo.toml')))).package;
        if (!packageManifest || typeof packageManifest.version !== 'string') throw Error(`patch ${patch.name} lacks an explicit version`);
        if (!versionMatches(packageManifest.version, requirement(declaration.version))) return false;
        return !lock || (lock.package ?? []).some(item => item.name === patch.name && item.version === packageManifest.version && !item.source);
      });
      if (patched.length > 1) throw Error(`${file}: ambiguous local patch for ${declaration.package ?? alias}`);
      const local = declaration.path ? path.resolve(base, declaration.path) : patched[0]?.directory ?? null;
      if (local) {
        const next = slash(path.relative(root, path.join(local, 'Cargo.toml')));
        if (!withinSource(local)) throw Error(`workspace dependency escapes source root: ${next}`);
        const selected = kind !== 'dev' || members.has(file);
        if (selected && !manifests.has(next)) {manifests.add(next); pending.push(next);}
        if (selected && declaration.path && rootManifest.workspace && !next.startsWith('../') && !path.isAbsolute(next) && !excluded(next)) {
          const dependency = toml(root, next);
          const explicitWorkspace = dependency.package?.workspace && path.resolve(local, dependency.package.workspace);
          if (!dependency.workspace && (!explicitWorkspace || explicitWorkspace === root) && !members.has(next)) {
            members.add(next);
            if (packages.has(next)) pending.push(next);
          }
        }
      }
      dependencies.push({name: declaration.package ?? alias, rename: declaration.package ? alias : null,
        req: requirement(declaration.version), kind, target, optional: declaration.optional ?? false,
        uses_default_features: declaration['default-features'] ?? true, features: declaration.features ?? [],
        source: declaration.path ? null : declaration.git ? 'git+' + declaration.git + (declaration.rev ? '?rev=' + declaration.rev : declaration.branch ? '?branch=' + declaration.branch : declaration.tag ? '?tag=' + declaration.tag : '') : 'registry+https://github.com/rust-lang/crates.io-index', ...(local ? {resolved_path: local} : {}), ...(declaration.path ? {path: local} : {})});
    }
    const features = {...manifest.features};
    const explicit = new Set(Object.values(features).flat().filter(value => value.startsWith('dep:')).map(value => value.slice(4)));
    for (const dep of dependencies) {
      const alias = dep.rename ?? dep.name;
      if (dep.optional && !explicit.has(alias) && !Object.hasOwn(features, alias)) features[alias] = [`dep:${alias}`];
    }
    const targets = [];
    const target = (kind, declaration, fallback, name) => {
      const src = declaration.path ?? (exists(path.resolve(directory, fallback)) ? fallback : fallback.replace(/\.rs$/, '/main.rs'));
      if (!exists(path.resolve(directory, src))) return;
      const proc = kind === 'lib' && declaration['proc-macro'];
      targets.push({kind: proc ? ['proc-macro'] : kind === 'lib' ? declaration['crate-type'] ?? ['lib'] : [kind], crate_types: declaration['crate-type'] ?? [proc ? 'proc-macro' : kind === 'lib' ? 'lib' : 'bin'],
        name: declaration.name ?? name, src_path: path.resolve(directory, src), edition: declaration.edition ?? own.edition ?? '2015',
        doc: declaration.doc ?? true, doctest: declaration.doctest ?? kind === 'lib', test: declaration.test ?? !['example', 'bench'].includes(kind),
        ...(declaration['required-features'] ? {'required-features': declaration['required-features']} : {})});
    };
    target('lib', manifest.lib ?? {}, 'src/lib.rs', own.name.replaceAll('-', '_'));
    for (const [kind, plural, folder] of [['bin', 'bins', 'src/bin'], ['test', 'tests', 'tests'], ['example', 'examples', 'examples'], ['bench', 'benches', 'benches']]) {
      const declared = manifest[kind] ?? [];
      for (const item of declared) target(kind, item, kind === 'bin' && item.name === own.name && exists(path.join(directory, 'src/main.rs')) ? 'src/main.rs' : `${folder}/${item.name}.rs`, item.name);
      if (own[`auto${plural}`] === false) continue;
      if (kind === 'bin' && !targets.some(t => t.src_path === path.join(directory, 'src/main.rs'))) target(kind, {}, 'src/main.rs', own.name);
      if (exists(path.join(directory, folder))) for (const entry of fs.readdirSync(path.join(directory, folder), {withFileTypes: true})) {
        if (!entry.isDirectory() && !entry.name.endsWith('.rs')) continue;
        const name = entry.name.replace(/\.rs$/, '');
        const candidate = `${folder}/${entry.name}${entry.isDirectory() ? '/main.rs' : ''}`;
        if (!declared.some(item => item.name === name) && !targets.some(t => t.src_path === path.resolve(directory, candidate))) target(kind, {}, candidate, name);
      }
    }
    if (own.build !== false) target('custom-build', {}, typeof own.build === 'string' ? own.build : 'build.rs', 'build-script-build');
    const rank = t => t.kind.includes('custom-build') ? 5 : t.kind.includes('bench') ? 4 : t.kind.includes('test') ? 3 : t.kind.includes('example') ? 2 : t.kind.includes('bin') ? 1 : 0;
    targets.sort((a, b) => rank(a) - rank(b) || (a.kind.includes('test') && own.autotests === false || a.kind.includes('bin') && own.autobins === false ? 0 : a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    const p = {name: own.name, version: own.version, id: `path+${pathToFileURL(directory).href}#${path.basename(directory) === own.name ? own.version : `${own.name}@${own.version}`}`,
      source: null, dependencies, targets, features, manifest_path: path.join(directory, 'Cargo.toml'), workspace_member: members.has(file),
      publish: own.publish === false ? [] : own.publish === true ? null : own.publish ?? null, metadata: own.metadata ?? null, authors: own.authors ?? [], categories: own.categories ?? [], keywords: own.keywords ?? []};
    for (const key of ['license', 'license-file', 'description', 'repository', 'homepage', 'documentation', 'links', 'default-run', 'rust-version']) p[key.replaceAll('-', '_')] = own[key] ?? null;
    p.edition = own.edition ?? '2015';
    p.readme = own.readme === false ? null : own.readme ?? ['README.md', 'README.txt', 'README'].find(name => exists(path.join(directory, name))) ?? null;
    packages.set(file, p);
  }
  for (const p of packages.values()) p.workspace_member = members.has(slash(path.relative(root, p.manifest_path)));
  return [...packages.values()].sort((a, b) => a.name.localeCompare(b.name, 'en'));
}

function defaultWorkspaceMembers(root, local) {
  const members = local.filter(p => p.workspace_member);
  const patterns = toml(root, 'Cargo.toml').workspace?.['default-members'] ?? members.map(p => slash(path.relative(root, path.dirname(p.manifest_path))));
  return members.filter(p => patterns.some(pattern => path.matchesGlob(slash(path.relative(root, path.dirname(p.manifest_path))), pattern))).map(p => p.id);
}

// A relocatable Cargo-metadata-shaped graph for profile planning. Keep all
// local path owners, including excluded dependencies, and their real targets.
export function workspaceProfileMetadata(root, {workspaceOnly = false, sourceRoot = root, additionalInputs = [], patchConfig, lockFile, universeFile} = {}) {
  root = path.resolve(root);
  sourceRoot = path.resolve(sourceRoot);
  const selectedLock = lockFile ?? slash(path.relative(sourceRoot, path.join(root, 'Cargo.lock')));
  const packages = workspacePackages(root, {sourceRoot, patchConfig, lockFile: workspaceOnly ? lockFile : selectedLock});
  const members = packages.filter(p => p.workspace_member);
  const manifest = slash(path.relative(sourceRoot, path.join(root, 'Cargo.toml')));
  const inputs = new Set([manifest, ...additionalInputs, ...packages.map(p => slash(path.relative(sourceRoot, p.manifest_path)))]);
  if (exists(path.join(root, 'Cargo.lock'))) inputs.add(slash(path.relative(sourceRoot, path.join(root, 'Cargo.lock'))));
  if (patchConfig) inputs.add(slash(patchConfig));
  if (lockFile) inputs.add(slash(lockFile));
  if (!workspaceOnly && universeFile) inputs.add(slash(universeFile));
  const digests = Object.fromEntries([...inputs].sort().map(file => {
    if (path.isAbsolute(file) || file.split('/').some(part => !part || part === '.' || part === '..') || file.includes('\\')) throw Error(`workspace graph input escapes root: ${file}`);
    return [file, `sha256:${createHash('sha256').update(fs.readFileSync(path.join(sourceRoot, file))).digest('hex')}`];
  }));
  const metadata = toml(root, 'Cargo.toml').workspace?.metadata ?? {};
  if (Object.hasOwn(metadata, 'acyclic_profile_inputs_v1')) throw Error('workspace metadata uses the reserved profile input key');
  if (Object.hasOwn(metadata, 'acyclic_profile_manifest_v1')) throw Error('workspace metadata uses the reserved profile manifest key');
  if (Object.hasOwn(metadata, 'acyclic_profile_local_resolution_v1')) throw Error('workspace metadata uses the reserved profile resolution key');
  if (Object.hasOwn(metadata, 'acyclic_profile_selection_v1')) throw Error('workspace metadata uses the reserved profile selection key');
  // Cargo.lock is the all-platform resolution. Bazel's target-specific pins
  // omit registry dependencies that aren't compiled in that configuration.
  const lock = workspaceOnly ? null : toml(sourceRoot, selectedLock);
  const reached = lock && reachable(members.map(p => lockedOwner(lock, p)),
    lockedEdges.bind(null, lockDependencyIndex(lock.package)));
  return {
    packages: packages.filter(p => !workspaceOnly || p.workspace_member)
      .map(({workspace_member, ...p}) => p),
    workspace_members: members.map(p => p.id),
    workspace_default_members: defaultWorkspaceMembers(root, packages),
    workspace_root: sourceRoot,
    target_directory: path.join(root, 'target'),
    version: 1,
    metadata: {...metadata, acyclic_profile_inputs_v1: digests, acyclic_profile_manifest_v1: manifest,
      acyclic_profile_selection_v1: {patch_config: patchConfig ? slash(patchConfig) : null,
        lock_file: slash(selectedLock)},
      ...(lock && reached ? {acyclic_profile_local_resolution_v1: packages.filter(p => reached.has(lockedOwner(lock, p))).map(p => p.id)} : {})},
    resolve: null,
  };
}

function targetMatches(cfg, triple) {
  const wasm = triple === 'wasm32-unknown-unknown';
  const platform = new Set([
    wasm ? 'wasm' : triple.includes('windows') ? 'windows' : 'unix', `target_os=${JSON.stringify(wasm ? 'unknown' : triple.includes('windows') ? 'windows' : triple.includes('darwin') ? 'macos' : 'linux')}`,
    `target_arch=${JSON.stringify(wasm ? 'wasm32' : triple.startsWith('aarch64') ? 'aarch64' : 'x86_64')}`, `target_pointer_width="${wasm ? '32' : '64'}"`, 'target_endian="little"',
    `target_env=${JSON.stringify(wasm ? '' : triple.includes('msvc') ? 'msvc' : triple.includes('darwin') ? '' : 'gnu')}`, `target_family=${JSON.stringify(wasm ? 'wasm' : triple.includes('windows') ? 'windows' : 'unix')}`,
    `target_vendor=${JSON.stringify(triple.includes('darwin') ? 'apple' : triple.includes('windows') ? 'pc' : 'unknown')}`,
  ]);
  if (!cfg || cfg === triple) return true;
  if (!cfg.startsWith('cfg(')) return false;
  if (!cfg.endsWith(')')) throw Error(`invalid target condition ${cfg}`);
  return evaluateCfg(cfg.slice(4, -1), (name, value, children) => {
    if (children) throw Error(`unknown target condition ${cfg}`);
    return platform.has(value === undefined ? name : `${name}="${value}"`);
  });
}

function lockedOwner(lock, {name, version}) {
  const owner = (lock.package ?? []).find(pkg => pkg.name === name && pkg.version === version && !pkg.source);
  if (!owner) throw Error(`Cargo.lock lacks local package ${name}@${version}`);
  return owner;
}

function lockedCandidates(resolve, declaration, owner) {
  return [...new Set((owner.dependencies ?? []).flatMap(ref => resolve(ref, declaration.name)))]
    .filter(pkg => (!declaration.source || pkg.source?.split('#')[0] === declaration.source) && versionMatches(pkg.version, declaration.req));
}

function uniqueDependency(owner, declaration, matches) {
  if (matches.length !== 1) throw Error(`${owner.name}: ambiguous or missing locked dependency ${declaration.name}`);
  return matches[0];
}

function applyRequests(add, enabled, dependency, declaration, features) {
  for (const feature of declaration.features) add(enabled, feature);
  if (declaration.uses_default_features && Object.hasOwn(dependency.features, 'default')) add(enabled, 'default');
  for (const feature of forwardedFeatures(features, declaration.rename ?? declaration.name)) add(enabled, feature);
}

function recordRegistryRequests(input, declaration, features) {
  for (const feature of declaration.features) input.features.add(feature);
  for (const feature of forwardedFeatures(features, declaration.rename ?? declaration.name)) input.features.add(feature);
  input.defaultFeatures ||= declaration.uses_default_features;
}

// Inputs to crate_universe come from the current Cargo lock and declarations.
// The resolved features themselves remain crate_universe's responsibility.
export function registryInputs(root) {
  const local = workspacePackages(root), lock = toml(root, 'Cargo.lock');
  const universe = bazelResolution(path.join(root, 'cargo-bazel-lock.json'), true) ?? {crates: {}, conditions: {}};
  const candidates = (lock.package ?? []).filter(p => p.source?.startsWith('registry+')).map(p => ({
    ...p, features: new Set(), defaultFeatures: false,
  }));
  const resolve = lockDependencyIndex(candidates);
  const inputs = new Set();
  for (const triple of ['x86_64-unknown-linux-gnu', 'aarch64-apple-darwin', 'x86_64-pc-windows-msvc']) {
    const enabled = new Map(local.map(p => [p.id, new Set(p.workspace_member ? Object.keys(p.features) : [])]));
    const reachable = new Set(local.filter(p => p.workspace_member).map(p => p.id));
    growSets(add => {
      for (const p of local.filter(p => reachable.has(p.id))) {
        const features = enabled.get(p.id);
        for (const feature of features) for (const next of p.features[feature] ?? []) add(features, next);
        const locked = lockedOwner(lock, p);
        for (const d of p.dependencies) {
          if ((d.kind === 'dev' && !p.workspace_member) || !targetMatches(d.target, triple) ||
              !dependencyEnabled(features, d)) continue;
          if (d.resolved_path) {
            const dep = local.find(x => path.dirname(x.manifest_path) === d.resolved_path);
            if (!dep) throw Error(`${p.name}: missing path dependency ${d.name}`);
            add(reachable, dep.id);
            applyRequests(add, enabled.get(dep.id), dep, d, features);
          } else if (d.source?.startsWith('registry+')) {
            const input = uniqueDependency(p, d, lockedCandidates(resolve, d, locked));
            inputs.add(input);
            recordRegistryRequests(input, d, features);
          }
        }
      }
    });
  }
  return {inputs: [...inputs], universe};
}

export function assertRegistryPinsCurrent(root) {
  const {inputs, universe} = registryInputs(root);
  const recovery = 'node scripts/repin-bazel.mjs';
  for (const p of inputs) {
    const pinned = universe.crates[`${p.name} ${p.version}`];
    if (!pinned) throw Error(`STEP_FAILED bazel-build-files: registry package ${p.name} ${p.version} missing from Bazel lock; repin: ${recovery}`);
    const features = new Set(selectedEntries(pinned.common_attrs?.crate_features, universe, null));
    for (const feature of p.features) if (!features.has(feature))
      throw Error(`STEP_FAILED bazel-build-files: ${p.name} ${p.version} feature ${feature} missing from Bazel lock; repin: ${recovery}`);
  }
}

function registryPackages(lock, universe, triple, {bazelTargets = [], sourceRoots = {}, bazelRules = new Map()} = {}) {
  const sources = new Map(Object.entries(sourceRoots).map(([source, directory]) => [source, workspacePackages(directory)]));
  const external = (lock.package ?? []).filter(pkg => pkg.source);
  const byPin = Map.groupBy(external, pkg => pkg.name + ' ' + pkg.version);
  const rules = bazelTargets.map(target => target.rule).filter(rule => ['rust_library', 'rust_proc_macro'].includes(rule?.ruleClass));
  return external.map(p => {
    const baseSource = p.source.split(/[?#]/)[0];
    let checkout = sources.get(p.source);
    if (!checkout && sources.has(baseSource)) {
      const revisions = new Set(lock.package.filter(x => x.source?.split(/[?#]/)[0] === baseSource).map(x => x.source));
      if (revisions.size !== 1) throw Error(`ambiguous Git source root ${baseSource}; supply exact locked-source keys`);
      checkout = sources.get(baseSource);
    }
    const manifest = checkout?.find(x => x.name === p.name && x.version === p.version);
    const matches = rules.filter(rule => {
      const attribute = name => rule.attribute?.find(a => a.name === name)?.stringValue;
      return rule.name.includes(`__${p.name}-${p.version}//`) || attribute('version') === p.version &&
        (attribute('crate_name') === p.name.replaceAll('-', '_') || rule.name.split(':').at(-1) === p.name.replaceAll('-', '_'));
    });
    const identities = new Set((byPin.get(p.name + ' ' + p.version) ?? []).map(x => x.source));
    if (identities.size > 1 && (matches.length || universe.crates[`${p.name} ${p.version}`]))
      throw Error(`ambiguous Bazel metadata for ${p.name}@${p.version}: multiple locked sources`);
    if (new Set(matches.map(rule => rule.name)).size > 1)
      throw Error(`ambiguous Bazel metadata for ${p.name}@${p.version}: multiple library rules`);
    const resolved = matches[0];
    const attribute = name => resolved?.attribute?.find(a => a.name === name);
    const pinned = universe.crates[`${p.name} ${p.version}`] ?? (resolved ? {
      targets: [{[resolved.ruleClass === 'rust_proc_macro' ? 'ProcMacro' : 'Library']: {crate_name: attribute('crate_name')?.stringValue || resolved.name.split(':').at(-1)}}],
      common_attrs: {crate_features: attribute('crate_features')?.stringListValue ?? []},
    } : undefined);
    const lib = pinned?.targets.find(t => t.Library || t.ProcMacro);
    const target = lib?.Library ?? lib?.ProcMacro;
    const library = target && {kind: [lib.ProcMacro ? 'proc-macro' : 'lib'], name: target.crate_name};
    const targets = [...(manifest?.targets ?? [])];
    if (library) {
      const index = targets.findIndex(target => target.kind.some(kind => ['lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro'].includes(kind)));
      if (index === -1) targets.push(library);
      else {
        const manifestTarget = targets[index];
        const kind = lib.ProcMacro || manifestTarget.kind.includes('proc-macro') ? library.kind : manifestTarget.kind;
        targets[index] = {...manifestTarget, name: library.name, kind};
      }
    }
    if (resolved) bazelRules.set(`${p.source}#${p.name}@${p.version}`, resolved);
    return {...p, id: `${p.source}#${p.name}@${p.version}`, manifest_path: manifest?.manifest_path ?? '', dependencies: [], metadata: manifest?.metadata ?? null,
      features: Object.fromEntries(selectedEntries(pinned?.common_attrs?.crate_features, universe, triple).map(f => [f, []])),
      targets};
  });
}

function* pinnedEdges(pinned, universe, triple) {
  for (const [attributes, kind] of [[pinned?.common_attrs, null], [pinned?.build_script_attrs, 'build']])
    for (const entry of [...selectedEntries(attributes?.deps, universe, triple), ...selectedEntries(attributes?.proc_macro_deps, universe, triple)])
      yield {entry, kind};
}

// Browser builds start from the published packages, rather than the native
// all-workspace, all-features test graph. Registry edges and resolved features
// come from the browser crate-universe lock; local features come from manifests.
export function workspaceBrowserGraph(root, selections) {
  root = path.resolve(root);
  const triple = 'wasm32-unknown-unknown';
  const local = workspacePackages(root), lock = toml(root, 'Cargo.lock');
  const universe = JSON.parse(fs.readFileSync(path.join(root, 'cargo-wasm-bazel-lock.json'), 'utf8'));
  const registry = registryPackages(lock, universe, triple);
  const resolve = lockDependencyIndex(registry);
  const packages = [...local, ...registry], byId = new Map(packages.map(p => [p.id, p]));
  const byPin = Map.groupBy(packages, p => `${p.name} ${p.version}`);
  const byPath = new Map(local.map(p => [path.dirname(p.manifest_path), p]));
  const enabled = new Map(local.map(p => [p.id, new Set()]));
  const reachable = new Set();
  const registryInputs = new Map();
  for (const args of selections) {
    const name = args[args.indexOf('-p') + 1], matches = local.filter(p => p.name === name);
    if (matches.length !== 1) throw Error(`browser package ${name}: expected one workspace package`);
    const pkg = matches[0], features = enabled.get(pkg.id);
    reachable.add(pkg.id);
    if (!args.includes('--no-default-features') && Object.hasOwn(pkg.features, 'default')) features.add('default');
    if (args.includes('--features')) for (const feature of args[args.indexOf('--features') + 1].split(',')) features.add(feature);
  }
  const localDependency = (pkg, d) => {
    if (d.resolved_path) return byPath.get(d.resolved_path);
    return uniqueDependency(pkg, d, lockedCandidates(resolve, d, lockedOwner(lock, pkg)));
  };
  const nodes = new Map();
  growSets(add => {
    for (const pkg of packages.filter(p => reachable.has(p.id))) {
      const deps = new Map();
      const addEdge = (name, dep, kind, target = null) => {
        if (!dep) throw Error(`${pkg.name}: missing dependency ${name}`);
        const key = `${name}:${dep.id}`;
        if (!deps.has(key)) deps.set(key, {name, pkg:dep.id, dep_kinds:[]});
        deps.get(key).dep_kinds.push({kind, target});
      };
      if (pkg.source) {
        const pinned = universe.crates[`${pkg.name} ${pkg.version}`];
        if (!pinned) throw Error(`browser Bazel lock lacks ${pkg.name}@${pkg.version}`);
        for (const {entry, kind} of pinnedEdges(pinned, universe, triple)) {
          const matches = byPin.get(entry.id) ?? [];
          if (matches.length !== 1) throw Error(`${pkg.name}: missing or ambiguous browser lock edge ${entry.id}`);
          const dep = matches[0];
          addEdge(entry.alias ?? entry.target, dep, kind);
          if (kind === null) add(reachable, dep.id);
        }
      } else {
        const features = enabled.get(pkg.id);
        for (const feature of features) for (const next of pkg.features[feature] ?? []) add(features, next);
        for (const d of pkg.dependencies) {
          const alias = d.rename ?? d.name;
          if (d.kind === 'dev' || !targetMatches(d.target, triple) ||
              !dependencyEnabled(features, d)) continue;
          const dep = localDependency(pkg, d);
          addEdge(alias.replaceAll('-', '_'), dep, d.kind, d.target);
          if (d.kind !== null) continue;
          add(reachable, dep.id);
          if (dep.source) {
            const input = registryInputs.get(dep.id) ?? {features:new Set(), defaultFeatures:false};
            recordRegistryRequests(input, d, features);
            registryInputs.set(dep.id, input);
          } else {
            applyRequests(add, enabled.get(dep.id), dep, d, features);
          }
        }
      }
      nodes.set(pkg.id, {id:pkg.id, deps:[...deps.values()], features:pkg.source ? Object.keys(pkg.features).sort() :
        Object.keys(pkg.features).filter(feature => enabled.get(pkg.id).has(feature)).sort()});
    }
  });
  return {packages:[...byId.values()].filter(p => !p.source || reachable.has(p.id)), workspace_members:local.filter(p => p.workspace_member).map(p => p.id),
    workspace_root:root, registry_inputs:registryInputs, resolve:{nodes:[...nodes.values()]}};
}

export function workspaceGraph(root, triple = 'x86_64-unknown-linux-gnu', {sourceRoot = root, patchConfig, lockFile, universeFile, resolution, bazelTargets = [], sourceRoots = {}} = {}) {
  const allPlatforms = triple === null;
  triple ??= 'x86_64-unknown-linux-gnu';
  root = path.resolve(root);
  const local = workspacePackages(root, {sourceRoot, patchConfig, lockFile}), lock = lockFile ? toml(sourceRoot, lockFile) : toml(root, 'Cargo.lock');
  const universePath = universeFile ? path.resolve(sourceRoot, universeFile) : path.join(root, 'cargo-bazel-lock.json');
  const universeRelative = path.relative(path.resolve(sourceRoot), universePath);
  if (universeRelative === '..' || universeRelative.startsWith(`..${path.sep}`) || path.isAbsolute(universeRelative)) throw Error('workspace universe escapes source root');
  const pinnedResolution = resolution ?? bazelResolution(universePath, !universeFile);
  const universe = pinnedResolution ?? {crates: {}, conditions: {}};
  const resolvedFeatures = pinnedResolution !== null || bazelTargets.length > 0;
  const bazelRules = new Map();
  const registry = registryPackages(lock, universe, allPlatforms ? null : triple, {bazelTargets, sourceRoots, bazelRules});
  const packagesByRule = new Map(registry.flatMap(pkg => bazelRules.has(pkg.id) ? [[bazelRules.get(pkg.id).name, pkg]] : []));
  const packages = [...local, ...registry];
  const resolve = lockDependencyIndex(packages);
  const byPin = Map.groupBy(packages, p => `${p.name} ${p.version}`);
  const matches = cfg => allPlatforms || targetMatches(cfg, triple);
  const enabled = new Map(local.map(p => [p.id, new Set(p.workspace_member ? Object.keys(p.features) : selectedEntries(universe.crates[`${p.name} ${p.version}`]?.common_attrs?.crate_features, universe, allPlatforms ? null : triple))]));
  const active = (p, d) => dependencyEnabled(enabled.get(p.id), d);
  growSets(add => {
    for (const p of local) {
      const features = enabled.get(p.id);
      for (const feature of features) for (const next of p.features[feature] ?? []) add(features, next);
      for (const d of p.dependencies.filter(d => (d.kind !== 'dev' || p.workspace_member) && matches(d.target) && active(p, d) && d.resolved_path)) {
        const dependency = local.find(x => path.dirname(x.manifest_path) === d.resolved_path);
        applyRequests(add, enabled.get(dependency.id), dependency, d, features);
      }
    }
  });
  const nodes = local.map(p => {
    const locked = lockedOwner(lock, p);
    const deps = new Map();
    for (const d of p.dependencies.filter(d => (d.kind !== 'dev' || p.workspace_member) && matches(d.target) && active(p, d))) {
      const dependency = uniqueDependency(p, d, d.resolved_path
        ? local.filter(x => path.dirname(x.manifest_path) === d.resolved_path) : lockedCandidates(resolve, d, locked));
      const name = (d.rename ?? d.name).replaceAll('-', '_');
      if (dependency.source && resolvedFeatures) {
        const requested = [...d.features, ...forwardedFeatures(enabled.get(p.id), d.rename ?? d.name)];
        for (const feature of requested) if (!Object.hasOwn(dependency.features, feature))
          throw Error(`${p.name}: ${d.name} feature ${feature} missing from Bazel resolution; repin the crate graph`);
      }
      const key = `${name}:${dependency.id}`;
      if (!deps.has(key)) deps.set(key, {name, pkg: dependency.id, dep_kinds: []});
      deps.get(key).dep_kinds.push({kind: d.kind, target: d.target});
    }
    return {id: p.id, deps: [...deps.values()], features: Object.keys(p.features).filter(f => enabled.get(p.id).has(f)).sort()};
  });
  nodes.push(...registry.map(p => {
    const pinned = universe.crates[`${p.name} ${p.version}`];
    const deps = [];
    let incomplete = !pinned && !bazelRules.has(p.id);
    for (const {entry, kind} of pinnedEdges(pinned, universe, allPlatforms ? null : triple)) {
      const candidates = byPin.get(entry.id) ?? [];
      if (candidates.length !== 1) {incomplete = true; continue;}
      deps.push({name: entry.alias ?? entry.target, pkg: candidates[0].id, dep_kinds: [{kind, target: null}]});
    }
    if (!pinned) {
      const rule = bazelRules.get(p.id);
      for (const attribute of rule?.attribute ?? []) {
        if (!['deps', 'proc_macro_deps'].includes(attribute.name)) continue;
        for (const label of attribute.stringListValue ?? []) {
          const dependency = packagesByRule.get(label);
          if (dependency) deps.push({name: label.split(':').at(-1), pkg: dependency.id, dep_kinds: [{kind: null, target: null}]});
        }
      }
    }
    return {id: p.id, deps, features: Object.keys(p.features), incomplete};
  }));
  return {packages, workspace_members: local.filter(p => p.workspace_member).map(p => p.id), workspace_root: root,
    workspace_default_members: defaultWorkspaceMembers(root, local), version: 1,
    metadata: toml(root, 'Cargo.toml').workspace?.metadata ?? null, resolve: {nodes}};
}
