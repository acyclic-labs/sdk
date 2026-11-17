// Repository declarations and Bazel's committed crate resolution are the metadata
// authority. Reading them never resolves, downloads or builds a Cargo workspace.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {dependencyEnabled, evaluateCfg, forwardedFeatures, manifestDependencies, reachable, toml, tomlFiles} from './policy-core.mjs';

const slash = value => value.replaceAll('\\', '/');
const exists = file => fs.existsSync(file);
const requirement = value => value === undefined ? '*' : /^\d/.test(value) ? `^${value}` : value;
const selectedEntries = (value, universe, triple) => Array.isArray(value) ? value :
  [...(value?.common ?? []), ...Object.entries(value?.selects ?? {}).flatMap(([cfg, entries]) => triple === null || (universe.conditions[cfg] ?? [cfg]).includes(triple) ? entries : [])];
export function compareVersions(a, b) {
  const parts = value => value.split('+')[0].split(/[.-]/).map(x => /^\d+$/.test(x) ? Number(x) : x);
  const aa = parts(a), bb = parts(b);
  for (let n = 0; n < Math.max(aa.length, bb.length); n++) {
    if (aa[n] === bb[n]) continue;
    if (aa[n] === undefined) return n >= 3 ? 1 : -1;
    if (bb[n] === undefined) return n >= 3 ? -1 : 1;
    return aa[n] < bb[n] ? -1 : 1;
  }
  return 0;
}
export function versionMatches(version, requirement) {
  return requirement.split(',').every(part => {
    const match = /^\s*(\^|~|=|>=|<=|>|<)?\s*(\d+|\*)(?:\.(\d+|\*))?(?:\.(\d+|\*))?\s*$/.exec(part);
    if (!match) throw Error(`unsupported locked version requirement ${requirement}`);
    const [, operator = '^', ...fields] = match;
    const count = fields.findIndex(x => x === undefined || x === '*');
    const length = count < 0 ? 3 : count;
    if (!length) return true;
    const lower = fields.map(x => x === undefined || x === '*' ? 0 : Number(x));
    const comparison = compareVersions(version, lower.join('.'));
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
      return (item.dependencies ?? []).map(dependency => {
        const match = /^(\S+)(?: (\S+))?(?: \((.+)\))?$/.exec(dependency);
        if (!match) throw Error(`invalid Cargo.lock dependency ${dependency}`);
        const candidates = (lock.package ?? []).filter(candidate => candidate.name === match[1] &&
          (!match[2] || candidate.version === match[2]) && (!match[3] || candidate.source === match[3]));
        if (candidates.length !== 1) throw Error(`ambiguous or missing Cargo.lock dependency ${dependency}`);
        return candidates[0];
      });
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
        source: declaration.path ? null : 'registry+https://github.com/rust-lang/crates.io-index', ...(local ? {resolved_path: local} : {}), ...(declaration.path ? {path: local} : {})});
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
    const p = {name: own.name, version: own.version, id: `path+file://${slash(directory)}#${path.basename(directory) === own.name ? own.version : `${own.name}@${own.version}`}`,
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
  // The resolved variant uses the same lock-validated graph as the Bazel
  // generator; the workspace-only view matches Cargo's no-deps selection.
  const options = {sourceRoot, patchConfig, lockFile, universeFile};
  const resolved = workspaceOnly ? null : workspaceGraph(root, null, options);
  const packages = workspaceOnly ? workspacePackages(root, options) : resolved.packages.filter(p => p.source === null);
  const members = packages.filter(p => p.workspace_member).map(p => p.id);
  const manifest = slash(path.relative(sourceRoot, path.join(root, 'Cargo.toml')));
  const inputs = new Set([manifest, ...additionalInputs, ...packages.map(p => slash(path.relative(sourceRoot, p.manifest_path)))]);
  if (exists(path.join(root, 'Cargo.lock'))) inputs.add(slash(path.relative(sourceRoot, path.join(root, 'Cargo.lock'))));
  if (patchConfig) inputs.add(slash(patchConfig));
  if (lockFile) inputs.add(slash(lockFile));
  if (!workspaceOnly) inputs.add(universeFile ? slash(universeFile) : slash(path.relative(sourceRoot, path.join(root, 'cargo-bazel-lock.json'))));
  const digests = Object.fromEntries([...inputs].sort().map(file => {
    if (path.isAbsolute(file) || file.split('/').some(part => !part || part === '.' || part === '..') || file.includes('\\')) throw Error(`workspace graph input escapes root: ${file}`);
    return [file, `sha256:${createHash('sha256').update(fs.readFileSync(path.join(sourceRoot, file))).digest('hex')}`];
  }));
  const metadata = toml(root, 'Cargo.toml').workspace?.metadata ?? {};
  if (Object.hasOwn(metadata, 'acyclic_profile_inputs_v1')) throw Error('workspace metadata uses the reserved profile input key');
  if (Object.hasOwn(metadata, 'acyclic_profile_manifest_v1')) throw Error('workspace metadata uses the reserved profile manifest key');
  if (Object.hasOwn(metadata, 'acyclic_profile_local_resolution_v1')) throw Error('workspace metadata uses the reserved profile resolution key');
  if (Object.hasOwn(metadata, 'acyclic_profile_selection_v1')) throw Error('workspace metadata uses the reserved profile selection key');
  let reached;
  if (resolved) {
    const nodes = new Map(resolved.resolve.nodes.map(node => [node.id, node]));
    reached = reachable(members, id => {
      const node = nodes.get(id);
      if (!node || node.incomplete) throw Error(`workspace graph has an incomplete resolution for ${id}`);
      return node.deps.map(dependency => dependency.pkg);
    });
  }
  return {
    packages: packages.filter(p => !workspaceOnly || p.workspace_member)
      .map(({workspace_member, ...p}) => p),
    workspace_members: members,
    workspace_default_members: defaultWorkspaceMembers(root, packages),
    workspace_root: sourceRoot,
    target_directory: path.join(root, 'target'),
    version: 1,
    metadata: {...metadata, acyclic_profile_inputs_v1: digests, acyclic_profile_manifest_v1: manifest,
      acyclic_profile_selection_v1: {patch_config: patchConfig ? slash(patchConfig) : null,
        lock_file: lockFile ? slash(lockFile) : slash(path.relative(sourceRoot, path.join(root, 'Cargo.lock')))},
      ...(resolved ? {acyclic_profile_local_resolution_v1: packages.filter(p => reached.has(p.id)).map(p => p.id)} : {})},
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

function lockedCandidates(candidates, declaration, owner) {
  return candidates.filter(pkg => pkg.name === declaration.name && versionMatches(pkg.version, declaration.req) &&
    (owner.dependencies ?? []).some(key =>
      key === pkg.name || key === `${pkg.name} ${pkg.version}` || key.startsWith(`${pkg.name} ${pkg.version} (`)));
}

function uniqueDependency(owner, declaration, matches) {
  if (matches.length !== 1) throw Error(`${owner.name}: ambiguous or missing locked dependency ${declaration.name}`);
  return matches[0];
}

// Inputs to crate_universe come from the current Cargo lock and declarations.
// The resolved features themselves remain crate_universe's responsibility.
export function registryInputs(root) {
  const local = workspacePackages(root), lock = toml(root, 'Cargo.lock');
  const universe = JSON.parse(fs.readFileSync(path.join(root, 'cargo-bazel-lock.json'), 'utf8'));
  const candidates = (lock.package ?? []).filter(p => p.source?.startsWith('registry+')).map(p => ({
    ...p, features: new Set(), defaultFeatures: false,
  }));
  const inputs = new Set();
  for (const triple of ['x86_64-unknown-linux-gnu', 'aarch64-apple-darwin', 'x86_64-pc-windows-msvc']) {
    const enabled = new Map(local.map(p => [p.id, new Set(p.workspace_member ? Object.keys(p.features) : [])]));
    const reachable = new Set(local.filter(p => p.workspace_member).map(p => p.id));
    let changed = true;
    while (changed) {
      changed = false;
      const add = (set, value) => { if (!set.has(value)) {set.add(value); changed = true;} };
      for (const p of local.filter(p => reachable.has(p.id))) {
        const features = enabled.get(p.id);
        for (const feature of features) for (const next of p.features[feature] ?? []) add(features, next);
        const locked = lockedOwner(lock, p);
        for (const d of p.dependencies) {
          if ((d.kind === 'dev' && !p.workspace_member) || !targetMatches(d.target, triple) ||
              !dependencyEnabled(features, d)) continue;
          const requested = [...d.features, ...forwardedFeatures(features, d.rename ?? d.name)];
          if (d.resolved_path) {
            const dep = local.find(x => path.dirname(x.manifest_path) === d.resolved_path);
            if (!dep) throw Error(`${p.name}: missing path dependency ${d.name}`);
            add(reachable, dep.id);
            const target = enabled.get(dep.id);
            for (const feature of requested) add(target, feature);
            if (d.uses_default_features && Object.hasOwn(dep.features, 'default')) add(target, 'default');
          } else {
            const input = uniqueDependency(p, d, lockedCandidates(candidates, d, locked));
            inputs.add(input);
            for (const feature of requested) input.features.add(feature);
            input.defaultFeatures ||= d.uses_default_features;
          }
        }
      }
    }
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

function registryPackages(lock, universe, triple) {
  return (lock.package ?? []).filter(p => p.source).map(p => {
    const pinned = universe.crates[`${p.name} ${p.version}`];
    const lib = pinned?.targets.find(t => t.Library || t.ProcMacro);
    const target = lib?.Library ?? lib?.ProcMacro;
    return {...p, id: `${p.source}#${p.name}@${p.version}`, manifest_path: '', dependencies: [], metadata: null,
      features: Object.fromEntries(selectedEntries(pinned?.common_attrs?.crate_features, universe, triple).map(f => [f, []])),
      targets: target ? [{kind: [lib.ProcMacro ? 'proc-macro' : 'lib'], name: target.crate_name}] : []};
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
    return uniqueDependency(pkg, d, lockedCandidates(registry, d, lockedOwner(lock, pkg)));
  };
  const nodes = new Map();
  let changed = true;
  while (changed) {
    changed = false;
    const add = (set, value) => {if (!set.has(value)) {set.add(value); changed = true;}};
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
          const requestedFeatures = [...d.features, ...forwardedFeatures(features, alias)];
          if (dep.source) {
            const input = registryInputs.get(dep.id) ?? {features:new Set(), defaultFeatures:false};
            for (const feature of requestedFeatures) input.features.add(feature);
            input.defaultFeatures ||= d.uses_default_features;
            registryInputs.set(dep.id, input);
          } else {
            const requested = enabled.get(dep.id);
            for (const feature of requestedFeatures) add(requested, feature);
            if (d.uses_default_features && Object.hasOwn(dep.features, 'default')) add(requested, 'default');
          }
        }
      }
      nodes.set(pkg.id, {id:pkg.id, deps:[...deps.values()], features:pkg.source ? Object.keys(pkg.features).sort() :
        Object.keys(pkg.features).filter(feature => enabled.get(pkg.id).has(feature)).sort()});
    }
  }
  return {packages:[...byId.values()].filter(p => !p.source || reachable.has(p.id)), workspace_members:local.filter(p => p.workspace_member).map(p => p.id),
    workspace_root:root, registry_inputs:registryInputs, resolve:{nodes:[...nodes.values()]}};
}

export function workspaceGraph(root, triple = 'x86_64-unknown-linux-gnu', {sourceRoot = root, patchConfig, lockFile, universeFile} = {}) {
  const allPlatforms = triple === null;
  triple ??= 'x86_64-unknown-linux-gnu';
  root = path.resolve(root);
  const local = workspacePackages(root, {sourceRoot, patchConfig, lockFile}), lock = lockFile ? toml(sourceRoot, lockFile) : toml(root, 'Cargo.lock');
  const universePath = universeFile ? path.resolve(sourceRoot, universeFile) : path.join(root, 'cargo-bazel-lock.json');
  const universeRelative = path.relative(path.resolve(sourceRoot), universePath);
  if (universeRelative === '..' || universeRelative.startsWith(`..${path.sep}`) || path.isAbsolute(universeRelative)) throw Error('workspace universe escapes source root');
  const universe = JSON.parse(fs.readFileSync(universePath, 'utf8'));
  const registry = registryPackages(lock, universe, allPlatforms ? null : triple);
  const packages = [...local, ...registry];
  const byPin = Map.groupBy(packages, p => `${p.name} ${p.version}`);
  const matches = cfg => allPlatforms || targetMatches(cfg, triple);
  const enabled = new Map(local.map(p => [p.id, new Set(p.workspace_member ? Object.keys(p.features) : selectedEntries(universe.crates[`${p.name} ${p.version}`]?.common_attrs?.crate_features, universe, allPlatforms ? null : triple))]));
  const active = (p, d) => dependencyEnabled(enabled.get(p.id), d);
  let changed = true;
  while (changed) {
    changed = false;
    const add = (set, value) => {if (!set.has(value)) {set.add(value); changed = true;}};
    for (const p of local) {
      const features = enabled.get(p.id);
      for (const feature of features) for (const next of p.features[feature] ?? []) add(features, next);
      for (const d of p.dependencies.filter(d => (d.kind !== 'dev' || p.workspace_member) && matches(d.target) && active(p, d) && d.resolved_path)) {
        const dependency = local.find(x => path.dirname(x.manifest_path) === d.resolved_path);
        const target = enabled.get(dependency.id), alias = d.rename ?? d.name;
        for (const feature of d.features) add(target, feature);
        if (d.uses_default_features && Object.hasOwn(dependency.features, 'default')) add(target, 'default');
        for (const feature of forwardedFeatures(features, alias)) add(target, feature);
      }
    }
  }
  const nodes = local.map(p => {
    const locked = lockedOwner(lock, p);
    const deps = new Map();
    for (const d of p.dependencies.filter(d => (d.kind !== 'dev' || p.workspace_member) && matches(d.target) && active(p, d))) {
      const dependency = uniqueDependency(p, d, d.resolved_path
        ? local.filter(x => path.dirname(x.manifest_path) === d.resolved_path) : lockedCandidates(packages, d, locked));
      const name = (d.rename ?? d.name).replaceAll('-', '_');
      const key = `${name}:${dependency.id}`;
      if (!deps.has(key)) deps.set(key, {name, pkg: dependency.id, dep_kinds: []});
      deps.get(key).dep_kinds.push({kind: d.kind, target: d.target});
    }
    return {id: p.id, deps: [...deps.values()], features: Object.keys(p.features).filter(f => enabled.get(p.id).has(f)).sort()};
  });
  nodes.push(...registry.map(p => {
    const pinned = universe.crates[`${p.name} ${p.version}`];
    const deps = [];
    let incomplete = !pinned;
    for (const {entry, kind} of pinnedEdges(pinned, universe, allPlatforms ? null : triple)) {
      const candidates = byPin.get(entry.id) ?? [];
      if (candidates.length !== 1) {incomplete = true; continue;}
      deps.push({name: entry.alias ?? entry.target, pkg: candidates[0].id, dep_kinds: [{kind, target: null}]});
    }
    return {id: p.id, deps, features: Object.keys(p.features), incomplete};
  }));
  return {packages, workspace_members: local.filter(p => p.workspace_member).map(p => p.id), workspace_root: root,
    workspace_default_members: defaultWorkspaceMembers(root, local), version: 1,
    metadata: toml(root, 'Cargo.toml').workspace?.metadata ?? null, resolve: {nodes}};
}
