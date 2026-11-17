// Repository declarations and Bazel's committed crate resolution are the metadata
// authority. Reading them never resolves, downloads or builds a Cargo workspace.
import fs from 'node:fs';
import path from 'node:path';
import {toml, tomlFiles} from './policy-core.mjs';

const slash = value => value.replaceAll('\\', '/');
const exists = file => fs.existsSync(file);
const requirement = value => value === undefined ? '*' : /^\d/.test(value) ? `^${value}` : value;
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

export function workspacePackages(root) {
  root = path.resolve(root);
  const rootManifest = toml(root, 'Cargo.toml');
  const manifests = new Set();
  const expand = pattern => fs.globSync(pattern.replace(/\/$/, '') + '/Cargo.toml', {cwd: root});
  for (const member of rootManifest.workspace?.members ?? []) for (const file of expand(member)) manifests.add(slash(file));
  if (rootManifest.package) manifests.add('Cargo.toml');
  const members = new Set(manifests);
  for (const patch of Object.values(rootManifest.patch?.['crates-io'] ?? {}))
    if (patch.path) manifests.add(slash(path.join(patch.path, 'Cargo.toml')));
  const packages = [];
  const pending = [...manifests];
  tomlFiles(root, pending);
  while (pending.length) {
    const file = pending.shift(), manifest = toml(root, file), directory = path.dirname(path.resolve(root, file));
    if (!manifest.package) throw Error(`${file}: missing package`);
    let workspaceDirectory = directory;
    if (manifest.package.workspace) workspaceDirectory = path.resolve(directory, manifest.package.workspace);
    else while (workspaceDirectory !== root && !(exists(path.join(workspaceDirectory, 'Cargo.toml')) && toml(root, slash(path.relative(root, path.join(workspaceDirectory, 'Cargo.toml')))).workspace))
      workspaceDirectory = path.dirname(workspaceDirectory);
    const workspace = toml(root, slash(path.relative(root, path.join(workspaceDirectory, 'Cargo.toml')))).workspace ?? {};
    const own = Object.fromEntries(Object.entries(manifest.package).map(([key, value]) =>
      [key, value?.workspace === true ? workspace.package?.[key] : value]));
    const dependencies = [];
    const declarations = (tables, target = null) => {
      for (const [table, kind] of [['dependencies', null], ['dev-dependencies', 'dev'], ['build-dependencies', 'build']])
        for (const [alias, value] of Object.entries(tables[table] ?? {})) {
          let declaration = typeof value === 'string' ? {version: value} : value;
          let base = directory;
          if (declaration.workspace) {
            const inherited = workspace.dependencies?.[alias];
            if (!inherited) throw Error(`${file}: missing workspace dependency ${alias}`);
            const entry = typeof inherited === 'string' ? {version: inherited} : inherited;
            declaration = {...entry, ...declaration, features: [...(entry.features ?? []), ...(declaration.features ?? [])]};
            base = workspaceDirectory;
          }
          const patched = rootManifest.patch?.['crates-io']?.[declaration.package ?? alias];
          const local = declaration.path ? path.resolve(base, declaration.path) : patched?.path ? path.resolve(root, patched.path) : null;
          if (local) {
            const next = slash(path.relative(root, path.join(local, 'Cargo.toml')));
            if (!manifests.has(next)) {manifests.add(next); pending.push(next);}
          }
          dependencies.push({name: declaration.package ?? alias, rename: declaration.package ? alias : null,
            req: requirement(declaration.version), kind, target, optional: declaration.optional ?? false,
            uses_default_features: declaration['default-features'] ?? true, features: declaration.features ?? [],
            source: declaration.path ? null : 'registry+https://github.com/rust-lang/crates.io-index', ...(local ? {resolved_path: local} : {}), ...(declaration.path ? {path: local} : {})});
        }
    };
    declarations(manifest);
    for (const [target, tables] of Object.entries(manifest.target ?? {})) declarations(tables, target);
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
    packages.push(p);
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name, 'en'));
}

export function workspaceGraph(root, triple = 'x86_64-unknown-linux-gnu') {
  const allPlatforms = triple === null;
  triple ??= 'x86_64-unknown-linux-gnu';
  root = path.resolve(root);
  const local = workspacePackages(root), lock = toml(root, 'Cargo.lock');
  const universe = JSON.parse(fs.readFileSync(path.join(root, 'cargo-bazel-lock.json'), 'utf8'));
  const spliced = toml(root, 'cargo-bazel.Cargo.lock').package.find(p => p.name === 'direct-cargo-bazel-deps');
  const direct = new Set(spliced?.dependencies ?? []);
  const registry = (lock.package ?? []).filter(p => p.source && (allPlatforms || direct.has(p.name) || direct.has(`${p.name} ${p.version}`))).map(p => {
    const pinned = universe.crates[`${p.name} ${p.version}`];
    const lib = pinned?.targets.find(t => t.Library || t.ProcMacro);
    const target = lib?.Library ?? lib?.ProcMacro;
    return {...p, id: `${p.source}#${p.name}@${p.version}`, manifest_path: '', dependencies: [], metadata: null,
      features: Object.fromEntries((pinned?.common_attrs?.crate_features ?? []).map(f => [f, []])),
      targets: target ? [{kind: [lib.ProcMacro ? 'proc-macro' : 'lib'], name: target.crate_name}] : []};
  });
  const packages = [...local, ...registry], byName = name => packages.filter(p => p.name === name);
  const platform = new Set([
    triple.includes('windows') ? 'windows' : 'unix', `target_os=${JSON.stringify(triple.includes('windows') ? 'windows' : triple.includes('darwin') ? 'macos' : 'linux')}`,
    `target_arch=${JSON.stringify(triple.startsWith('aarch64') ? 'aarch64' : 'x86_64')}`, 'target_pointer_width="64"', 'target_endian="little"',
    `target_env=${JSON.stringify(triple.includes('msvc') ? 'msvc' : triple.includes('darwin') ? '' : 'gnu')}`, `target_family=${JSON.stringify(triple.includes('windows') ? 'windows' : 'unix')}`,
    `target_vendor=${JSON.stringify(triple.includes('darwin') ? 'apple' : triple.includes('windows') ? 'pc' : 'unknown')}`,
  ]);
  const matches = cfg => {
    if (allPlatforms) return true;
    if (!cfg || cfg === triple) return true;
    if (!cfg.startsWith('cfg(')) return false;
    const tokens = cfg.slice(4, -1).match(/"[^"\\]*(?:\\.[^"\\]*)*"|\w+|[(),=]/g) ?? [];
    let at = 0;
    const parse = () => {
      const name = tokens[at++];
      if (tokens[at] === '=') {at++; return platform.has(`${name}=${tokens[at++]}`);}
      if (tokens[at] !== '(') return platform.has(name);
      at++;
      const values = [];
      while (tokens[at] !== ')' && at < tokens.length) {values.push(parse()); if (tokens[at] === ',') at++;}
      if (tokens[at++] !== ')') throw Error(`invalid target condition ${cfg}`);
      if (name === 'all') return values.every(Boolean);
      if (name === 'any') return values.some(Boolean);
      if (name === 'not' && values.length === 1) return !values[0];
      throw Error(`unknown target condition ${cfg}`);
    };
    const result = parse();
    if (at !== tokens.length) throw Error(`invalid target condition ${cfg}`);
    return result;
  };
  const enabled = new Map(local.map(p => [p.id, new Set(p.workspace_member ? Object.keys(p.features) : universe.crates[`${p.name} ${p.version}`]?.common_attrs?.crate_features ?? [])]));
  const active = (p, d) => !d.optional || [...enabled.get(p.id)].some(f => f === `dep:${d.rename ?? d.name}` || f.startsWith(`${d.rename ?? d.name}/`));
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
        for (const feature of features) {
          const match = /^([^/?]+)\??\/(.+)$/.exec(feature);
          if (match?.[1] === alias) add(target, match[2]);
        }
      }
    }
  }
  const nodes = local.map(p => {
    const locked = lock.package.find(x => x.name === p.name && x.version === p.version && !x.source);
    if (!locked) throw Error(`Cargo.lock lacks local package ${p.name}@${p.version}`);
    const deps = new Map();
    for (const d of p.dependencies.filter(d => (d.kind !== 'dev' || p.workspace_member) && matches(d.target) && active(p, d))) {
      const candidates = d.resolved_path ? local.filter(x => path.dirname(x.manifest_path) === d.resolved_path) : byName(d.name).filter(x => versionMatches(x.version, d.req) &&
        (locked.dependencies ?? []).some(key => key === x.name || key === `${x.name} ${x.version}` || key.startsWith(`${x.name} ${x.version} (`)));
      if (candidates.length !== 1) throw Error(`${p.name}: ambiguous or missing locked dependency ${d.name}`);
      const dependency = candidates[0], name = (d.rename ?? d.name).replaceAll('-', '_');
      const key = `${name}:${dependency.id}`;
      if (!deps.has(key)) deps.set(key, {name, pkg: dependency.id, dep_kinds: []});
      deps.get(key).dep_kinds.push({kind: d.kind, target: d.target});
    }
    return {id: p.id, deps: [...deps.values()], features: Object.keys(p.features).filter(f => enabled.get(p.id).has(f)).sort()};
  });
  const selected = value => Array.isArray(value) ? value : [...(value?.common ?? []), ...Object.entries(value?.selects ?? {}).flatMap(([cfg, entries]) => allPlatforms || (universe.conditions[cfg] ?? [cfg]).includes(triple) ? entries : [])];
  nodes.push(...registry.map(p => {
    const pinned = universe.crates[`${p.name} ${p.version}`];
    const deps = [];
    for (const [attributes, kind] of [[pinned?.common_attrs, null], [pinned?.build_script_attrs, 'build']])
      for (const entry of [...selected(attributes?.deps), ...selected(attributes?.proc_macro_deps)]) {
        // Registry-internal edges remain Bazel's graph. Only overlays on local
        // patched crates enter this declaration model.
        const dependency = local.find(x => `${x.name} ${x.version}` === entry.id);
        if (!dependency) continue;
        deps.push({name: entry.alias ?? entry.target, pkg: dependency.id, dep_kinds: [{kind, target: null}]});
      }
    return {id: p.id, deps, features: Object.keys(p.features)};
  }));
  return {packages, workspace_members: local.filter(p => p.workspace_member).map(p => p.id), workspace_root: root,
    workspace_default_members: local.filter(p => p.workspace_member && (toml(root, 'Cargo.toml').workspace?.['default-members'] ?? local.filter(p => p.workspace_member).map(p => slash(path.relative(root,path.dirname(p.manifest_path))))).some(pattern=>path.matchesGlob(slash(path.relative(root,path.dirname(p.manifest_path))),pattern))).map(p=>p.id), version: 1,
    metadata: toml(root, 'Cargo.toml').workspace?.metadata ?? null, resolve: {nodes}};
}
