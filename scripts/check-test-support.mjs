import {workspaceGraph} from './workspace-model.mjs';
import {readFileSync} from 'node:fs';
import {evaluateCfg, readSources, trackedFiles} from './policy-core.mjs';

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

// Executable scripts and workflow steps. A data file that records how something
// was once run is provenance, not a command, and is out of scope by type.
export const COMMAND_FILE = /^(?:.+\.(?:sh|ps1|[cm]?js|ts|py)|\.github\/(?:workflows|actions)\/.+\.ya?ml)$/;

// Loom models compile only under ACYCLIC_LOOM=1 with the package's model-checking
// feature, through one build-script stanza (CONTRIBUTING.md). A package with loom
// code and a loom dependency but no stanza would silently run none of its models.
const LOOM_STANZA = ['println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");',
  'if std::env::var_os("ACYCLIC_LOOM").is_some() && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some() {',
  'println!("cargo:rustc-cfg=loom");', '}'].join(' ');
const LOOM_CFG = /#!?\[cfg(?:_attr)?\(([^\]]*)\]|\bcfg!\(([^;\n]*)/g;
// Literals and line comments, which may spell a cfg without being one.
const NOT_CODE = /\br(#*)"[\s\S]*?"\1|'(?:[^'\\\n]|\\.)'|"(?:[^"\\]|\\[\s\S])*"|\/\/[^\n]*/g;
// The packages whose model-checking feature links loom; checkLoomOptIn requires every
// package with loom code to be one of them.
export function loomModelPackages(metadata) {
  return metadata.packages.filter(pkg => metadata.workspace_members.includes(pkg.id)
    && pkg.features['model-checking']?.includes('dep:loom')).map(pkg => pkg.name);
}

export function checkLoomOptIn(metadata, sources, read = file => readFileSync(file, 'utf8')) {
  const slash = file => file.replaceAll('\\', '/');
  const root = slash(metadata.workspace_root);
  // Each source belongs to the innermost package directory that contains it.
  const owners = metadata.packages.map(pkg => [slash(pkg.manifest_path).replace(/\/[^/]*$/, ''), pkg])
    .filter(([dir]) => dir === root || dir.startsWith(`${root}/`))
    .map(([dir, pkg]) => [dir.slice(root.length + 1), pkg]).sort(([a], [b]) => b.length - a.length);
  const loom = new Set();
  for (const [file, text] of sources) {
    const owner = owners.find(([dir]) => dir === '' || slash(file).startsWith(`${dir}/`));
    if (owner && [...text.replace(NOT_CODE, '').matchAll(LOOM_CFG)].some(match => /\bloom\b/.test(match[1] ?? match[2])))
      loom.add(owner[1].id);
  }
  const failures = [];
  let checked = 0;
  for (const pkg of metadata.packages) {
    // Without a loom dependency, cfg(loom) branches compile only where another package includes the file.
    if (!metadata.workspace_members.includes(pkg.id) || !loom.has(pkg.id) ||
        !pkg.dependencies.some(dependency => dependency.name === 'loom')) continue;
    checked++;
    // Models link loom only through the feature, so a build without it never compiles loom.
    if (!pkg.dependencies.some(dependency => dependency.name === 'loom' && dependency.kind === null && dependency.optional))
      failures.push(`${pkg.name}: loom must be an optional normal dependency`);
    if (!pkg.features['model-checking']?.includes('dep:loom'))
      failures.push(`${pkg.name}: its model-checking feature must enable dep:loom`);
    const script = pkg.targets.find(target => target.kind.includes('custom-build'));
    const text = script ? read(script.src_path).replace(/\s+/g, ' ').replaceAll('cargo::', 'cargo:') : '';
    if (!text.includes(LOOM_STANZA))
      failures.push(`${pkg.name}: its build script must emit cfg(loom) under ACYCLIC_LOOM and model-checking (CONTRIBUTING.md)`);
  }
  if (failures.length) throw new Error(failures.join('\n'));
  return checked;
}

// A loom cfg passed by hand or appended by a compiler wrapper bypasses the build-script
// opt-in; cfg(loom) comes only from the stanza, in rustc argv where caches key on it.
const LOOM_CFG_FLAG = /--cfg(?:=|\s+|["']\s*,\s*["'])loom\b/;
export function checkNoLoomCfgFlag(sources) {
  const failures = [];
  for (const [file, text] of sources)
    text.split('\n').forEach((line, index) => {
      if (LOOM_CFG_FLAG.test(line)) failures.push(`${file}:${index + 1}: set ACYCLIC_LOOM=1 with model-checking, never pass the loom cfg by hand`);
    });
  if (failures.length) throw new Error(failures.join('\n'));
  return sources.length;
}

// Shipped binaries decode their requests with serde. A field or variant of a
// deserialized type that exists only under test-support is unreachable from
// any shipped-binary e2e, so it is refused wherever it is declared.
export function checkDeserializedTestSupport(sources) {
  const failures = [];
  for (const [file, text] of sources) {
    let attribute = '', derived = false, body = null, depth = 0;
    text.split('\n').forEach((line, index) => {
      const code = line.replace(/\/\/.*$/, '').trim();
      if (attribute || code.startsWith('#[')) {
        attribute += code;
        const count = character => [...attribute].filter(c => c === character).length;
        if (count('[') > count(']')) return;
        const complete = attribute;
        attribute = '';
        if (body !== null && depth >= 1 && /^#\[cfg(?:_attr)?\(.*test-support/.test(complete))
          failures.push(`${file}:${index + 1}: ${body} has a test-support-only member`);
        if (body === null && /^#\[derive\(.*\bDeserialize\b/.test(complete)) derived = true;
        return;
      }
      if (body === null) {
        const declared = derived && /\b(?:enum|struct)\s+(\w+)/.exec(code);
        derived = false;
        if (!declared) return;
        body = declared[1];
        depth = 0;
      }
      for (const c of code) depth += c === '{' ? 1 : c === '}' ? -1 : 0;
      if (depth <= 0 && /[};]/.test(code)) body = null;
    });
  }
  if (failures.length) throw new Error(failures.join('\n'));
  return sources.length;
}

// Evaluate only the two cfg inputs whose relationship this policy owns. Other
// features and platform predicates remain unknown, so an OS-specific export
// cannot hide a target that unconditionally requires test-support.
const testCfg = attributes => [...attributes.matchAll(/#\[\s*cfg\s*\((.*?)\)\s*\]/gs)]
  .some(match => evaluateCfg(match[1], (name, value) => name === 'test' && value === undefined ? true
    : name === 'feature' && value === 'test-support' ? false : undefined) === false) ? false : undefined;

function splitUse(text) {
  let depth = 0, start = 0;
  const parts = [];
  for (let i = 0; i < text.length; i++) {
    if ('({'.includes(text[i])) depth++;
    if (')}'.includes(text[i])) depth--;
    if (text[i] === ',' && depth === 0) {
      parts.push(text.slice(start, i));
      start = i + 1;
    }
  }
  return [...parts, text.slice(start)].filter(part => part.trim());
}

function usePaths(tree, prefix = '') {
  return splitUse(tree).flatMap(part => {
    const group = part.indexOf('{');
    if (group !== -1) return usePaths(part.slice(group + 1, part.lastIndexOf('}')), prefix + part.slice(0, group));
    return [(prefix + part).replace(/\s+as\s+\w+\s*$/, '').replace(/\s/g, '').replace(/::self$/, '')];
  });
}

// Index crate-relative declarations before checking exports, including every
// parent on a re-export's path. This deliberately checks source declarations,
// independent of Cargo's dev-dependency feature unification and the host OS.
export function checkTestSupportExports(sources) {
  const declarations = new Map(), exports = [];
  for (const [file, text] of sources) {
    const source = /^(.*\/src\/)(.*)\.rs$/.exec(file);
    if (!source) continue;
    const root = source[1];
    const module = source[2].replace(/(?:^|\/)(?:lib|main|mod)$/, '').replaceAll('/', '::');
    const scope = module ? `crate::${module}::` : 'crate::';
    const code = text.replace(/\/\/[^\n]*/g, '');
    const items = /((?:#\[[^\]]*\]\s*)*)(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(mod|fn|struct|enum|type|const|static|use)\s+([\s\S]*?)(?=[;{=(])/g;
    for (const match of code.matchAll(items)) {
      const [, attributes, kind, value] = match;
      if (kind === 'use') continue;
      const name = /^\w+/.exec(value)?.[0];
      if (!name) continue;
      declarations.set(root + scope + name, {attributes, file, name});
    }
    if (!module.split('::').includes('test_support')) continue;
    for (const match of code.matchAll(/((?:#\[[^\]]*\]\s*)*)pub(?:\([^)]*\))?\s+use\s+([^;]+);/g)) {
      if (testCfg(match[1]) === false) continue;
      for (let path of usePaths(match[2])) {
        let relative = scope;
        if (path.startsWith('self::')) path = scope + path.slice(6);
        while (path.startsWith('super::')) {
          relative = relative.replace(/[^:]+::$/, '');
          path = path.slice(7);
        }
        if (relative !== scope) path = relative + path;
        if (path.startsWith('crate::')) exports.push({root, scope, path, file});
      }
    }
  }
  const failures = new Set();
  for (const {root, scope, path, file} of exports) {
    const parents = value => value.split('::').map((_, i, parts) => root + parts.slice(0, i + 1).join('::'));
    if (parents(scope.replace(/::$/, '')).some(key => testCfg(declarations.get(key)?.attributes ?? '') === false)) continue;
    for (const key of parents(path)) {
      const target = declarations.get(key);
      if (target && testCfg(target.attributes) === false)
        failures.add(`${file}: ${path} re-exports ${target.file}::${target.name}, which requires test-support under cfg(test)`);
    }
  }
  if (failures.size) throw new Error([...failures].sort().join('\n'));
  return exports.length;
}

if (import.meta.main) {
  // Resolve every optional and target-specific edge, including registry crates.
  const metadata = workspaceGraph(process.cwd(), null);
  console.log(`test-support forwarding: ${checkTestSupport(metadata)} dependency declarations checked`);
  const sources = await readSources(process.cwd(), trackedFiles(process.cwd(), ['*.rs']));
  const commands = await readSources(process.cwd(), trackedFiles(process.cwd(), [':!scripts/tests/fixtures'])
    .filter(file => COMMAND_FILE.test(file)));
  console.log(`loom opt-in: ${checkLoomOptIn(metadata, sources)} model packages checked, ` +
    `${checkNoLoomCfgFlag(commands)} command files without a hand-passed loom cfg`);
  console.log(`test-support deserialized members: ${checkDeserializedTestSupport(sources)} Rust sources checked`);
  console.log(`test-support exports: ${checkTestSupportExports(sources)} local re-exports checked`);
}
