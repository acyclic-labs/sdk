import {execFileSync, spawn} from 'node:child_process';
import {readFileSync} from 'node:fs';
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
function testCfg(attributes) {
  const evaluate = expression => {
    const call = /^(all|any|not)\((.*)\)$/s.exec(expression.trim());
    if (call) {
      const values = splitUse(call[2]).map(evaluate);
      if (call[1] === 'not') return values[0] === undefined ? undefined : !values[0];
      const decisive = call[1] === 'any';
      return values.includes(decisive) ? decisive : values.includes(undefined) ? undefined : !decisive;
    }
    if (expression.trim() === 'test') return true;
    if (/^feature\s*=\s*"test-support"$/.test(expression.trim())) return false;
    return undefined;
  };
  return [...attributes.matchAll(/#\[\s*cfg\s*\((.*?)\)\s*\]/gs)]
    .some(match => evaluate(match[1]) === false) ? false : undefined;
}

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
  const files = execFileSync('git', ['ls-files', '--', '*.rs'], {encoding: 'utf8'}).split('\n').filter(Boolean);
  const sources = files.map(file => [file, readFileSync(file, 'utf8')]);
  console.log(`test-support deserialized members: ${checkDeserializedTestSupport(sources)} Rust sources checked`);
  console.log(`test-support exports: ${checkTestSupportExports(sources)} local re-exports checked`);
}
