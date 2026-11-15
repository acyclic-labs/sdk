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

// A test target that gates modules on its own test-support feature would
// silently run a fraction of itself without the feature. Its native build
// must refuse to compile instead, so no runner can drop those tests unseen.
const GUARD = /#\[cfg\(all\(not\(loom\), not\(target_arch = "wasm32"\), not\(feature = "test-support"\)\)\)\]\s*compile_error!\(/;
export function checkGatedTestRoots(metadata, read = file => readFileSync(file, 'utf8')) {
  const failures = [];
  let checked = 0;
  for (const pkg of metadata.packages) {
    if (!metadata.workspace_members.includes(pkg.id)) continue;
    for (const target of pkg.targets) {
      if (!target.kind.includes('test')) continue;
      const text = read(target.src_path);
      checked++;
      if (/feature\s*=\s*"test-support"/.test(text.replace(GUARD, '')) && !GUARD.test(text))
        failures.push(`${pkg.name} test ${target.name}: gates on test-support but compiles without it`);
    }
  }
  if (failures.length) throw new Error(failures.join('\n'));
  return checked;
}

const COMMAND_FILE = /\.(?:sh|ps1|m?js|cjs|ts|py|ya?ml|json)$/;

// A selection that matches no tests must fail, never pass.
const ZERO_PASS = new RegExp(['--no-tests[= ]+(?:pass|warn)\\b', 'NEXTEST_' + 'NO_TESTS'].join('|'));
export function checkTestSelection(sources) {
  const failures = [];
  for (const [file, text] of sources)
    text.split('\n').forEach((line, index) => {
      if (ZERO_PASS.test(line)) failures.push(`${file}:${index + 1}: a selection matching no tests must fail`);
    });
  if (failures.length) throw new Error(failures.join('\n'));
  return sources.length;
}

// libtest exits 0 when a name filter matches nothing, so a filtered selection
// runs through nextest, whose empty selection fails. Doc tests and builds
// that run nothing (`--no-run`) select no named test and stay with cargo.
const CARGO_VALUE = new Set(['-p', '--package', '-F', '--features', '--test', '--bin', '--example', '--bench',
  '--target', '--target-dir', '--manifest-path', '--profile', '-j', '--jobs', '--message-format', '--color',
  '--config', '-Z', '--exclude']);
// A target nextest cannot run keeps cargo's runner; each entry names how its runner refuses an empty selection.
export const LIBTEST_TARGETS = new Map([
  ['wasm32-unknown-unknown', 'nextest cannot run wasm-bindgen browser tests; scripts/run-browser-tests.sh fails unless the runner reports a nonzero passed count'],
]);
const LIBTEST_VALUE = new Set(['--test-threads', '--skip', '--format', '-Z', '--shuffle-seed', '--logfile', '--color']);
export function cargoTestFilters(text) {
  const commands = [];
  // Join shell and PowerShell continuations, keeping each command's first line number.
  let start = 0, logical = '';
  const lines = [];
  text.split('\n').forEach((physical, index) => {
    if (!logical) start = index + 1;
    const continued = /[\\`]\r?$/.test(physical);
    logical += (continued ? physical.replace(/[\\`]\r?$/, '') : physical) + ' ';
    if (!continued) { lines.push([start, logical]); logical = ''; }
  });
  if (logical) lines.push([start, logical]);
  for (const [line, code] of lines) {
    if (/^\s*(?:#|\/\/|\/\*|\*)/.test(code)) continue;
    // Each cargo test invocation, up to its shell boundary or redirection.
    // A quoted expansion is one shell word; any other quote ends the command's text.
    const shell = code.replace(/"(\$[\w@#{}[\]]+)"/g, '$1');
    for (const match of shell.matchAll(/(?:\bcargo(?:\.exe)?|\$\{?cargo_bin\}?)(?:\s+\+\S+)?\s+test\b([^|;&>()"'`]*)/g))
      commands.push({line, tokens: match[1].replace(/\s\d+$/, '').trim().split(/\s+/).filter(token => token && !token.startsWith('\\'))});
    // Argument arrays handed to cargo from JavaScript and Python.
    // An array is a cargo invocation when it names cargo or a cargo selection option.
    for (const match of code.matchAll(/\[\s*(?:(["'])cargo\1\s*,\s*)?(["'])test\2\s*,([^\]]*)\]/g)) {
      // A spread forwards options; a bare identifier is a computed argument.
      const tokens = [...match[3].matchAll(/(["'])(.*?)\1|(?:\.\.\.|\*)\w+|[A-Za-z_][\w.]*/g)]
        .flatMap(token => token[2] ?? (/^(?:\.\.\.|\*)/.test(token[0]) ? [] : ['$' + token[0]]));
      if (match[1] || tokens.some(token => ['--locked', '-p', '--package', '--lib', '--test'].includes(token)))
        commands.push({line, tokens});
    }
  }
  return commands.filter(({tokens}) => {
    if (tokens.includes('--no-run') || tokens.includes('--doc')) return false;
    const target = tokens.flatMap((token, i) => token === '--target' ? [tokens[i + 1]]
      : token.startsWith('--target=') ? [token.slice('--target='.length)] : []);
    if (target.some(triple => LIBTEST_TARGETS.has(triple))) return false;
    let libtest = false;
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i].replace(/^["']|["']$/g, '');
      if (token === '--') { libtest = true; continue; }
      if ((libtest ? LIBTEST_VALUE : CARGO_VALUE).has(token)) { i++; continue; }
      if (!token.startsWith('-')) return true;
    }
    return false;
  }).map(({line}) => line);
}

// Each exception names why libtest is required and how the file refuses an empty selection.
export const LIBTEST_SELECTIONS = new Map([
  ['scripts/check-harness-test-isolation.sh', 'one libtest process exposes process-local fixture leakage that per-test nextest processes hide; the script fails unless its selection ran tests'],
]);
export function checkCargoTestFilters(sources, allowed = LIBTEST_SELECTIONS) {
  const failures = [];
  for (const [file, text] of sources)
    for (const line of allowed.has(file) ? [] : cargoTestFilters(text))
      failures.push(`${file}:${line}: a name filter under libtest passes when nothing matches; select it with nextest`);
  if (failures.length) throw new Error(failures.join('\n'));
  return sources.length;
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
  console.log(`test-support gated test roots: ${checkGatedTestRoots(JSON.parse(output))} test targets checked`);
  const tracked = execFileSync('git', ['ls-files', '--', ':!scripts/tests/fixtures'], {encoding: 'utf8'}).split('\n')
    .filter(file => /\.(?:sh|ps1|m?js|ts|py|ya?ml|json|toml|md|bazel|bzl|rs)$/.test(file));
  const trackedSources = tracked.map(file => [file, readFileSync(file, 'utf8')]);
  console.log(`test selection: ${checkTestSelection(trackedSources)} files checked`);
  console.log(`filtered cargo tests: ${checkCargoTestFilters(trackedSources.filter(([file]) => COMMAND_FILE.test(file)))} command files checked`);
}
