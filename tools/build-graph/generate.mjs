// Generated BUILD files; Cargo metadata remains authoritative.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
const [metadataFile, destination, configurationFile] = process.argv.slice(2);
if (!metadataFile || !destination || !configurationFile) throw Error('usage: node generate.mjs METADATA_JSON SDK_ROOT CONFIG_JSON');
const metadata = JSON.parse(fs.readFileSync(metadataFile, 'utf8'));
const root = path.resolve(destination);
const configuration = JSON.parse(fs.readFileSync(configurationFile, 'utf8'));
if (configuration.version !== 1 || !Array.isArray(configuration.roots) || !configuration.roots.length) throw Error('Expected version 1 configuration with nonempty roots');
const images = configuration.images;
const testCases = configuration.testCases ?? [];
if (new Set(testCases.map(t => t.name)).size !== testCases.length || testCases.some(t => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(t.name) || typeof t.binary !== 'string' || typeof t.case !== 'string')) throw Error('Invalid test case configuration');
if (images.version !== 1 || ![images.compile, images.test].every(x => /^sha256:[0-9a-f]{64}$/.test(x))) throw Error('Invalid execution image policy');
function write(file, bytes) {
  if (!fs.existsSync(file) || fs.readFileSync(file, 'utf8') !== bytes) fs.writeFileSync(file, bytes);
}

// Cargo metadata omits profiles; derive the supported scalar dev fields.
const profiles = new Map(); let profile = null;
const profileKeys = new Set(['opt-level', 'lto', 'codegen-units', 'debug', 'debug-assertions', 'overflow-checks']);
for (const raw of fs.readFileSync(path.join(root, 'Cargo.toml'), 'utf8').split('\n')) {
  const line = raw.trim();
  if (line.startsWith('[')) {
    const table = line.slice(1, -1);
    profile = table === 'profile.dev' ? '' : table === 'profile.dev.build-override' ? 'build-override' : table.startsWith('profile.dev.package.') ? 'package.' + table.slice(20) : null;
    if (profile !== null) {
      if (!line.endsWith(']')) throw Error('Unsupported Cargo profile header: ' + line);
      if (!profiles.has(profile)) profiles.set(profile, {});
    }
    continue;
  }
  if (profile === null || !line || line.startsWith('#')) continue;
  const equal = line.indexOf('='), key = line.slice(0, equal).trim();
  if (equal < 0 || !profileKeys.has(key)) throw Error('Unsupported Cargo dev profile field: ' + line);
  profiles.get(profile)[key] = JSON.parse(line.slice(equal + 1).split('#')[0].trim());
}
function profileFlags(values, build = false) {
  const flags = {'opt-level': 0, 'lto': 'off', 'codegen-units': 256, 'debug': build ? false : 2, 'debug-assertions': true, 'overflow-checks': true, ...values};
  return Object.entries(flags).map(([key, value]) => {
    if (key === 'debug') return '-Cdebuginfo=' + (value === false ? 0 : value === true ? 2 : value);
    if (key === 'lto' && value === false) throw Error('Cargo local ThinLTO requires explicit policy');
    return '-C' + key + '=' + (key === 'lto' && value === true ? 'fat' : value);
  });
}
let profileRc = '# Generated from Cargo.toml dev profiles. DO NOT EDIT.\n';
for (const [setting, values] of [['extra_rustc_flag', profiles.get('')], ['extra_exec_rustc_flag', profiles.get('build-override')]]) {
  for (const flag of profileFlags(values, setting === 'extra_exec_rustc_flag')) profileRc += 'build --@rules_rust//rust/settings:' + setting + '=' + flag + '\n';
}
for (const [table, values] of profiles) {
  if (!table.startsWith('package.')) continue;
  let name = table.slice(8); if (name.startsWith('"')) name = JSON.parse(name);
  if (!/^[A-Za-z0-9_-]+$/.test(name)) throw Error('Unsupported Cargo package profile selector: ' + name);
  const p = metadata.packages.find(p => p.name === name && p.source === null);
  const prefix = p ? '//' + path.dirname(path.relative(metadata.workspace_root, p.manifest_path)).split(path.sep).join('/') + ':' : 'external/rules_rust++crate+crates__' + name + '-';
  for (const [key, value] of Object.entries(values)) {
    const flag = profileFlags({...profiles.get(''), [key]: value}).find(flag => flag.startsWith('-C' + (key === 'debug' ? 'debuginfo' : key) + '='));
    profileRc += 'build --@rules_rust//rust/settings:per_crate_rustc_flag=' + prefix + '@' + flag + '\n';
  }
}
write(path.join(root, 'Cargo.Bazel.rc'), profileRc);

const packages = new Map(metadata.packages.filter(p => p.source === null).map(p => [p.name, p]));
const roots = [...new Set(configuration.roots)];
for (const name of roots) if (!packages.has(name)) throw Error('Unknown workspace package: ' + name);
const active = new Map();
function activate(name, features = [], defaults = true) {
  if (!packages.has(name)) return false;
  let changed = false;
  if (!active.has(name)) { active.set(name, new Set()); changed = true; }
  const set = active.get(name);
  for (const f of [...features, ...(defaults && packages.get(name).features.default ? ['default'] : [])]) {
    if (!set.has(f)) { set.add(f); changed = true; }
  }
  return changed;
}
for (const name of roots) activate(name);
let changed;
do {
  changed = false;
  for (const [name, features] of active) {
    const p = packages.get(name);
    for (const f of [...features]) for (const value of p.features[f] ?? []) {
      if (!features.has(value)) { features.add(value); changed = true; }
    }
    for (const d of p.dependencies) {
      if (!matches(d.target, 'linux') && !matches(d.target, 'windows')) continue;
      if (d.kind === 'dev' && !roots.includes(name)) continue;
      const alias = d.rename ?? d.name;
      const optional = features.has(alias) || features.has(`dep:${alias}`) || [...features].some(f => f.startsWith(`${alias}/`));
      if (d.optional && !optional) continue;
      const forwarded = [...features].filter(f => f.startsWith(`${alias}/`) || f.startsWith(`${alias}?/`)).map(f => f.slice(f.indexOf('/') + 1));
      changed = activate(d.name, [...d.features, ...forwarded], d.uses_default_features) || changed;
    }
  }
} while (changed);
const q = JSON.stringify;
const list = xs => '[' + xs.map(q).join(', ') + ']';
const externalSpecs = new Map();
const resolvedPackages = new Map(metadata.packages.map(p => [p.id, p]));
const resolvedNodes = new Map((metadata.resolve?.nodes ?? []).map(n => [n.id, n]));
// Cargo metadata resolves registry patches to source:null packages, including
// transitive patches. Preserve those choices in crate_universe's Cargo config.
const relative = file => path.relative(metadata.workspace_root, file).replaceAll('\\', '/');
// Declare literal module/include paths and runtime source_path fixtures per
// root, rather than attaching every SDK corpus to every test.
const packageDirectories = [...active.keys()].map(name => path.posix.dirname(relative(packages.get(name).manifest_path)));
const sourceLabels = new Map(roots.map(name => [name, new Set()]));
const fixturePatterns = new Map(roots.map(name => {
  const directory = path.join(root, path.posix.dirname(relative(packages.get(name).manifest_path)));
  const patterns = new Set();
  function input(absolute, modules = false) {
    if (!absolute.startsWith(root + path.sep)) throw Error('Test input lies outside the SDK: ' + absolute);
    if (absolute.startsWith(directory + path.sep) || absolute === directory) return;
    const imported = path.relative(root, absolute).replaceAll('\\', '/');
    const owner = packageDirectories.find(directory => imported.startsWith(directory + '/'));
    if (owner) {
      const files = fs.statSync(absolute).isDirectory()
        ? fs.readdirSync(absolute, {recursive: true, withFileTypes: true}).filter(file => file.isFile() && (!modules || file.name.endsWith('.rs'))).map(file => path.join(file.parentPath, file.name))
        : [absolute];
      for (const file of files) sourceLabels.get(name).add('//' + owner + ':' + path.relative(path.join(root, owner), file).replaceAll('\\', '/'));
    } else patterns.add(imported + (fs.statSync(absolute).isDirectory() ? modules ? '/**/*.rs' : '/**' : ''));
  }
  for (const file of fs.readdirSync(directory, {recursive: true, withFileTypes: true})) {
    if (!file.isFile() || !file.name.endsWith('.rs')) continue;
    const source = fs.readFileSync(path.join(file.parentPath, file.name), 'utf8');
    for (const match of source.matchAll(/#\[path\s*=\s*"([^"]+)"\]/g)) {
      const absolute = path.resolve(file.parentPath, match[1]);
      input(path.basename(absolute) === 'mod.rs' ? path.dirname(absolute) : absolute, path.basename(absolute) === 'mod.rs');
    }
    for (const match of source.matchAll(/\binclude(?:_str|_bytes)?!\(\s*(?:concat!\(\s*)?"([^"]+)"/g)) {
      input(path.resolve(file.parentPath, match[1]));
    }
    for (const match of source.matchAll(/\bsource_path\(\s*"([^"]+)"\s*\)((?:\s*\.join\(\s*"[^"]+"\s*\))*)/g)) {
      let fixture = match[1];
      for (const suffix of match[2].matchAll(/\.join\(\s*"([^"]+)"\s*\)/g)) fixture = path.posix.join(fixture, suffix[1]);
      input(path.resolve(root, fixture));
    }
  }
  return [name, [...patterns].sort()];
}));
const fixtureName = name => name.replaceAll('-', '_') + '_test_inputs';
const registryPatches = new Map();
for (const node of resolvedNodes.values()) {
  const parent = resolvedPackages.get(node.id);
  for (const edge of node.deps) {
    const selected = resolvedPackages.get(edge.pkg);
    if (selected.source !== null) continue;
    for (const dependency of parent.dependencies.filter(d => (d.rename ?? d.name).replaceAll('-', '_') === edge.name && d.source?.startsWith('registry+'))) {
      if (dependency.source !== 'registry+https://github.com/rust-lang/crates.io-index') throw Error('Custom registry patch requires explicit configuration');
      if (registryPatches.has(dependency.name) && registryPatches.get(dependency.name).id !== selected.id) throw Error('Conflicting local registry patches');
      registryPatches.set(dependency.name, selected);
    }
  }
}
const cargoConfig = '[patch.crates-io]\n' + [...registryPatches].sort().map(([name, p]) => q(name) + ' = { path = ' + q(path.resolve(root, path.posix.dirname(relative(p.manifest_path))).replaceAll('\\', '/')) + ' }\n').join('');
write(path.join(root, 'Cargo.Bazel.config.toml'), cargoConfig);

function resolveEdge(p, d) {
  if (packages.has(d.name)) return d;
  const name = (d.rename ?? d.name).replaceAll('-', '_');
  const candidates = resolvedNodes.get(p.id)?.deps.filter(e => e.name === name && e.dep_kinds.some(k => k.kind === d.kind && k.target === d.target)) ?? [];
  const ids = [...new Set(candidates.map(e => e.pkg))];
  if (ids.length !== 1) throw Error(`Expected one locked dependency for ${p.name}/${name}, got ${ids.length}`);
  return {...d, resolved: resolvedPackages.get(ids[0])};
}
function externalKey(d) {
  const key = d.resolved.id;
  return d.name.replaceAll('-', '_') + '__' + createHash('sha256').update(key).digest('hex').slice(0, 12);
}
function procMacro(d) {
  if (packages.has(d.name)) return packages.get(d.name).targets.some(t => t.kind.includes('proc-macro'));
  const candidates = d.resolved ? [d.resolved] : [];
  if (!candidates.length) throw Error(`Full cargo metadata required: missing dependency ${d.name}`);
  const kinds = new Set(candidates.map(p => p.targets.some(t => t.kind.includes('proc-macro'))));
  if (kinds.size !== 1) throw Error(`Dependency versions disagree on proc-macro kind: ${d.name}`);
  return [...kinds][0];
}
function label(p) { return `//${path.posix.dirname(relative(p.manifest_path))}:lib`; }
// Evaluate Cargo cfg expressions exactly for the supported target triples.
function matches(expression, os) {
  if (!expression) return true;
  const triple = os === 'linux' ? 'x86_64-unknown-linux-gnu' : 'x86_64-pc-windows-gnu';
  if (!expression.startsWith('cfg(')) return expression === triple;
  const tokens = expression.match(/"[^"]*"|[A-Za-z_][A-Za-z_0-9]*|[(),=]/g);
  let index = 2;
  const attrs = {target_os: os, target_arch: 'x86_64', target_env: 'gnu', target_family: os === 'linux' ? 'unix' : 'windows', target_vendor: os === 'linux' ? 'unknown' : 'pc', target_pointer_width: '64', target_endian: 'little'};
  function parse() {
    const key = tokens[index++];
    if (tokens[index] === '(') {
      index++;
      const args = [];
      while (tokens[index] !== ')') { args.push(parse()); if (tokens[index] === ',') index++; }
      index++;
      if (key === 'all') return args.every(Boolean);
      if (key === 'any') return args.some(Boolean);
      if (key === 'not' && args.length === 1) return !args[0];
      throw Error(`Unsupported cfg operator: ${expression}`);
    }
    if (tokens[index] === '=') {
      index++;
      const value = JSON.parse(tokens[index++]);
      if (!Object.hasOwn(attrs, key)) throw Error(`Unsupported cfg attribute: ${expression}`);
      return attrs[key] === value;
    }
    if (key === 'unix') return os === 'linux';
    if (key === 'windows') return os === 'windows';
    throw Error(`Unsupported cfg flag: ${expression}`);
  }
  const result = parse();
  if (tokens[index] !== ')' || index !== tokens.length - 1) throw Error(`Unparsed cfg: ${expression}`);
  return result;
}
for (const [name, enabled] of [...active].sort()) {
  const p = packages.get(name), directory = path.posix.dirname(relative(p.manifest_path));
  const library = p.targets.find(t => t.kind.some(k => ['lib', 'rlib', 'cdylib', 'staticlib', 'dylib', 'proc-macro'].includes(k)));
  if (!library) throw Error(`No library target for ${name}`);
  const edges = kind => p.dependencies.filter(d => (d.kind ?? 'normal') === kind && (!packages.has(d.name) || active.has(d.name)) && (matches(d.target, 'linux') || matches(d.target, 'windows')) && (!d.optional || enabled.has(d.rename ?? d.name) || enabled.has(`dep:${d.rename ?? d.name}`) || [...enabled].some(f => f.startsWith(`${d.rename ?? d.name}/`)))).map(d => resolveEdge(p, d));
  function edgeLabel(d) {
    if (packages.has(d.name)) return label(packages.get(d.name));
    const key = externalKey(d);
    if (!externalSpecs.has(key)) externalSpecs.set(key, {dependency: d, defaults: false, features: new Set()});
    const spec = externalSpecs.get(key);
    spec.defaults ||= d.uses_default_features;
    const alias = d.rename ?? d.name;
    for (const f of [...d.features, ...[...enabled].filter(f => f.startsWith(`${alias}/`) || f.startsWith(`${alias}?/`)).map(f => f.slice(f.indexOf('/') + 1))]) spec.features.add(f);
    return `@crates//:${key}`;
  }
  const deps = (kind, proc = false) => {
    const selected = (Array.isArray(kind) ? kind : [kind]).flatMap(edges).filter(d => procMacro(d) === proc);
    const values = os => [...new Set(selected.filter(d => matches(d.target, os)).map(edgeLabel))];
    return `select({"//:linux": ${list(values('linux'))}, "//:windows": ${list(values('windows'))}})`;
  };
  const aliases = kinds => '{' + kinds.flatMap(kind => edges(kind)).filter(d => d.rename && (matches(d.target, 'linux') || matches(d.target, 'windows'))).map(d => `${q(edgeLabel(d))}: ${q(d.rename.replaceAll('-', '_'))}`).join(', ') + '}';
  const featureList = list([...enabled].filter(f => Object.hasOwn(p.features, f)).sort());
  const build = p.targets.find(t => t.kind.includes('custom-build'));
  const src = t => path.posix.relative(directory, relative(t.src_path));
  const rules = ['rust_library', 'rust_proc_macro', 'rust_test', 'rust_binary'];
  let out = '# Generated by tools/build-graph/generate.mjs. DO NOT EDIT.\n';
  out += 'load("@rules_rust//rust:defs.bzl", ' + rules.map(q).join(', ') + ')\n';
  out += 'load("@rules_rust//cargo:defs.bzl", "cargo_build_script")\n';

  out += 'package(default_visibility = ["//visibility:public"])\nexports_files(glob(["**"], exclude = ["BUILD.bazel"]))\n';
  if (build) out += `cargo_build_script(\n    name = "build_script",\n    aliases = ${aliases(['build'])},\n    crate_name = ${q(library.name)},\n    crate_root = ${q(src(build))},\n    srcs = glob(["**/*.rs"], exclude = ["tests/**"]),\n    data = glob(["**"], exclude = ["BUILD.bazel"]),\n    edition = ${q(p.edition)},\n    version = ${q(p.version)},\n    crate_features = ${featureList},\n    deps = ${deps('build')},\n    proc_macro_deps = ${deps('build', true)},\n)\n`;
  out += `${library.kind.includes('proc-macro') ? 'rust_proc_macro' : 'rust_library'}(\n    name = "lib",\n    aliases = ${aliases(['normal'])},\n    crate_name = ${q(library.name)},\n    crate_root = ${q(src(library))},\n    srcs = glob(["src/**/*.rs"]),\n    compile_data = glob(["**"], exclude = ["BUILD.bazel"]),\n    data = glob(["proto/**"], allow_empty = True),\n    edition = ${q(p.edition)},\n    version = ${q(p.version)},\n    crate_features = ${featureList},\n    deps = ${deps('normal')}${build ? ' + [":build_script"]' : ''},\n    proc_macro_deps = ${deps('normal', true)},\n)\n`;
  if (roots.includes(name)) {
    if (library.test) out += `rust_test(name = "unit_test", aliases = ${aliases(['dev'])}, crate = ":lib", compile_data = glob(["**"], exclude = ["BUILD.bazel"]) + [${q("//:" + fixtureName(name))}], data = glob(["**"], exclude = ["BUILD.bazel"]) + [${q("//:" + fixtureName(name))}], deps = ${deps('dev')}, proc_macro_deps = ${deps('dev', true)})\n`;
    for (const t of p.targets.filter(t => t.kind.includes('test'))) {
      if ((t['required-features'] ?? []).some(f => !enabled.has(f))) continue;
      out += `rust_test(name = ${q(t.name + '_test')}, crate_name = ${q(t.name.replaceAll('-', '_'))}, aliases = ${aliases(['normal', 'dev'])}, crate_root = ${q(src(t))}, srcs = glob(["tests/**/*.rs"]), edition = ${q(p.edition)}, crate_features = ${featureList}, deps = [":lib"] + ${deps(['normal', 'dev'])}, proc_macro_deps = ${deps(['normal', 'dev'], true)}, compile_data = glob(["**"], exclude = ["BUILD.bazel"]) + [${q("//:" + fixtureName(name))}], data = glob(["**"], exclude = ["BUILD.bazel"]) + [${q("//:" + fixtureName(name))}])\n`;
    }
  }
  write(path.join(root, directory, 'BUILD.bazel'), out);
}
const selectedTests = roots.flatMap(n => {const p=packages.get(n);return p.targets.filter(t => t.kind.includes('test') && (t['required-features'] ?? []).every(f => active.get(n).has(f))).map(t => label(p).replace(/:lib$/, ':' + t.name + '_test')).concat(p.targets.some(t => t.test && t.kind.some(k => ['lib', 'rlib', 'cdylib', 'staticlib', 'dylib'].includes(k))) ? [label(p).replace(/:lib$/, ':unit_test')] : []);});
write(path.join(root, 'BUILD.bazel'), '# Generated.\nload("//:tools/build-graph/cc_toolchain.bzl", "image_cc_toolchain", "image_execution_platform")\nload("//:tools/build-graph/test_case.bzl", "binary_case_test")\npackage(default_visibility = ["//visibility:public"])\nconfig_setting(name = "linux", constraint_values = ["@platforms//os:linux"])\nconfig_setting(name = "windows", constraint_values = ["@platforms//os:windows"])\nconfig_setting(name = "x86_64-unknown-linux-gnu", constraint_values = ["@platforms//os:linux", "@platforms//cpu:x86_64"])\nconfig_setting(name = "x86_64-pc-windows-gnu", constraint_values = ["@platforms//os:windows", "@platforms//cpu:x86_64"])\nimage_execution_platform(name = "linux_x86_64", compile_image = ' + q(images.compile) + ', test_image = ' + q(images.test) + ')\nplatform(name = "windows_gnu", constraint_values = ["@platforms//os:windows", "@platforms//cpu:x86_64"])\nexports_files(["Cargo.toml", "Cargo.lock", "Cargo.Bazel.lock", "Cargo.Bazel.config.toml", "tools/build-graph/cc_toolchain.bzl", "tools/build-graph/test_case.bzl", "tools/build-graph/rules_swift-host.patch"])\n' + roots.map(name => 'filegroup(name = ' + q(fixtureName(name)) + ', srcs = glob(' + list(fixturePatterns.get(name)) + ', allow_empty = True) + ' + list([...sourceLabels.get(name)].sort()) + ')\n').join('') + 'image_cc_toolchain(name = "linux_cc")\nimage_cc_toolchain(name = "windows_cc", windows = True, compiler = ' + q(configuration.windowsCompiler ?? "/usr/bin/x86_64-w64-mingw32-gcc") + ')\n' + testCases.map(t => 'binary_case_test(name = ' + q(t.name) + ', binary = ' + q(t.binary) + ', case = ' + q(t.case) + ')\n').join('') + 'test_suite(name = "selected_tests", tests = ' + list(selectedTests) + ')\nfilegroup(name = "selected_test_binaries", testonly = True, srcs = ' + list(selectedTests) + ')\n');
let specs = '';
for (const [key, {dependency: d, defaults, features}] of [...externalSpecs].sort()) {
  let origin = `version = ${q('=' + d.resolved.version)}`;
  if (d.source?.startsWith('git+')) {
    const url = new URL(d.source.slice(4));
    const rev = url.hash.slice(1);
    url.hash = ''; url.search = '';
    origin = `git = ${q(url.href)}, rev = ${q(rev)}`;
  }
  if (d.registry) throw Error(`Custom registry requires explicit configuration: ${d.registry}`);
  specs += `crate.spec(package = ${q(d.name)}, package_alias = ${q(key)}, ${origin}, default_features = ${defaults ? 'True' : 'False'}, features = ${list([...features].sort())})\n`;
}
for (const name of new Set([...externalSpecs.values()].map(s => s.dependency.name).filter(n => n.startsWith("protoc-bin-vendored-")))) {
  specs += `crate.annotation(crate = ${q(name)}, data_glob = ["bin/**", "include/**"])\n`;
}
fs.mkdirSync(path.join(root, "tools/build-graph"), {recursive: true});
write(path.join(root, "tools/build-graph/cc_toolchain.bzl"), fs.readFileSync(new URL("./cc_toolchain.bzl", import.meta.url), "utf8"));
const sourceLock = fs.readFileSync(path.join(root, 'Cargo.lock'));
const lockInputs = createHash('sha256').update(sourceLock).update(specs).update(cargoConfig).update(JSON.stringify([...registryPatches.values()].map(p => [p.dependencies, p.features]))).digest('hex');
const stamp = path.join(root, 'Cargo.Bazel.inputs');
if (!fs.existsSync(path.join(root, 'Cargo.Bazel.lock')) || !fs.existsSync(stamp) || fs.readFileSync(stamp, 'utf8') !== lockInputs) {
  fs.writeFileSync(path.join(root, 'Cargo.Bazel.lock'), sourceLock);
  write(stamp, lockInputs);
}
write(path.join(root, 'MODULE.bazel'), `# Generated from Cargo metadata; dependency versions pinned.\nbazel_dep(name = "rules_rust", version = "0.75.0")\nbazel_dep(name = "platforms", version = "1.1.0")\nbazel_dep(name = "rules_cc", version = "0.2.17")\nsingle_version_override(module_name = "rules_swift", version = "3.1.2", patches = ["//:tools/build-graph/rules_swift-host.patch"], patch_strip = 1)\nregister_toolchains("//:linux_cc", "//:windows_cc")\nregister_execution_platforms("//:linux_x86_64")\nrust = use_extension("@rules_rust//rust:extensions.bzl", "rust")\nrust.toolchain(edition = "2024", versions = ["1.98.1"], extra_target_triples = ["x86_64-pc-windows-gnu"])\nuse_repo(rust, "rust_toolchains")\nregister_toolchains("@rust_toolchains//:all")\ncrate = use_extension("@rules_rust//crate_universe:extensions.bzl", "crate")\n${specs}crate.render_config(platforms_template = "@@//:{triple}", regen_command = "node tools/build-graph/regenerate.mjs")\ncrate.from_specs(name = "crates", cargo_lockfile = "//:Cargo.Bazel.lock", cargo_config = "//:Cargo.Bazel.config.toml", supported_platform_triples = ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-gnu"])\nuse_repo(crate, "crates")\n`);
console.log(JSON.stringify({packages: active.size, roots, generated: [...active.keys()].sort()}));
