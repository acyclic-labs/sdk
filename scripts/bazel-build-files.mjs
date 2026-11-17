// Generates every Bazel file that restates Cargo: one BUILD.bazel per Bazel member and
// cargo.MODULE.bazel (Rust toolchain, rustc flags and crate universe inputs).
// Cargo.toml, Cargo.lock, .cargo/config.toml and rust-toolchain.toml remain the only sources;
// third-party crates resolve through crate_universe from the same Cargo.lock.
// Features come from one whole-workspace resolution with every feature enabled, as the cargo
// test legs build with --all-features, and are fixed per target, so a target's action keys never
// depend on which targets a build selects (Cargo's -p unification rebuilt unchanged units
// whenever the selection changed).
// Every workspace member builds with Bazel, and every target `cargo test --all-features` builds
// is rendered: a target with `test = false` is the only one left out of testing.
import { assertRegistryPinsCurrent, registryInputs, workspaceBrowserGraph, workspaceGraph } from "./workspace-model.mjs";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { GENERATED_HEADER as HEADER, bazelCommand } from "./bazel-executables.mjs";
import { pathToFileURL } from "node:url";
import { repositoryRoot, toml, tomlFiles, trackedFiles } from "./policy-core.mjs";
import { parseJsonc } from "./jsonc.mjs";
import { selectTypeScriptBindings } from "./protobuf-bindings.mjs";
import { runtimeOutputs as streamTokenOutputs } from "./generate-stream-token-operations.mjs";

// The workspace this generator describes. It defaults to the repository that holds this
// script, and --root points the same generator at another Cargo workspace (Cloud).
const args = process.argv.slice(2);
const rootAt = args.indexOf("--root");
const root = rootAt < 0 ? repositoryRoot : path.resolve(args[rootAt + 1]);
if (rootAt >= 0) args.splice(rootAt, 2);
const runnerAt = args.indexOf("--test-runner");
const testRunner = runnerAt < 0 ? "//rust/crates/test-runner:isolated_test.bzl" : args[runnerAt + 1];
if (runnerAt >= 0) args.splice(runnerAt, 2);
// A generation job declares one output manifest instead of changing its source checkout.
const outputAt = args.indexOf("--output");
const output = outputAt < 0 ? null : path.resolve(args[outputAt + 1]);
if (outputAt >= 0) args.splice(outputAt, 2);
const mode = args[0];
if (args.length !== 1 || (mode !== "write" && mode !== "check") || (output && mode !== "write")) {
  throw new Error("STEP_FAILED bazel-build-files: expected [--root DIR] [--test-runner LABEL] write [--output FILE]|check; recovery: node scripts/bazel-build-files.mjs write");
}
assertRegistryPinsCurrent(root);

// Bazel builds each crate for these targets. A dependency or feature that differs between
// them becomes a select() on the rules_rust platform setting.
// Every target a build runs for: the Linux and macOS hosts, and Windows cross-built from Linux.
// The musl toolchain reuses the gnu target's metadata; only the toolchain differs.
const LINUX_GNU_TRIPLE = "x86_64-unknown-linux-gnu";
const WINDOWS_TRIPLE = "x86_64-pc-windows-msvc";
const LINUX_MUSL_TRIPLE = "x86_64-unknown-linux-musl";
const WASM_TRIPLE = "wasm32-unknown-unknown";
// The crate universe hub. Its own name keeps it apart from a consumer module's hub.
const HUB = "sdk_crates";
const TRIPLES = [LINUX_GNU_TRIPLE, "aarch64-apple-darwin", WINDOWS_TRIPLE];
const NOT_LIB = ["tests/**", "benches/**", "examples/**"];
// Each libtest case runs in its own process and fails past this limit, as under cargo-nextest.
const CASE_LIMIT_SECONDS = 10;
const LOADS = {
  cargo_build_script: "@rules_rust//cargo:defs.bzl",
  extract_cargo_lints: "@rules_rust//cargo:defs.bzl",
  isolated_test: testRunner,
  rust_binary: "@rules_rust//rust:defs.bzl",
  rust_library: "@rules_rust//rust:defs.bzl",
  rust_proc_macro: "@rules_rust//rust:defs.bzl",
  rust_shared_library: "@rules_rust//rust:defs.bzl",
  rust_test: "@rules_rust//rust:defs.bzl",
};
const q = value => JSON.stringify(value);

/** @template K,V @param {Map<K,V>} map @param {K} key @returns {V} */
function requiredEntry(map, key) {
  const value = map.get(key);
  if (value === undefined) throw Error(`STEP_FAILED bazel-input-missing: ${key}`);
  return value;
}

// A test is cached by its action key, which includes every --test_env value and every tag that
// marks external state. These tags and flags make a target uncacheable or non-hermetic, so the
// generator refuses them: a test declares its inputs instead.
const UNCACHEABLE_TAGS = new Set(["external", "local", "no-cache", "nocache"]);
const UNCACHEABLE_FLAGS = /--(?:test_env|nocache_test_results|cache_test_results=no)\b/;
function assertCacheable(file, text) {
  for (const [, tags] of text.matchAll(/tags = (\[[^\]]*\])/g)) {
    for (const tag of JSON.parse(tags)) {
      if (UNCACHEABLE_TAGS.has(tag)) throw new Error(`STEP_FAILED bazel-build-files: ${file} tags a test ${tag}, which makes it uncacheable; recovery: declare the input or remove the tag`);
    }
  }
}
const bazelrc = path.join(root, ".bazelrc");
if (fs.existsSync(bazelrc)) {
  for (const line of fs.readFileSync(bazelrc, "utf8").split("\n")) {
    if (line.trimStart().startsWith("#") || !UNCACHEABLE_FLAGS.test(line)) continue;
    throw new Error(`STEP_FAILED bazel-build-files: .bazelrc sets ${line.trim()}, which makes every test uncacheable; recovery: declare the input in the test target`);
  }
}

// Cargo's [lints.rust] check-cfg entries. rules_rust's extract_cargo_lints carries the lint
// levels into its lint files but not these, so they are emitted as rustc flags on every target
// of the package. A member that inherits its lints reads the workspace table.
function checkCfgFlags(manifest, features) {
  const own = toml(root, manifest);
  const table = own.lints?.workspace === true ? toml(root, "Cargo.toml").workspace?.lints?.rust ?? {} : own.lints?.rust ?? {};
  // Cargo itself always passes these two for a manifest build, before the [lints.rust] table: the
  // package's declared features are the feature values it expects.
  return ["--check-cfg=cfg(docsrs,test)", `--check-cfg=cfg(feature, values(${features.map(f => q(f)).join(", ")}))`,
    ...Object.values(table).flatMap(lint => lint["check-cfg"] ?? []).map(cfg => `--check-cfg=${cfg}`)];
}

// .cargo/config.toml's [env], as Cargo gives it to every compile and build script. A relative
// value is resolved against the repository root, which is the execution root of a Bazel action.
const configEnv = Object.fromEntries(Object.entries(toml(root, ".cargo/config.toml").env ?? {}).map(([name, entry]) => {
  if (typeof entry === "string") return [name, entry];
  const relative = path.posix.normalize(entry.value);
  // rules_rust's process wrapper replaces ${pwd} with the execution root; $$ escapes make expansion.
  return [name, entry.relative ? (relative === "." ? "$${pwd}" : `$\${pwd}/${relative}`) : entry.value];
}));

// Every target builds with every feature on, as `cargo test --all-features` does. crate_universe
// resolves third-party features its own way, so universeSpecs() pins each crate's all-features set.
const runs = TRIPLES.map(triple => workspaceGraph(root, triple));
// Published browser modules use a selected production feature closure.
async function wasmResolution() {
  const producer = path.join(root, "scripts/wasm-packages.mjs");
  if (!fs.existsSync(producer)) return;
  const {wasmPackages, wasmSelection, wasmTestConsumers} = await import(pathToFileURL(producer).href);
  const {hostedActorsWasm} = await import(pathToFileURL(path.join(root, "scripts/build-hosted-actors-conformance.mjs")).href);
  const modules = [...Object.keys(wasmPackages).map(name => ({name, ...wasmSelection(name),
    outputPackage:wasmPackages[name].outputPackage ?? name, runtime:wasmPackages[name].runtime, build:wasmPackages[name].build, runtimeDirectory:wasmPackages[name].runtimeDirectory,
    directory:`typescript/packages/${wasmPackages[name].outputPackage ?? name}/generated/wasm`})), {...hostedActorsWasm, outputPackage:undefined}];
  const selected = modules.map(module => module.selection);
  return {run: workspaceBrowserGraph(root, selected), roots:new Set(selected.map(args => args[args.indexOf("-p") + 1])), modules, testConsumers:wasmTestConsumers};
}
const wasm = await wasmResolution();
const [base] = runs;
// The workspace metadata the tests read instead of running Cargo. It holds the workspace members
// only, with manifest paths relative to the workspace root, so the file is relocatable and small.
const workspaceMetadata = (() => {
  const members = new Set(base.workspace_members);
  const relative = p => path.relative(base.workspace_root, p).replaceAll("\\", "/");
  // Cargo's package id embeds the absolute checkout path; the tests only compare ids, so keep
  // them stable across worktrees.
  const stableId = id => id.replaceAll(base.workspace_root, ".");
  return JSON.stringify({
    packages: base.packages.filter(p => members.has(p.id)).map(({workspace_member, ...p}) => ({
      ...p,
      id: stableId(p.id),
      manifest_path: relative(p.manifest_path),
      dependencies: [],
      targets: [],
    })),
    workspace_members: base.workspace_members.map(stableId),
    workspace_default_members: Array.isArray(base.workspace_default_members) ? base.workspace_default_members.map(stableId) : base.workspace_default_members,
    resolve: null,
    target_directory: "target",
    version: base.version,
    workspace_root: ".",
    metadata: base.metadata,
  }) + "\n";
})();
const packages = new Map([...runs, ...(wasm ? [wasm.run] : [])].flatMap(run => run.packages.map(p => [p.id, p])));
const workspace = new Set(runs.flatMap(run => run.workspace_members));
const members = new Set(workspace);
const nodes = runs.map(run => new Map(run.resolve.nodes.map(node => [node.id, node])));
const dir = p => path.relative(root, path.dirname(p.manifest_path)).replaceAll("\\", "/");
tomlFiles(root, [...new Set(["Cargo.toml", "rust-toolchain.toml", ...base.packages.filter(p => p.source === null).map(p => path.relative(root, p.manifest_path))])]);
// Local path crates are first-party: their BUILD files are generated here, so the
// crate universe must not carry them as local path dependencies. Cargo tests only the
// workspace, so a path crate renders its library and build script alone.
for (const p of packages.values()) if (p.source === null) members.add(p.id);
const label = p => `//${dir(p)}`;
const libOf = p => p.targets.find(t => t.kind.some(k => k === "lib" || k === "rlib" || k === "proc-macro"));
const crateName = target => target.name.replaceAll("-", "_");

const wasmNodeModulesTarget = name => `//:wasm_${name}_node_modules`;
const workspaceTargetName = name => `workspace_${name.replaceAll("@", "").replaceAll("/", "_").replaceAll("-", "_")}`;
const starlarkDict = entries => `{\n${entries.map(([key, value]) => `        ${q(key)}: ${q(value)},\n`).join("")}    }`;
// Workspace TypeScript packages by npm name, from their package.json files.
const workspacePackages = new Map();
for (const file of trackedFiles(root, ["typescript/packages/*/package.json", "typescript/packages/@acyclic/*/package.json"])) {
  const pkg = JSON.parse(fs.readFileSync(path.join(root, file), "utf8"));
  if (pkg.name) workspacePackages.set(pkg.name, path.dirname(file).replaceAll("\\", "/"));
}
// Assemble the complete locked runtime closure, including the consumer itself.
// Its sources and generated artifacts then share one physical package directory.
const wasmSelections = wasm?.modules ?? [];
const wasmGenerated = wasmSelections.filter(selection => selection.outputPackage).map(selection => ({ name: `//wasm:${selection.name}`, outDir: selection.directory }));
const npmLock = wasmSelections.some(selection => selection.runtime)
  ? parseJsonc(fs.readFileSync(path.join(root, "bun.lock"), "utf8")) : undefined;
const npmRepositories = new Map();
const wasmNodeModules = new Map();
for (const selection of wasmSelections) {
  if (!selection.runtime) continue;
  const pkg = JSON.parse(fs.readFileSync(path.join(root, `typescript/packages/${selection.outputPackage ?? selection.name}/package.json`), "utf8"));
  const closure = new Map();
  const visit = name => {
    if (closure.has(name)) return;
    const dir = workspacePackages.get(name);
    let dependencies;
    if (dir) {
      closure.set(name, { name, dir });
      dependencies = JSON.parse(fs.readFileSync(path.join(root, dir, "package.json"), "utf8"));
    } else {
      const locked = npmLock.packages[name];
      if (!locked || !locked[3]?.startsWith("sha512-")) throw new Error(`No locked registry archive for ${name}`);
      const resolved = locked[0], at = resolved.lastIndexOf("@");
      const packageName = resolved.slice(0, at), version = resolved.slice(at + 1);
      const repository = `npm_${workspaceTargetName(name)}`;
      npmRepositories.set(repository, { name: packageName, version, integrity: locked[3], patch: npmLock.patchedDependencies?.[resolved] });
      closure.set(name, { name, repository });
      dependencies = locked[2];
    }
    for (const dependency of Object.keys({ ...dependencies.dependencies, ...dependencies.peerDependencies })) visit(dependency);
  };
  visit(pkg.name);
  wasmNodeModules.set(selection.name, [...closure.values()]);
}

// Every tracked file belongs to the innermost member directory, or to the root package, as in Bazel.
const tracked = trackedFiles(root);
const memberDirs = [...members].map(id => dir(packages.get(id))).sort((a, b) => b.length - a.length);
const ownerOf = file => memberDirs.find(d => file.startsWith(`${d}/`)) ?? "";
const within = (owner, file) => (owner ? file.slice(owner.length + 1) : file);
// The files one crate's compile reads, from its root: `mod` children, `#[path]` modules and
// include!, include_str! or include_bytes! literals, followed through every Rust file reached.
// `mod` children are taken from both module layouts; a candidate that exists but is unused is an
// extra input only. A `concat!` whose first literal names no file is a path prefix completed by
// later arguments (a macro's name, say): every tracked file under that prefix is an input.
// A file this misses fails the sandboxed compile loudly; it is never read stale.
const EMBED = /include(?:_str|_bytes)?!\(\s*(concat!\(\s*)?(env!\(\s*"CARGO_MANIFEST_DIR"\s*\)\s*,\s*)?"([^"]+)"|#\[path\s*=\s*()"([^"]+)"\]/g;
const MOD = /^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?mod[ \t]+(?:r#)?(\w+)[ \t]*;/gm;
const TEST_DIRS = /^(tests|benches|examples)\//;
const trackedSet = new Set(tracked);
// A file of another package. Every package exports each of its files (exportsRule), so a test
// here or in a workspace that depends on the SDK can declare any SDK file it reads.
function fileLabel(file) {
  const owner = ownerOf(file);
  return `//${owner}:${within(owner, file)}`;
}
function closure(crateDir, rootFile) {
  const seen = new Set([rootFile]), queue = [rootFile];
  while (queue.length) {
    const file = queue.pop(), here = path.posix.dirname(file);
    const text = fs.readFileSync(path.join(root, file), "utf8");
    const stem = path.posix.join(here, path.posix.basename(file, ".rs"));
    const found = [];
    for (const [, concat, manifestRelative, literal, , module] of text.matchAll(EMBED)) {
      const named = path.posix.normalize(path.posix.join(manifestRelative ? crateDir : here, literal ?? module));
      found.push(...(concat && !trackedSet.has(named) ? tracked.filter(file => file.startsWith(named)) : [named]));
    }
    for (const [, name] of text.matchAll(MOD)) {
      for (const base of [here, stem]) found.push(`${base}/${name}.rs`, `${base}/${name}/mod.rs`);
    }
    for (const next of found) {
      if (!trackedSet.has(next) || seen.has(next)) continue;
      seen.add(next);
      if (next.endsWith(".rs")) queue.push(next);
    }
  }
  return seen;
}
// A `test` atom in a #[cfg] expression counts only when no enclosing `not(...)` negates it.
function cfgHasTest(expr) {
  let i = 0, notDepth = 0, pendingNot = false;
  const stack = [];
  while (i < expr.length) {
    const c = expr[i];
    if (c === "(") {
      stack.push(notDepth);
      if (pendingNot) notDepth++;
      pendingNot = false;
      i++;
      continue;
    }
    if (c === ")") {
      notDepth = stack.pop() ?? 0;
      pendingNot = false;
      i++;
      continue;
    }
    if (/[A-Za-z_]/.test(c)) {
      const start = i;
      while (i < expr.length && /[A-Za-z0-9_]/.test(expr[i])) i++;
      const word = expr.slice(start, i);
      if (word === "not") pendingNot = true;
      else {
        if (word === "test" && notDepth === 0) return true;
        pendingNot = false;
      }
      continue;
    }
    if (!/\s/.test(c)) pendingNot = false;
    i++;
  }
  return false;
}
// The end of the string literal that starts at `at`: its closing quote, raw or not.
function stringEnd(text, at) {
  const raw = /^b?r(#*)"/.exec(text.slice(at));
  if (raw) {
    const close = `"${raw[1]}`, end = text.indexOf(close, at + raw[0].length);
    return end < 0 ? text.length : end + close.length;
  }
  let i = at + 1;
  while (i < text.length && text[i] !== '"') i += text[i] === "\\" ? 2 : 1;
  return i + 1;
}
// Every attribute a source file declares, as its path and arguments: `#[tokio::test]` is
// { path: "tokio::test", args: "" }. Comments and literals are skipped, so a `#[test]` a doc
// comment spells is not a test of this crate.
function* attributes(text) {
  const raw = at => /^(?:b?r#*")/.test(text.slice(at)) && !/\w/.test(text[at - 1] ?? "");
  const string = at => text[at] === '"' || raw(at);
  // A char literal, not a lifetime: `'a'`, `'\n'`, `b'"'`.
  const character = at => (text[at] === "'" && (text[at + 1] === "\\" || text[at + 2] === "'")) || (text[at] === "b" && text[at + 1] === "'");
  for (let i = 0; i < text.length;) {
    if (text.startsWith("//", i)) {
      const end = text.indexOf("\n", i);
      i = end < 0 ? text.length : end;
    } else if (text.startsWith("/*", i)) {
      let depth = 0;
      while (i < text.length) {
        if (text.startsWith("/*", i)) depth++, i += 2;
        else if (text.startsWith("*/", i)) { i += 2; if (!--depth) break; }
        else i++;
      }
    } else if (string(i)) i = stringEnd(text, i);
    else if (character(i)) {
      i += text[i] === "b" ? 2 : 1;
      while (i < text.length && text[i] !== "'") i += text[i] === "\\" ? 2 : 1;
      i++;
    } else if (text[i] === "#" && text[i + 1] === "[") {
      let depth = 1, j = i + 2;
      while (j < text.length && depth) {
        if (string(j)) j = stringEnd(text, j);
        else if (text[j] === "[") depth++, j++;
        else if (text[j] === "]") depth--, j++;
        else j++;
      }
      const inner = text.slice(i + 2, j - 1);
      const head = /^\s*((?:[A-Za-z_]\w*\s*::\s*)*[A-Za-z_]\w*)\s*(?:\(([\s\S]*)\))?\s*$/.exec(inner);
      if (head) yield { path: head[1].replace(/\s+/g, ""), args: head[2] ?? "" };
      i = j;
    } else i++;
  }
}
// The crate's own unit tests, from the test attributes and #[cfg] expressions its library root
// reaches. Only such a crate needs a test-config compile of its library; a crate whose tests all
// live under tests/ links the library's rlib instead and never compiles the library twice.
const unitTests = new Map();
function hasUnitTests(crateDir, rootFile) {
  const key = `${crateDir} ${rootFile}`;
  if (unitTests.has(key)) return unitTests.get(key);
  let found = false;
  for (const file of closure(crateDir, rootFile)) {
    if (!file.endsWith(".rs")) continue;
    for (const { path: attribute, args } of attributes(fs.readFileSync(path.join(root, file), "utf8"))) {
      // The test harnesses in use: `#[test]`, `#[tokio::test]`, `#[wasm_bindgen_test]`.
      const name = attribute.slice(attribute.lastIndexOf(":") + 1);
      if (name === "cfg" ? cfgHasTest(args) : name === "test" || name.endsWith("_test")) {
        found = true;
        break;
      }
    }
    if (found) break;
  }
  unitTests.set(key, found);
  return found;
}
// What a test reads at run time, from the string literals of a module closure: a literal naming
// a tracked file, or a directory holding tracked files, either repository-relative (test-support's
// source_path) or relative to the crate that spells it (crate_path!, CARGO_MANIFEST_DIR joins).
// A test therefore spells each path it reads whole, not assembled from parts: a read this misses
// fails the sandboxed test loudly with a missing file. A literal that only looks like a path adds
// an unread input. An ancestor of the crate is never an input: "../" would name the whole tree.
// The argument of source_path or crate_path! names its read whatever its shape.
const LITERAL = /"([^"\\\n]*\/[^"\\\n]*)"/g;
const NAMED = /\b(source_path|crate_path!|crate_path_str!)\(\s*"([^"\\\n]+)"/g;
const literalReads = new Map();
function readsOf(crateDir, rootFile) {
  const key = `${crateDir} ${rootFile}`;
  if (literalReads.has(key)) return literalReads.get(key);
  const files = new Set(), dirs = new Set();
  for (const file of closure(crateDir, rootFile)) {
    if (!file.endsWith(".rs")) continue;
    const text = fs.readFileSync(path.join(root, file), "utf8");
    const add = (spelled, directory) => {
      const named = path.posix.normalize(spelled).replace(/\/$/, "");
      if (named.startsWith("..") || named === "." || crateDir.startsWith(`${named}/`)) return;
      if (trackedSet.has(named)) files.add(named);
      else if (trackedDirs.has(named) && directory(named)) dirs.add(named);
    };
    for (const [, literal] of text.matchAll(LITERAL)) {
      if (literal.startsWith("/") || literal.includes("{")) continue;
      for (const spelled of [literal, path.posix.join(crateDir, literal)]) {
        add(spelled, named => spelled !== literal || named.includes("/"));
      }
    }
    for (const [, reader, literal] of text.matchAll(NAMED)) {
      add(reader === "source_path" ? literal : path.posix.join(crateDir, literal), () => true);
    }
  }
  const reads = { files, dirs };
  literalReads.set(key, reads);
  return reads;
}
// Every directory that holds a tracked file, at any depth.
const trackedDirs = new Set(tracked.flatMap(file => file.split("/").slice(0, -1).map((_, i, parts) => parts.slice(0, i + 1).join("/"))));
// Cargo discovers .cargo/config.toml from a test's working directory, so a test that carries the
// config also reads every tracked file the config names (the linker wrapper, for instance).
const cargoConfigInputs = (() => {
  const found = new Set();
  const visit = value => {
    if (typeof value === "string") {
      const named = path.posix.normalize(value);
      if (!named.startsWith("..") && !named.startsWith("/") && trackedSet.has(named)) found.add(named);
    } else if (Array.isArray(value)) value.forEach(visit);
    else if (value && typeof value === "object") Object.values(value).forEach(visit);
  };
  visit(toml(root, ".cargo/config.toml"));
  return found;
})();
// A directory's tracked files, as one filegroup in each package that owns some of them: a glob
// cannot cross a package boundary, so a directory spanning nested packages has one per package.
const trees = new Map();
function treeLabels(directory) {
  const owners = new Set(tracked.filter(file => file.startsWith(`${directory}/`)).map(ownerOf));
  return [...owners].sort().map(owner => {
    const rel = owner === directory || owner.startsWith(`${directory}/`) ? "" : within(owner, directory);
    if (!trees.has(owner)) trees.set(owner, new Set());
    trees.get(owner).add(rel);
    return `//${owner}:${rel ? `tree/${rel}` : "tree"}`;
  });
}
// Test-only members: taken as dev-dependencies and never as a normal or build dependency. Their
// helpers name the files the tests read; product libraries do not read the source tree.
const productDeps = new Set(nodes.flatMap(byId => [...byId.values()].flatMap(node => node.deps.filter(dep => dep.dep_kinds.some(k => k.kind !== "dev")).map(dep => dep.pkg))));
function devLibraries(p) {
  const found = new Set();
  for (const byId of nodes) for (const dep of byId.get(p.id)?.deps ?? []) {
    if (members.has(dep.pkg) && !productDeps.has(dep.pkg) && dep.dep_kinds.some(k => k.kind === "dev")) found.add(dep.pkg);
  }
  return [...found].map(id => packages.get(id)).filter(libOf);
}
// A test that can mount FUSE runs outside the linux-sandbox, whose user namespace strips
// fusermount3's setuid bit. A package declaring [package.metadata.bazel] mounts-fuse = true mounts;
// every test whose crate reaches one through its normal or dev dependencies is tagged no-sandbox.
const mountsFuse = new Map();
/** @param {string} id @param {(string|null)[]} [kinds] */
function reachesMount(id, kinds = [null]) {
  if (kinds.length === 1 && mountsFuse.has(id)) return mountsFuse.get(id);
  const reached = packages.get(id).metadata?.bazel?.["mounts-fuse"] === true || nodes.some(byId => (byId.get(id)?.deps ?? [])
    .some(dep => dep.dep_kinds.some(k => kinds.includes(k.kind)) && reachesMount(dep.pkg)));
  if (kinds.length === 1) mountsFuse.set(id, reached);
  return reached;
}
// Each case's temporary directory is RAM-backed on Linux. A package whose tests need a real disk's
// semantics (O_DIRECT, a block device behind statfs) declares [package.metadata.bazel]
// requires-disk-tmp = true, and its tests are tagged requires-disk-tmp.
function testTags(p) {
  const tags = [];
  if (reachesMount(p.id, [null, "dev"])) tags.push("no-sandbox");
  if (p.metadata?.bazel?.["requires-disk-tmp"] === true) tags.push("requires-disk-tmp");
  return tags;
}
// The inputs a target reads beyond its globs, as srcs (Rust) and compile_data (other files):
// same-package paths relative to the package, other packages' files by exported label.
function extraInputs(crateDir, rootFile, covered) {
  const srcs = [], data = [];
  for (const file of [...closure(crateDir, rootFile)].sort()) {
    const owner = ownerOf(file);
    if (owner === crateDir && covered(within(owner, file))) continue;
    (file.endsWith(".rs") ? srcs : data).push(owner === crateDir ? within(owner, file) : fileLabel(file));
  }
  return { srcs, data };
}
// Starlark rendering.
const list = items => (items.length ? `[\n${items.map(i => `        ${q(i)},\n`).join("")}    ]` : "[]");
const glob = (include, exclude) => `glob(${q(include)}${exclude.length ? `, exclude = ${q(exclude)}` : ""}, allow_empty = True)`;
const plus = (...parts) => parts.filter(p => p !== undefined && p !== "[]").join(" + ") || "[]";
const rule = (kind, attrs) => `${kind}(\n${attrs.filter(([, v]) => v !== undefined && v !== "[]").map(([k, v]) => `    ${k} = ${v},\n`).join("")})\n`;

// Files of third-party build dependencies that a build script reads, per platform, from
// [package.metadata.bazel.build-script-env.<os>-<cpu>]: VARIABLE = "package/path". Each file is
// exported from the package's crate universe repository and named to the script by its execution
// path. The platform is the crate's target platform: a build script bundles these files into the crate
// (acyclic-proto-build embeds the protoc its crate runs), so a Windows crate gets the Windows protoc. A
// crate built as a build dependency targets the execution platform, so the protoc a build script runs is
// the host's. A platform the table does not name fails to configure, never builds with another's files.
const staged = new Map();
function stagedFile(spec) {
  const [, name, file] = /^([^/]+)\/(.+)$/.exec(spec) ?? [];
  const found = [...packages.values()].filter(p => p.name === name && !members.has(p.id));
  if (found.length !== 1) throw new Error(`STEP_FAILED bazel-build-files: build-script-env names ${spec}, but ${found.length} third-party packages are named ${name}`);
  const repo = `${HUB}__${name}-${found[0].version}`;
  if (!staged.has(repo)) staged.set(repo, { name, version: found[0].version, files: new Set() });
  staged.get(repo).files.add(file);
  return `@${repo}//:${file}`;
}
const scriptPlatforms = new Set();
const scriptEnv = new Map([...members].map(id => {
  const p = packages.get(id);
  return [id, Object.entries(p.metadata?.bazel?.["build-script-env"] ?? {}).map(([platform, variables]) => {
    if (!/^(linux|macos|windows)-(x86_64|aarch64)$/.test(platform)) throw new Error(`STEP_FAILED bazel-build-files: ${p.name} build-script-env names unknown platform ${platform}`);
    scriptPlatforms.add(platform);
    return [platform, Object.entries(variables).map(([variable, spec]) => [variable, stagedFile(spec)])];
  })];
}));
// select() over the platforms a package names, with no default.
const platformSelect = (id, render) => `select(${q(Object.fromEntries(requiredEntry(scriptEnv,id).map(([platform, vars]) => [`//:${platform}`, render(vars)])))}, no_match_error = ${q(`${packages.get(id).name}: build-script-env names no file for this platform`)})`;
const platformRules = [...scriptPlatforms].sort().map(platform => {
  const [os, cpu] = platform.split("-");
  return `\n${rule("config_setting", [["name", q(platform)], ["constraint_values", list([`@platforms//os:${os}`, `@platforms//cpu:${cpu}`])]])}\n`;
}).join("");
// A package with [package.metadata.bazel] repository-rust-sources = true compiles and builds
// from every Rust source in the repository; each package then exports its own as `rust_sources`.
const readsRepository = p => p.metadata?.bazel?.["repository-rust-sources"] === true;
const repositorySources = [...members].some(id => readsRepository(packages.get(id)))
  ? ["//:rust_sources", ...memberDirs.map(d => `//${d}:rust_sources`).sort()]
  : [];
const sourcesRule = repositorySources.length
  ? `\n${rule("filegroup", [["name", q("rust_sources")], ["srcs", glob(["**/*.rs"], [])], ["visibility", q(["//visibility:public"])]])}\n`
  : "";
// Build script paths name tracked files or directories relative to the package.
// build-script-inputs replaces the package-wide defaults. build-script-data adds inputs.
function scriptData(p, key) {
  return (p.metadata?.bazel?.[key] ?? []).flatMap(relative => {
    const named = path.posix.normalize(path.posix.join(dir(p), relative));
    const files = tracked.filter(file => file === named || file.startsWith(`${named}/`)).sort();
    if (!files.length) throw new Error(`STEP_FAILED bazel-build-files: ${p.name} ${key} names ${relative}, which holds no tracked file`);
    return files.map(file => (ownerOf(file) === dir(p) ? within(dir(p), file) : fileLabel(file)));
  });
}
const exportsRule = owner => `\nexports_files(${glob(["**"], ["BUILD.bazel"])}, visibility = ["//visibility:public"])\n` +
  [...(trees.get(owner) ?? [])].sort().map(rel => `\n${rule("filegroup", [
    ["name", q(rel ? `tree/${rel}` : "tree")],
    ["srcs", glob([rel ? `${rel}/**` : "**"], [])],
    ["visibility", q(["//visibility:public"])],
  ])}`).join("");

// .config/nextest.toml's capability profiles, `[package(=<name>) & ]test(<pattern>)`: under nextest the default
// profile filters their cases out and each profile runs only its own. Each test target leaves them to a manual
// `<target>_<profile>` target that runs them alone; any other filter shape is refused, never guessed.
const nextest = fs.existsSync(path.join(root, ".config/nextest.toml")) ? toml(root, ".config/nextest.toml") : {};
const PROFILES = Object.entries(nextest.profile ?? {}).flatMap(([profile, settings]) => {
  const filter = settings["default-filter"];
  if (profile === "default" || filter === undefined) return [];
  /** @returns {never} */
  const refuse = why => { throw new Error(`STEP_FAILED bazel-build-files: nextest profile ${profile} ${why}`); };
  const match = /^(?:package\(=([\w-]+)\) & )?test\(([^()&|]+)\)$/.exec(filter);
  // test(x) and test(~x) match names containing x, as the runner's selection does; nextest's exact (=), regex (/…/)
  // and glob (#) forms would be matched differently, so they are refused too.
  if (!match || /^[=/#]/.test(match[2])) return refuse(`filter ${filter} is not [package(=name) & ]test(substring); recovery: narrow the profile's default-filter`);
  return [{ profile, package: match[1], pattern: match[2].replace(/^~/, ""), ...profileWrapper(profile, settings, filter, refuse) }];
});
// The run wrapper nextest starts each of a profile's cases under (a fresh service per case, say), as its target's
// `wrapper`: a script of this repository, with the siblings it runs as `$(dirname -- "${BASH_SOURCE[0]}")/<file>`.
// A wrapper over only some of the profile's cases, or a command that is not one tracked script, is refused.
function profileWrapper(profile, settings, filter, refuse) {
  const wrapped = (settings.scripts ?? []).filter(script => script["run-wrapper"] !== undefined);
  if (!wrapped.length) return {};
  if (wrapped.length > 1 || wrapped[0].filter !== filter) refuse(`wraps cases by a filter other than its default-filter ${filter}; recovery: wrap every case of the profile with one run-wrapper`);
  const name = wrapped[0]["run-wrapper"], command = nextest.scripts?.wrapper?.[name]?.command;
  const line = typeof command === "string" ? command : command?.["command-line"];
  if (typeof line !== "string" || (typeof command === "object" && command["relative-to"] !== "workspace-root") || !trackedSet.has(line)) refuse(`run-wrapper ${name} is not one tracked script relative to the workspace root; recovery: name the script alone, relative-to = "workspace-root"`);
  const here = path.posix.dirname(line), text = fs.readFileSync(path.join(root, line), "utf8");
  const siblings = [...text.matchAll(/\$\(dirname -- "\$\{BASH_SOURCE\[0\]\}"\)\/([\w./-]+)/g)].map(([, file]) => path.posix.join(here, file));
  for (const file of siblings) if (!trackedSet.has(file)) refuse(`run-wrapper ${name} runs ${file}, which is not tracked; recovery: track it or stop running it`);
  return { wrapper: line, wrapperData: [...new Set(siblings)].sort() };
}
const cargo = toml(root, "Cargo.toml");
const dev = cargo.profile?.dev ?? {};
// rules_rust (patched) builds fastbuild and dbg as Cargo's dev profile, with debug assertions and overflow checks
// on at every opt-level, and opt as its release profile, with both off: Cargo's defaults. A profile that changes
// either default, at the profile or in any of its package or build-override tables, has no Bazel rendering, so it
// refuses rather than build differently from Cargo.
for (const [profile, expected] of Object.entries({dev:true, test:true, release:false, bench:false})) {
  const settings = cargo.profile?.[profile] ?? {};
  const tables = [
    [`profile.${profile}`, settings],
    [`profile.${profile}.build-override`, settings["build-override"] ?? {}],
    ...Object.entries(settings.package ?? {}).map(([name, table]) => [`profile.${profile}.package.${JSON.stringify(name)}`, table]),
  ];
  for (const [table, values] of tables) {
    for (const key of ["debug-assertions", "overflow-checks"]) {
      const value = values[key];
      if (value !== undefined && value !== expected) throw new Error(`STEP_FAILED bazel-build-files: [${table}] ${key} = ${value} has no Bazel rendering; Bazel builds Cargo's default (${expected})`);
    }
  }
}
const optLevels = { dbg: "0", fastbuild: String(dev["opt-level"] ?? 0), opt: "3" };
const packageOpt = name => dev.package?.[name]?.["opt-level"];

// One value per triple, rendered once when every triple agrees.
function perTriple(values, render) {
  const rendered = values.map(render);
  if (rendered.every(r => r === rendered[0])) return rendered[0];
  return `select({\n${TRIPLES.map((t, i) => `        "@rules_rust//rust/platform:${t}": ${rendered[i]},\n`).join("")}    })`;
}

// Every registry crate's --all-features feature set, pinned by a crate.spec: crate_universe resolves
// third-party features its own way. Path and patch sources keep their lockfile resolution.
const currentRegistry = registryInputs(root).inputs;
const pinned = new Map(currentRegistry.map(p => [`${p.source}#${p.name}@${p.version}`, p.features]));
// The crate universe: every registry crate any target platform resolves. Vendored path crates
// are first-party targets, never universe crates.
const universe = currentRegistry.map(p => ({...p, id: `${p.source}#${p.name}@${p.version}`}))
  .sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : a.version < b.version ? -1 : 1);
// Two versions of one package need distinct splicing dependency keys. A key is a Cargo
// package name, so the version is spelled with XID characters only.
const spliceKey = new Map([...Map.groupBy(universe, p => p.name).values()]
  .filter(versions => versions.length > 1).flat()
  .map(p => [p.id, `${p.name}-${p.version.replace(/[^A-Za-z0-9_-]/g, "_")}`]));
const universeIds = new Set(universe.map(p => p.id));
const spokeRepo = p => `${HUB}__${p.name}-${p.version}`;
// A local patched crate may depend on a registry crate that is only reached
// transitively. The hub exposes direct specs; crate_universe also exposes the
// resolved transitive crate as a spoke, without adding an inactive root spec.
const transitiveSpokes = new Set(runs.flatMap(run => run.resolve.nodes)
  .filter(node => members.has(node.id)).flatMap(node => node.deps)
  .map(dep => packages.get(dep.pkg)).filter(p => p?.source?.startsWith("registry+") && !universeIds.has(p.id))
  .map(spokeRepo));
// The hub names a crate by a splicing dependency key, with dashes as underscores, and its version.
const hubLabel = id => {
  const p = packages.get(id), { name, version } = p;
  if (!universeIds.has(id)) return `@${spokeRepo(p)}//:${crateName(libOf(p))}`;
  return `@${HUB}//${spliceKey.has(id) ? spliceKey.get(id).replaceAll("-", "_") : name}-${version}`;
};
const wasmRuntimeIds = new Set(wasm?.run.resolve.nodes.map(node => node.id) ?? []);
const wasmUniverse = (wasm?.run.packages ?? []).filter(pkg => wasmRuntimeIds.has(pkg.id) && wasm.run.registry_inputs.has(pkg.id))
  .sort((a,b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version));
const wasmSplice = new Map([...Map.groupBy(wasmUniverse, pkg => pkg.name).values()]
  .filter(versions => versions.length > 1).flat().map(pkg =>
    [pkg.id, `${pkg.name}-${pkg.version.replace(/[^A-Za-z0-9_-]/g, "_")}`]));
const wasmNodes = new Map((wasm?.run.resolve.nodes ?? []).map(node => [node.id,node]));
const wasmGraph = wasm && {nodes:[wasmNodes], roots:wasm.roots, suffix:"_wasm",
  hubLabel: id => {const pkg = packages.get(id); return `@wasm_crates//${wasmSplice.get(id)?.replaceAll("-", "_") ?? pkg.name}-${pkg.version}`;}};

// Edges of one member for the selected dependency kinds, per triple, from the all-features
// resolution: first-party dependencies by label, third-party ones by crate universe hub label,
// and every renamed dependency's alias. crate_universe's all_crate_deps() and aliases() are not
// used: they follow its default-features resolution, and aliases() drops the conditional aliases
// of a package with more than one cfg condition.
function edges(p, kinds, graph, variants = {}, names) {
  return (graph?.nodes ?? nodes).map(byId => {
    const deps = [], procMacros = [], aliases = {};
    for (const dep of byId.get(p.id)?.deps ?? []) {
      if (!dep.dep_kinds.some(k => kinds.includes(k.kind)) || (names && !names.includes(dep.name))) continue;
      const target = packages.get(dep.pkg);
      const lib = libOf(target);
      const targetGraph = kinds.includes("build") || lib.kind.includes("proc-macro") ? undefined : graph;
      const variant = variants[target.name];
      if (variant && (!members.has(dep.pkg) || target.metadata?.bazel?.["library-test-variant"] !== variant)) {
        throw new Error(`Unknown unit-test library variant ${target.name}:${variant}`);
      }
      const targetSuffix = variant ? `_${variant}` : targetGraph?.suffix ?? "";
      const name = members.has(dep.pkg) ? `${label(target)}:${crateName(lib)}${targetSuffix}` : (targetGraph?.hubLabel ?? hubLabel)(dep.pkg);
      if (dep.name !== crateName(lib)) aliases[name] = dep.name;
      (lib.kind.includes("proc-macro") ? procMacros : deps).push(name);
    }
    return { deps: deps.sort(), procMacros: procMacros.sort(), aliases };
  });
}

function buildFile(p, graph) {
  const lib = libOf(p), script = p.targets.find(t => t.kind.includes("custom-build"));
  const manifest = path.relative(root, p.manifest_path).replaceAll("\\", "/");
  const inheritedLints = toml(root, manifest).lints?.workspace === true;
  const checkCfg = checkCfgFlags(manifest, Object.keys(p.features).sort());
  const bazel = p.metadata?.bazel;
  const platformSources = Object.entries(bazel?.["platform-sources"] ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const platformSourcePatterns = platformSources.flatMap(([, sources]) => sources);
  const configuredSources = graph || !platformSources.length ? undefined : `select({
${platformSources.map(([platform, sources]) => `        ${q(platform)}: ${glob(sources, [])},`).join("\n")}
        "//conditions:default": [],
    })`;
  const testOnlySources = bazel?.["unit-test-only-sources"] ?? [];
  const libraryTestFeatures = bazel?.["library-test-features"] ?? [];
  const libraryTestVariant = bazel?.["library-test-variant"];
  const unitTestVariants = bazel?.["unit-test-dependency-variants"] ?? {};
  const features = (graph?.nodes ?? nodes).map(byId =>
    [...(byId.get(p.id)?.features ?? [])].filter(feature => !libraryTestFeatures.includes(feature)).sort());
  const suffix = graph?.suffix ?? "";
  const configured = graph ? (values, render) => render(values[0]) : perTriple;
  // A source path relative to the package, or the label of a file another package owns.
  const local = file => {
    const relative = path.relative(root, file).replaceAll("\\", "/");
    return ownerOf(relative) === dir(p) ? within(dir(p), relative) : fileLabel(relative);
  };
  const opt = packageOpt(p.name);
  const repoRelative = target => path.relative(root, target.src_path).replaceAll("\\", "/");
  const libUnitTests = lib !== undefined && workspace.has(p.id) && hasUnitTests(dir(p), repoRelative(lib));
  // Library and binary globs: the package's files outside its test, bench and example trees.
  const libCovered = file => !TEST_DIRS.test(file) && file !== "build.rs";
  const extras = (target, covered) => extraInputs(dir(p), repoRelative(target), covered);
  // srcs and compile_data of a library-shaped target: the package globs plus its extra inputs.
  const libInputs = (target, unitTest = false) => {
    const extra = extras(target, libCovered);
    const testOnly = file => !unitTest && testOnlySources.some(pattern => path.posix.matchesGlob(file, pattern));
    return [
      ["srcs", plus(
        glob(["**/*.rs"], ["build.rs", ...NOT_LIB, ...platformSourcePatterns, ...(unitTest ? [] : testOnlySources)]),
        list(extra.srcs.filter(file => !platformSourcePatterns.includes(file) && !testOnly(file))),
        configuredSources,
      )],
      ["compile_data", plus(glob(["**"], ["**/*.rs", "BUILD.bazel", ...NOT_LIB]), list([...extra.data, ...(readsRepository(p) ? repositorySources : [])]))],
    ];
  };
  const env = { ...configEnv, ...Object.fromEntries([["CARGO_PKG_NAME", p.name], ["CARGO_PKG_DESCRIPTION", p.description], ["CARGO_PKG_REPOSITORY", p.repository], ["CARGO_PKG_LICENSE", p.license], ["CARGO_PKG_RUST_VERSION", p.rust_version]].filter(([, v]) => v)) };
  const clippyConfig = fs.existsSync(path.join(path.dirname(p.manifest_path), "clippy.toml")) ? q("clippy.toml") : undefined;
  const common = (target, features = graph ? "WASM_FEATURES" : "FEATURES") => [
    ["clippy_config", clippyConfig],
    ["edition", q(target.edition)],
    ["version", q(p.version)],
    ["crate_features", features],
    ["rustc_env", "PACKAGE_ENV"],
    ["rustc_flags", opt === undefined && !checkCfg.length ? undefined : list([...(opt === undefined ? [] : [`-Copt-level=${opt}`]), ...checkCfg])],
    ["lint_config", q(":lints")],
    ["target_compatible_with", graph ? q(["@platforms//os:none", "@platforms//cpu:wasm32"]) : undefined],
  ];
  // Rendered dependencies of one target.
  const deps = (kinds, extra = [], extraMacros = [], variants = {}, names) => {
    const first = edges(p, kinds, graph, variants, names);
    return [
      ["deps", plus(list(extra), configured(first, e => list(e.deps)))],
      ["proc_macro_deps", plus(list(extraMacros), configured(first, e => list(e.procMacros)))],
      ["aliases", first.some(e => Object.keys(e.aliases).length) ? configured(first, e => q(Object.fromEntries(Object.entries(e.aliases).sort()))) : undefined],
    ];
  };
  const rules = [], ownDeps = script ? [`:build_script${suffix}`] : [];
  // Cargo's [lints] section, read by rules_rust from the same manifest (and workspace) Cargo reads.
  if (!graph) rules.push(["extract_cargo_lints", [
    ["name", q("lints")],
    ["manifest", q("Cargo.toml")],
    ["workspace", inheritedLints ? q("//:Cargo.toml") : undefined],
  ]]);
  // Targets that use the package's own library: a proc-macro library is a macro dependency.
  const ownLib = lib && !lib.kind.includes("proc-macro") ? [`:${crateName(lib)}`] : [];
  const ownMacro = lib?.kind.includes("proc-macro") ? [`:${crateName(lib)}`] : [];
  // Test names that collide with another target of the package take a `_test` suffix.
  const names = new Set([lib && crateName(lib), ...p.targets.filter(t => t.kind.includes("bin")).map(t => t.name), "build_script"]);
  // Each target `cargo test --all-features --no-run` builds, as rendered; checked against Cargo.
  const covered = graph ? new Set() : requiredEntry(rendered,p.id);
  // The libtest executable builds on its own; the isolated test runs its cases.
  // Cases run in the package directory, as under Cargo, and read its files there.
  // What a test of `target` reads at run time beyond its package's non-Rust files: the literal
  // reads of its own closure, of this package's library, and of its first-party dev-dependencies.
  const directTestDependencies = target => p.metadata?.bazel?.["test-dependencies"]?.[target.name];
  const standaloneTest = target => {
    const direct = directTestDependencies(target);
    return direct !== undefined && !direct.includes(lib && crateName(lib));
  };
  const runtimeData = target => {
    const standalone = standaloneTest(target);
    const libraries = standalone ? [] : [...(lib ? [p] : []), ...devLibraries(p)];
    const all = [readsOf(dir(p), repoRelative(target)), ...libraries.map(other => readsOf(dir(other), repoRelative(libOf(other))))];
    const files = new Set(all.flatMap(reads => [...reads.files])), dirs = new Set(all.flatMap(reads => [...reads.dirs]));
    if (files.has(".cargo/config.toml")) for (const file of cargoConfigInputs) files.add(file);
    const labels = new Set([...dirs].flatMap(treeLabels));
    for (const file of files) {
      const owner = ownerOf(file);
      if (owner !== dir(p)) labels.add(fileLabel(file));
      else if (standalone || file.endsWith(".rs")) labels.add(within(owner, file));
    }
    return [...labels].sort();
  };
  // Each isolated test reports its cases under the executable's nextest binary id, so a case has one
  // identity whichever runner ran it: `<package>` for the library, `<package>::bin/<name>` for a
  // binary, `<package>::<name>` for an integration test, `<package>::<kind>/<name>` for the rest.
  const profiles = PROFILES.filter(entry => entry.package === undefined || entry.package === p.name);
  // Tools a test starts beyond its package's own executables, from
  // [package.metadata.bazel.test-tools.<test>]: ENV = "@repo//:target". The runner names each
  // executable by its Cargo variable, so the test finds it in runfiles without a host path.
  const testTools = (p, test) => Object.fromEntries(Object.entries(p.metadata?.bazel?.["test-tools"]?.[test] ?? {}).map(([name, label]) => [label, name]));
  // Files a test reads at run time beyond its package, from
  // [package.metadata.bazel.test-data.<test>]: NAME = "@repo//:target".
  const testData = (p, test) => Object.values(p.metadata?.bazel?.["test-data"]?.[test] ?? {});
  // A suite whose cases each start Cargo runs fewer at once, so they do not contend for the same
  // cores and exceed the per-case limit.
  const workers = p.metadata?.bazel?.["test-workers"];
  const test = (wanted, attrs, reads, binaries, id, standalone = false) => {
    const tools = { ...binaries, ...testTools(p, wanted) };
    reads = [...new Set([...reads, ...testData(p, wanted)])];
    let name = wanted;
    while (names.has(name) || names.has(`${name}_cases`) || profiles.some(({ profile }) => names.has(`${name}_${profile}`))) name += "_test";
    names.add(name).add(`${name}_cases`);
    for (const { profile } of profiles) names.add(`${name}_${profile}`);
    const tags = testTags(p);
    // Host variables a package's tests read, from [package.metadata.bazel] test-env-inherit =
    // ["NAME"]: only those tests inherit them, so one test's host fact never keys every other test.
    const envInherit = p.metadata?.bazel?.["test-env-inherit"] ?? [];
    const isolated = (target, selection, extra, extraBinaries = {}) => {
      const all = { ...tools, ...extraBinaries };
      return ["isolated_test", [["name", q(target)], ["binary", q(`:${name}_cases`)], ["binaries", Object.keys(all).length ? q(all) : undefined], ["id", q(id)], ...selection, ["workers", workers ? String(workers) : undefined], ["limit_seconds", String(CASE_LIMIT_SECONDS)], ["env_inherit", envInherit.length ? q(envInherit) : undefined], ["tags", tags.length || extra.length ? q([...extra, ...tags]) : undefined]]];
    };
    rules.push(
      ["rust_test", [["name", q(`${name}_cases`)], ...attrs, ["data", plus(standalone ? undefined : glob(["**"], ["**/*.rs", "BUILD.bazel"]), list(reads))], ["tags", q(["manual"])]]],
      isolated(name, [["exclude", profiles.length ? q(profiles.map(({ pattern }) => pattern)) : undefined]], []),
      ...profiles.map(({ profile, pattern, wrapper, wrapperData }) => isolated(`${name}_${profile}`, [["only", q(pattern)], ["wrapper", wrapper && q(fileLabel(wrapper))], ["wrapper_data", wrapperData?.length ? q(wrapperData.map(fileLabel)) : undefined]], ["manual"], wrapper ? { "@nodejs//:node": "node" } : {})),
    );
  };
  if (script) {
    const defaultInputs = p.metadata?.bazel?.["build-script-inputs"] === undefined;
    const extra = extras(script, file => defaultInputs && !file.endsWith(".rs") && !TEST_DIRS.test(file));
    rules.push(["cargo_build_script", [
      ["name", q(`build_script${suffix}`)],
      ["crate_root", q(local(script.src_path))],
      ["srcs", list(extra.srcs)],
      ["compile_data", plus(defaultInputs ? glob(["**"], ["**/*.rs", "BUILD.bazel", ...NOT_LIB]) : undefined, list(extra.data))],
      ["data", plus(defaultInputs ? glob(["**"], ["BUILD.bazel", ...NOT_LIB]) : undefined, list([...(readsRepository(p) ? repositorySources : []), ...scriptData(p, "build-script-inputs"), ...scriptData(p, "build-script-data")]), requiredEntry(scriptEnv,p.id).length ? platformSelect(p.id, vars => vars.map(([, file]) => file)) : undefined)],
      ["pkg_name", q(p.name)],
      ["links", p.links ? q(p.links) : undefined],
      ...common(script).filter(([k]) => k !== "clippy_config"),
      ["build_script_env", (requiredEntry(scriptEnv,p.id).length ? `PACKAGE_ENV | ${platformSelect(p.id, vars => Object.fromEntries(vars.map(([variable, file]) => [variable, `$(execpath ${file})`])))}` : "PACKAGE_ENV") + (graph ? ` | ${q({TARGET: WASM_TRIPLE})}` : "")],
      ...deps(["build"]),
    ]]);
  }
  // A cdylib output, such as a Node addon, builds as a shared library; beside an rlib it takes a suffix.
  const shared = graph ? (graph.roots.has(p.name) ? lib : undefined) : p.targets.find(t => t.kind.includes("cdylib"));
  if (shared) {
    covered.add(`build cdylib ${shared.name}`);
    rules.push(["rust_shared_library", [
      ["name", q(shared === lib ? `${crateName(shared)}${suffix}_cdylib` : crateName(shared))],
      ["crate_name", graph ? q(crateName(shared)) : undefined],
      ["crate_root", q(local(shared.src_path))],
      ...libInputs(shared),
      ...common(shared),
      ...deps([null], ownDeps),
      ["visibility", q(["//visibility:public"])],
    ]]);
  }
  if (lib) {
    for (const kind of lib.kind) if (kind !== "cdylib") covered.add(`build ${kind} ${lib.name}`);
    const proc = lib.kind.includes("proc-macro");
    rules.push([proc ? "rust_proc_macro" : "rust_library", [
      ["name", q(`${crateName(lib)}${suffix}`)],
      ["crate_name", graph ? q(crateName(lib)) : undefined],
      ["crate_root", q(local(lib.src_path))],
      ...libInputs(lib),
      ...common(lib),
      ...deps([null], ownDeps),
      ["visibility", q(["//visibility:public"])],
    ]]);
    if (!graph && libraryTestVariant) {
      rules.push(["rust_library", [
        ["name", q(`${crateName(lib)}_${libraryTestVariant}`)],
        ["crate_name", q(crateName(lib))],
        ["crate_root", q(local(lib.src_path))],
        ...libInputs(lib),
        ...common(lib, plus("FEATURES", list(libraryTestFeatures))),
        ...deps([null], ownDeps),
        ["visibility", q(["//visibility:public"])],
        ["tags", q(["manual"])],
      ]]);
    }
    // A crate with unit tests compiles its library again in test configuration; a crate whose
    // tests are all integration tests only links the library's rlib. rust_test does not inherit
    // compile_data.
    if (!graph && lib.test && libUnitTests) {
      const standalone = testOnlySources.length || Object.keys(unitTestVariants).length;
      const attrs = standalone ? [
        ["crate_name", q(crateName(lib))],
        ["crate_root", q(local(lib.src_path))],
        ...libInputs(lib, true),
        ...common(lib),
        ...deps([null, "dev"], ownDeps, [], unitTestVariants),
      ] : [
        ["crate", q(`:${crateName(lib)}`)],
        ["compile_data", list(extras(lib, libCovered).data)],
        ...common(lib).filter(([k]) => k !== "rustc_env"),
        ...deps(["dev"]),
      ];
      covered.add(`test ${lib.kind[0]} ${lib.name}`);
      test(`${crateName(lib)}_test`, attrs, runtimeData(lib), {}, p.name);
    }
  }
  const executables = !graph && workspace.has(p.id) ? p.targets : [];
  for (const bin of executables.filter(t => t.kind.includes("bin"))) {
    covered.add(`build bin ${bin.name}`);
    rules.push(["rust_binary", [
      ["name", q(bin.name)],
      ["crate_root", q(local(bin.src_path))],
      ...libInputs(bin),
      ...common(bin),
      ...deps([null], [...ownLib, ...ownDeps], ownMacro),
      ["visibility", q(["//visibility:public"])],
    ]]);
    // A binary's own #[cfg(test)] cases, as `cargo test` runs them.
    const binCrate = bin.name.replaceAll("-", "_");
    if (bin.test) covered.add(`test bin ${bin.name}`), test(lib && binCrate === crateName(lib) ? `${binCrate}_bin_test` : `${binCrate}_test`, [
      ["crate_root", q(local(bin.src_path))],
      ...libInputs(bin),
      ...common(bin),
      ...deps([null, "dev"],
        [...ownLib, ...ownDeps], ownMacro),
    ], runtimeData(bin), {}, `${p.name}::bin/${bin.name}`);
  }
  // Integration tests, examples and benches compile from their own directory and the package's
  // library, with dev-dependencies. Cargo builds every example and runs those with `test = true`;
  // it builds a bench only when its `test = true`.
  const separateDeps = () => deps([null, "dev"], [...ownLib, ...ownDeps], ownMacro);
  const sharedDeps = !graph && p.metadata?.bazel?.["share-separate-deps"]
    ? separateDeps().filter(([, value]) => value !== undefined && value !== "[]")
    : [];
  const depVariable = key => `SEPARATE_${key.toUpperCase()}`;
  const separate = target => {
    const names = directTestDependencies(target);
    const includeOwn = !standaloneTest(target);
    const needsBuildScript = includeOwn || p.metadata?.bazel?.["test-build-script"]?.includes(target.name);
    const extraDeps = [...(includeOwn ? ownLib : []), ...(needsBuildScript ? ownDeps : [])];
    const crateRoot = local(target.src_path);
    const extra = extras(target, () => false);
    return [
      ["crate_root", q(crateRoot)],
      ["srcs", list(extra.srcs)],
      ["compile_data", list(extra.data)],
      ...common(target),
      ...(names ? deps([null, "dev"], extraDeps,
        includeOwn ? ownMacro : [], {}, names)
        : sharedDeps.length ? sharedDeps.map(([key]) => [key, depVariable(key)]) : separateDeps()),
    ];
  };
  for (const target of executables.filter(t => (t.kind.includes("test") || t.kind.includes("example") || t.kind.includes("bench")) && t.test)) {
    covered.add(`test ${target.kind[0]} ${target.name}`);
    // Cargo gives integration tests and benches the package's executables as CARGO_BIN_EXE_<name>.
    const binaries = target.kind.includes("test") || target.kind.includes("bench")
      ? Object.fromEntries(p.targets.filter(t => t.kind.includes("bin")).map(bin => [`:${bin.name}`, bin.name]))
      : {};
    const reads = runtimeData(target);
    const consumer = wasm?.testConsumers?.[p.name];
    if (consumer) reads.push(wasmNodeModulesTarget(consumer));
    test(target.name, separate(target), reads, binaries, target.kind.includes("test") ? `${p.name}::${target.name}` : `${p.name}::${target.kind[0]}/${target.name}`, standaloneTest(target));
  }
  for (const example of executables.filter(t => t.kind.includes("example") && !t.test)) {
    let name = `${example.name}_example`;
    while (names.has(name)) name += "_example";
    names.add(name);
    covered.add(`build example ${example.name}`);
    rules.push(["rust_binary", [["name", q(name)], ...separate(example),
      ["visibility", harnessTools.has(`${label(p)}:${name}`) ? q(["//visibility:public"]) : undefined]]]);
  }
  return {rules, features:configured(features, list), env,
    bindings: sharedDeps.map(([key, value]) => [depVariable(key), value])};
}
function renderBuild(native, wasm) {
  const rules = [...native.rules, ...(wasm?.rules ?? [])];
  const loads = Map.groupBy([...new Set(rules.map(([kind]) => kind))].sort(), kind => LOADS[kind]);
  const header = [
    HEADER,
    ...[...loads].sort().map(([file, kinds]) => `load(${q(file)}, ${kinds.map(q).join(", ")})\n`),
    "\n",
    `FEATURES = ${native.features}\n\n`,
    ...(wasm ? [`WASM_FEATURES = ${wasm.features}\n\n`] : []),
    `PACKAGE_ENV = ${q(native.env)}\n`,
    ...native.bindings.map(([name, value]) => `\n${name} = ${value}\n`),
  ];
  return header.join("") + rules.map(([kind, attrs]) => `\n${rule(kind, attrs)}`).join("") + sourcesRule;
}

// Every universe crate is a spec with exactly Cargo's resolved feature set, which names "default"
// when Cargo enables it, so the hub names each one directly and the committed lockfile resolves
// without reading a Cargo workspace.
function universeSpecs() {
  return universe.map(p => `${rule("crate.spec", [
    ["package", q(p.name)],
    ["repositories", list([HUB])],
    ["version", q(`=${p.version}`)],
    ["package_alias", spliceKey.has(p.id) ? q(spliceKey.get(p.id)) : undefined],
    ["default_features", p.defaultFeatures ? "True" : "False"],
    ["features", list([...(pinned.get(p.id) ?? [])].sort())],
  ])}
`);
}

// The Windows target's own rustc flags: a static CRT.
// The xwin import libraries, which the C toolchain gives every link through LIB.
const WINDOWS_LIB_DIRS = ["crt/lib/x86_64", "sdk/lib/um/x86_64", "sdk/lib/ucrt/x86_64"];
const WINDOWS_RUSTC_FLAGS = [
  "-Ctarget-feature=+crt-static",
  "-Clink-arg=-nodefaultlib:ucrt",
  "-Clink-arg=-defaultlib:libucrt",
];

// The C toolchain for the Windows target. Its Rust toolchain is rules_rust's own (extra_target_triples): it
// runs on the Linux host, so build scripts and proc macros build in the exec configuration with the host
// toolchain, and only the Windows crates build with the Windows target's flags.
function windowsToolchain() {
  // Paths inside the LLVM and xwin repositories, from their labels: a repository's directory is its
  // canonical name, which only Bazel knows.
  const llvm = tool => `WINDOWS_LLVM + ${q(`/bin/${tool}`)}`;
  const xwin = dir => `WINDOWS_XWIN + ${q(`/${dir}`)}`;
  const env = dirs => `";".join([${dirs.map(xwin).join(", ")}])`;
  const includes = ["crt/include", "sdk/include/ucrt", "sdk/include/um", "sdk/include/shared", "sdk/include/winrt"];
  return `
${rule("cc_toolchain_config", [
    ["name", q("windows_cc_config")],
    ["visibility", q(["//:__pkg__"])],
    ["target_compatible_with", list(["@platforms//os:windows", "@platforms//cpu:x86_64"])],
    // x64_windows with clang-cl selects the MSVC actions: lld-link links and llvm-lib archives.
    ["cpu", q("x64_windows")],
    ["compiler", q("clang-cl")],
    ["toolchain_identifier", q("xwin-x86_64-pc-windows-msvc")],
    ["host_system_name", q("x86_64-unknown-linux-gnu")],
    ["target_system_name", q(WINDOWS_TRIPLE)],
    ["target_libc", q("msvcrt")],
    ["abi_version", q("19.44")],
    ["abi_libc_version", q("msvcrt")],
    ["msvc_cl_path", llvm("clang-cl")],
    ["msvc_link_path", llvm("lld-link")],
    ["msvc_lib_path", llvm("llvm-lib")],
    ["msvc_ml_path", llvm("clang-cl")],
    ["msvc_env_include", env(includes)],
    ["msvc_env_lib", env(WINDOWS_LIB_DIRS)],
    ["msvc_env_path", `WINDOWS_LLVM + "/bin"`],
    ["msvc_env_tmp", q("/tmp")],
    ["cxx_builtin_include_directories", `[${[...includes.map(d => `"%workspace%/" + ${xwin(d)}`), `"%workspace%/" + WINDOWS_LLVM + "/lib/clang/22/include"`].join(", ")}]`],
    ["default_compile_flags", list([`--target=${WINDOWS_TRIPLE}`])],
    ["cxx_flags", list(["/std:c++17"])],
    ["conly_flags", list([])],
    ["archiver_flags", list(["/nologo"])],
    ["tool_paths", `{${Object.entries({ gcc: "clang-cl", ld: "lld-link", ar: "llvm-lib", cpp: "clang-cl", gcov: "clang-cl", nm: "llvm-nm", objdump: "llvm-objdump", strip: "llvm-strip" }).map(([k, t]) => `${q(k)}: ${llvm(t)}`).join(", ")}}`],
    ["supports_parse_showincludes", "False"],
    ["win32_winnt_flag", q("/D_WIN32_WINNT=0x0A00")],
])}
${rule("filegroup", [
    ["name", q("windows_cc_files")],
    ["visibility", q(["//:__pkg__"])],
    ["target_compatible_with", list(["@platforms//os:windows", "@platforms//cpu:x86_64"])],
    ["srcs", q(["@xwin//:sysroot", "@llvm_toolchain_llvm//:bin"])],
])}
${rule("toolchain", [
    ["name", q("windows_cc_toolchain_registration")],
    ["toolchain", q("//:windows_cc_toolchain")],
    ["toolchain_type", q("@bazel_tools//tools/cpp:toolchain_type")],
    ["exec_compatible_with", list(["@platforms//os:linux", "@platforms//cpu:x86_64"])],
    ["target_compatible_with", list(["@platforms//os:windows", "@platforms//cpu:x86_64"])],
])}
`;
}

// The C toolchain resolves its tool paths from its own package, and the Windows paths are relative to
// the exec root, so it stays in the root package. Its config and files read the LLVM and xwin
// repositories, which only the root module creates, so they live in the Windows toolchain package; a
// module that depends on this one never analyses this target.
function windowsCcToolchain() {
  const files = `//${WINDOWS_TOOLCHAIN_PACKAGE}:windows_cc_files`;
  return `
${rule("cc_toolchain", [
    ["name", q("windows_cc_toolchain")],
    ["toolchain_config", q(`//${WINDOWS_TOOLCHAIN_PACKAGE}:windows_cc_config`)],
    ["target_compatible_with", list(["@platforms//os:windows", "@platforms//cpu:x86_64"])],
    ...["all", "ar", "compiler", "linker", "dwp", "objcopy", "strip"].map(kind => [`${kind}_files`, q(files)]),
    ["visibility", q([`//${WINDOWS_TOOLCHAIN_PACKAGE}:__pkg__`])],
])}
`;
}

function rustcFlags() {
  const rustflags = toml(root, ".cargo/config.toml").target?.["cfg(all())"]?.rustflags ?? [];
  return [...rustflags, ...(dev["codegen-units"] ? [`-Ccodegen-units=${dev["codegen-units"]}`] : []), ...(dev.lto === "off" ? ["-Clto=off"] : [])];
}

function moduleFile() {
  const flags = rustcFlags();
  const channel = toml(root, "rust-toolchain.toml").toolchain.channel;
  const channelName = /^(beta|nightly)/.test(channel) ? channel.split("/")[0] : "stable";
  const thirdParty = Object.keys(dev.package ?? {}).flatMap(name =>
    ![...members].some(id => packages.get(id).name === name) && packageOpt(name) !== undefined ? [[name, packageOpt(name)]] : []);
  return [
    HEADER,
    npmRepositories.size ? `npm_archive = use_repo_rule("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")\n` : "",
    ...[...npmRepositories].sort().map(([repository, pkg]) => rule("npm_archive", [
      ["name", q(repository)],
      ["urls", list([`https://registry.npmjs.org/${pkg.name}/-/${pkg.name.split("/").at(-1)}-${pkg.version}.tgz`])],
      ["integrity", q(pkg.integrity)],
      ["strip_prefix", q("package")],
      ["patches", pkg.patch ? list([fileLabel(pkg.patch)]) : undefined],
      ["patch_args", pkg.patch ? list(["-p1"]) : undefined],
      ["build_file_content", q('filegroup(name = "files", srcs = glob(["**"], exclude = ["BUILD.bazel"]), visibility = ["//visibility:public"])\n')],
    ])),
    `rust = use_extension("@rules_rust//rust:extensions.bzl", "rust")\n`,
    // The formatting check uses the pinned toolchain's rustfmt, not rules_rust's default nightly:
    // rustfmt's macro formatting changes between releases, and the nightly one emits calls the
    // repository's macros do not accept. These repositories are independent of the rust toolchain
    // hub, so pinning rustfmt does not change any compile action.
    `rustfmt = use_repo_rule("@rules_rust//rust:repositories.bzl", "rustfmt_toolchain_tools_repository")\n`,
    ...TRIPLES.map(t => `rustfmt(name = ${q(`pinned_rustfmt_${t.replaceAll("-", "_")}`)}, version = ${q(channel)}, exec_triple = ${q(t)})\n`),
    `\n`,
    rule("rust.toolchain", [
      ["versions", list([channel])],
      ["extra_target_triples", list([WINDOWS_TRIPLE, WASM_TRIPLE])],
      // [workspace.package] is optional; every target carries its own package's edition.
      ["edition", cargo.workspace?.package?.edition === undefined ? undefined : q(cargo.workspace.package.edition)],
      ["opt_level", q(optLevels)],
      ["extra_rustc_flags_triples", q(Object.fromEntries(TRIPLES.map(t => [t, t === WINDOWS_TRIPLE ? [...flags, ...WINDOWS_RUSTC_FLAGS] : flags])))],
      ["extra_exec_rustc_flags_triples", q(Object.fromEntries(TRIPLES.map(t => [t, flags])))],
      // A stable name for the Windows toolchain's tools repository, which rules_rust otherwise
      // names with a hash. The root BUILD's cargo filegroup selects it by that name.
      ["aliases", q({ [`rust_windows_x86_64__${WINDOWS_TRIPLE}__${channelName}`]: "rust_windows_x86_64" })],
    ]),
    // rules_rust maps the musl triple to the gnu constraints, so its toolchain carries the
    // repository's musl constraint explicitly and the gnu toolchain cannot be selected for it.
    rule("rust.repository_set", [
      ["name", q("rust_linux_musl")],
      ["versions", list([channel])],
      ["exec_triple", q(LINUX_GNU_TRIPLE)],
      ["target_triple", q(LINUX_MUSL_TRIPLE)],
      ["target_compatible_with", list(["@platforms//os:linux", "@platforms//cpu:x86_64", "//:libc_musl"])],
      // A repository set takes no opt_level from rust.toolchain. Without its own, its musl toolchain and the one it
      // registers for its exec triple (which resolution prefers for gnu targets) compile fastbuild at opt-level 0.
      ["opt_level", q(optLevels)],
      ["extra_rustc_flags", list(flags)],
      ["extra_exec_rustc_flags", list(flags)],
    ]),
    // Compiler executables support this workspace's own qualification scripts.
    // rules_rust generates tools only for the root's toolchain tags; importing
    // these from a dependency would require its consumer to reproduce our tags.
    `use_repo(rust, "rust_toolchains")\n\n`,
    `rust_tools = use_extension("@rules_rust//rust:extensions.bzl", "rust", dev_dependency = True)\n`,
    `use_repo(rust_tools, ${[
      `rust_linux_x86_64__${LINUX_GNU_TRIPLE}__${channelName}_tools`,
      `rust_macos_aarch64__aarch64-apple-darwin__${channelName}_tools`,
      "rust_windows_x86_64_tools",
    ].map(q).join(", ")})\n\n`,
    `xwin = use_repo_rule("//:xwin_repo.bzl", "xwin_repository")\n`,
    `xwin(name = "xwin")\n\n`,
    // The musl toolchain is registered before the hub so it wins over the gnu toolchain, which
    // rules_rust also considers compatible with the musl platform. Every registration is the root
    // module's own: rules_rust keeps only the root module's toolchains and repository sets, and the
    // Windows C toolchain runs from the LLVM repository only the root creates. A module that depends
    // on this one registers its own toolchains.
    `register_toolchains("//toolchains/rustfmt:linux_x86_64", "//toolchains/rustfmt:macos_aarch64", "//toolchains/rustfmt:windows_x86_64", "//${WINDOWS_TOOLCHAIN_PACKAGE}:windows_cc_toolchain_registration", "@rust_toolchains//:rust_linux_musl__${LINUX_MUSL_TRIPLE}__${channelName}", "@rust_toolchains//:all", dev_dependency = True)\n\n`,
    `crate = use_extension("@rules_rust//crate_universe:extensions.bzl", "crate")\n\n`,
    ...thirdParty.map(([name, opt]) => `${rule("crate.annotation", [["crate", q(name)], ["repositories", list([HUB])], ["rustc_flags", list([`-Copt-level=${opt}`])]])}
`),
    // A [patch] replaces a registry crate with a first-party one. The specs resolve the registry
    // crate, so every dependent's edge to it is overridden by the first-party target.
    ...[...new Set(runs.flatMap(run => run.resolve.nodes).filter(node => !members.has(node.id))
      .flatMap(node => node.deps.map(dep => dep.pkg).filter(id => members.has(id) && libOf(packages.get(id)))))].sort().map(id => {
      const p = packages.get(id), lib = libOf(p);
      return `${rule("crate.annotation", [
        ["crate", q(p.name)],
        ["repositories", list([HUB])],
        [lib.kind.includes("proc-macro") ? "override_target_proc_macro" : "override_target_lib", q(`${label(p)}:${crateName(lib)}`)],
      ])}
`;
    }),
    ...universeSpecs(),
    rule("crate.from_specs", [
      ["name", q(HUB)],
      ["lockfile", q("//:cargo-bazel-lock.json")],
      // The splice's own Cargo.lock, which a repin writes; it was first seeded from the workspace
      // Cargo.lock, so the universe resolves Cargo's versions.
      ["cargo_lockfile", q("//:cargo-bazel.Cargo.lock")],
      ["supported_platform_triples", list(TRIPLES)],
    ]),
    ...(wasm ? [
      ...wasmUniverse.map(pkg => rule("crate.spec", [
        ["package", q(pkg.name)], ["version", q(`=${pkg.version}`)],
        ["repositories", list(["wasm_crates"])],
        ["package_alias", wasmSplice.has(pkg.id) ? q(wasmSplice.get(pkg.id)) : undefined],
        // Only live direct requests are roots. crate_universe resolves their
        // transitive features; an old lock must not keep removed features alive.
        ["default_features", wasm.run.registry_inputs.get(pkg.id).defaultFeatures ? "True" : "False"],
        ["features", list([...wasm.run.registry_inputs.get(pkg.id).features].sort())],
      ])),
      ...[...new Set(wasm.run.resolve.nodes.filter(node => !members.has(node.id))
        .flatMap(node => node.deps.map(dep => dep.pkg).filter(id => members.has(id) && libOf(packages.get(id)))))].sort().map(id => {
        const pkg = packages.get(id), lib = libOf(pkg);
        return rule("crate.annotation", [["crate", q(pkg.name)], ["repositories", list(["wasm_crates"])],
          [lib.kind.includes("proc-macro") ? "override_target_proc_macro" : "override_target_lib", q(`${label(pkg)}:${crateName(lib)}${lib.kind.includes("proc-macro") ? "" : "_wasm"}`)]]);
      }),
      rule("crate.from_specs", [["name", q("wasm_crates")], ["lockfile", q("//:cargo-wasm-bazel-lock.json")],
        ["cargo_lockfile", q("//:cargo-wasm-bazel.Cargo.lock")], ["supported_platform_triples", list([LINUX_GNU_TRIPLE, WASM_TRIPLE])]]),
    ] : []),
    ...[...staged].sort().map(([, { name, version, files }]) => `${rule("crate.annotation", [
      ["crate", q(name)],
      ["version", q(`=${version}`)],
      ["repositories", list([HUB])],
      ["additive_build_file_content", q(`exports_files(${JSON.stringify([...files].sort())})\n`)],
    ])}\n`),
    `use_repo(${["crate", HUB, ...(wasm ? ["wasm_crates"] : []), ...[...new Set([...staged.keys(), ...transitiveSpokes])].sort()].map((r, i) => (i ? q(r) : r)).join(", ")})\n`,
  ].join("");
}

const harnessTools = new Set();
const runtimeTools = new Set();
if (wasm) {
  const {packageClosure} = await import(pathToFileURL(path.join(root,"scripts/package-closure.mjs")).href);
  const {bindingTools} = await import(pathToFileURL(path.join(root,"scripts/project-bindings.mjs")).href);
  for (const tool of await bindingTools(packageClosure(root,"harness-actor"))) harnessTools.add(tool);
  for (const selection of wasmSelections.filter(selection => selection.build)) {
    for (const tool of await bindingTools(packageClosure(root, selection.outputPackage))) {
      runtimeTools.add(tool);
      harnessTools.add(tool);
    }
  }
}

// Bodies first: rendering them records every file another package takes by label.
const rendered = new Map([...members].map(id => [id, new Set()]));
const bodies = new Map([...members].map(id => {
  const pkg = packages.get(id);
  // Procedural macros execute on the host, including when their consumer targets Wasm.
  return [dir(pkg), renderBuild(buildFile(pkg), wasmNodes.has(id) && !libOf(pkg)?.kind.includes("proc-macro") ? buildFile(pkg, wasmGraph) : undefined)];
}));
// Coverage equals the cargo legs: every target `cargo test --all-features --no-run` builds.
for (const id of members) {
  const p = packages.get(id), expected = new Set();
  for (const t of workspace.has(id) ? p.targets : [libOf(p)].filter(Boolean)) {
    // Each output kind of a target is its own rendered rule; one that is not rendered fails the check.
    if (!t.kind.some(k => k === "test" || k === "bench" || k === "custom-build")) for (const kind of t.kind) expected.add(`build ${kind} ${t.name}`);
    const unitTestedLib = t === libOf(p) && hasUnitTests(dir(p), path.relative(root, t.src_path).replaceAll("\\", "/"));
    if (t.test && workspace.has(id) && (t.kind.some(k => ["bin", "test", "example", "bench"].includes(k)) || unitTestedLib)) expected.add(`test ${t.kind[0]} ${t.name}`);
  }
  const have = requiredEntry(rendered,id);
  const missing = [...expected].filter(t => !have.has(t)), extra = [...have].filter(t => !expected.has(t));
  if (missing.length || extra.length) throw new Error(`STEP_FAILED bazel-build-files: ${p.name} renders a different target set than cargo test --all-features (missing: ${missing.join(", ") || "none"}; extra: ${extra.join(", ") || "none"})`);
}
const WINDOWS_TOOLCHAIN_PACKAGE = "toolchains/windows";
const windowsLoads = `load("@rules_cc//cc/private/toolchain:windows_cc_toolchain_config.bzl", "cc_toolchain_config")\nload("//:windows_paths.bzl", "WINDOWS_LLVM", "WINDOWS_XWIN")\n`;
const windowsPlatform = rule("platform", [["name", q("windows")], ["constraint_values", list(["@platforms//os:windows", "@platforms//cpu:x86_64"])]]);
const macosPlatform = rule("platform", [["name", q("macos_aarch64")], ["constraint_values", list(["@platforms//os:macos", "@platforms//cpu:aarch64"])]]);
const wasmPlatform = rule("platform", [["name", q("wasm32")], ["constraint_values", list(["@platforms//os:none", "@platforms//cpu:wasm32"])], ["visibility", q(["//visibility:public"])]]);
const libcConstraints = `
${rule("constraint_setting", [["name", q("libc")], ["visibility", q(["//visibility:public"])]])}
${rule("constraint_value", [["name", q("libc_musl")], ["constraint_setting", q(":libc")], ["visibility", q(["//visibility:public"])]])}
`;
const linuxMusl = `
${rule("config_setting", [["name", q("linux-musl")], ["constraint_values", list(["@platforms//os:linux", "@platforms//cpu:x86_64", ":libc_musl"])]])}
${rule("platform", [["name", q("linux_musl")], ["constraint_values", list(["@platforms//os:linux", "@platforms//cpu:x86_64", ":libc_musl"])]])}
`;
// The toolchain's Cargo, as the tests that check real Cargo behaviour start it. Each platform's
// toolchain repository carries the same release's Cargo beside its rustc.
const toolchainFilegroups = (() => {
  const channel = toml(root, "rust-toolchain.toml").toolchain.channel;
  const channelName = /^(beta|nightly)/.test(channel) ? channel.split("/")[0] : "stable";
  const tools = (name, triple, tool) => `@${name}__${triple}__${channelName}_tools//:${tool}`;
  const select = tool => `select(${q({
    "//:linux-x86_64": [tools("rust_linux_x86_64", LINUX_GNU_TRIPLE, tool)],
    "//:macos-aarch64": [tools("rust_macos_aarch64", "aarch64-apple-darwin", tool)],
    "//:windows-x86_64": [`@rust_windows_x86_64_tools//:${tool}`],
  })})`;
  return ["cargo", "rustc"].map(tool => `
${rule("filegroup", [
    ["name", q(tool === "cargo" ? "toolchain_cargo" : tool)],
    ["srcs", select(tool)],
    ["visibility", q(["//visibility:public"])],
  ])}`).join("");
})();
// Every workspace manifest and the lockfile, for tests that validate the scenario source closure.
const workspaceManifests = `
${rule("filegroup", [
    ["name", q("workspace-manifests")],
    ["srcs", list([...new Set([...members].map(id => `${dir(packages.get(id))}/Cargo.toml`))].sort().map(fileLabel).concat(["Cargo.lock"]))],
    ["visibility", q(["//visibility:public"])],
  ])}
`;
const runtimeSelections = wasmSelections.filter(selection => selection.runtime);
const workspaceRuntimeDeps = new Map([...wasmNodeModules.values()].flat().filter(dep => dep.dir).map(dep => [dep.name, dep]));
const runtimeFilegroups = [...workspaceRuntimeDeps.values()].map(dep => {
    const contractOutputs = streamTokenOutputs.filter(file => file.startsWith(`${dep.dir}/`));
    const bindings = selectTypeScriptBindings(new Set([dep.dir.split("/").at(-1)]));
    return rule("filegroup", [
    ["name", q(workspaceTargetName(dep.name))],
    ["srcs", glob([`${dep.dir}/**`], [`${dep.dir}/generated/wasm/**`, `${dep.dir}/generated/proto/**`, ...contractOutputs]) + " + " + list(
      contractOutputs.map(fileLabel))],
    ["visibility", q(["//visibility:public"])],
    ]) + (bindings.length ? `\n${rule("proto_js", [
      ["name", q(`${workspaceTargetName(dep.name)}_proto`)],
      ["schemas", starlarkDict(bindings.map(([stem, , owner]) => {
        const schema = `${owner ?? `rust/crates/${stem.split("/")[0]}`}/proto/${stem.replace(/_pb$/, ".proto")}`;
        return [fileLabel(schema), schema];
      }))],
    ])}` : "");
}).join("\n");
const builtRuntimeDeps = new Map(wasmSelections.filter(selection => selection.build)
  .flatMap(selection => wasmNodeModules.get(selection.name) ?? []).filter(dep => dep.dir).map(dep => [dep.name, dep]));
const runtimePackageNames = [...builtRuntimeDeps.values()].map(dep => dep.dir.split("/").at(-1));
const runtimeBuild = runtimePackageNames.length ? `\n${rule("typescript_packages", [
  ["name", q("sdk_typescript_packages")],
  ["packages", list(runtimePackageNames)],
  ["srcs", glob(["scripts/*.mjs", "scripts/vendor/**", "typescript/packages/*/package.json"], []) + " + " + list(["//:package.json", "//:tsconfig.base.json", ...[...builtRuntimeDeps.values()].map(dep => `:${workspaceTargetName(dep.name)}`)])],
  ["protos", starlarkDict([...builtRuntimeDeps.values()].filter(dep => selectTypeScriptBindings(new Set([dep.dir.split("/").at(-1)])).length)
    .map(dep => [`:${workspaceTargetName(dep.name)}_proto`, dep.dir.split("/").at(-1)]))],
  ["wasms", starlarkDict([...builtRuntimeDeps.values()].flatMap(dep => {
    const generated = wasmGenerated.find(g => g.outDir === `${dep.dir}/generated/wasm`);
    return generated ? [[generated.name, dep.dir.split("/").at(-1)]] : [];
  }))],
  ["tools", starlarkDict([...runtimeTools].sort().map(label => [label, label]))],
])}` + runtimePackageNames.map(name => `\n${rule("filegroup", [
  ["name", q(`sdk_typescript_${name}`)],
  ["srcs", list([":sdk_typescript_packages"])],
  ["output_group", q(name)],
])}`).join("") : "";
const wasmNodeModulesRules = runtimeFilegroups + (runtimeSelections.length ? "\n" : "") + runtimeSelections.map(selection => {
  const deps = wasmNodeModules.get(selection.name) ?? [];
  const packages = deps.map(dep => [dep.dir ? (builtRuntimeDeps.has(dep.name) ? `:sdk_typescript_${dep.dir.split("/").at(-1)}` : `:${workspaceTargetName(dep.name)}`) : `@${dep.repository}//:files`, `${dep.name}=${dep.dir && !builtRuntimeDeps.has(dep.name) ? dep.dir : ""}`]);
  for (const dep of deps.filter(dep => dep.dir)) {
    if (!builtRuntimeDeps.has(dep.name) && selectTypeScriptBindings(new Set([dep.dir.split("/").at(-1)])).length) {
      packages.push([`:${workspaceTargetName(dep.name)}_proto`, `${dep.name}/generated/proto=`]);
    }
    const generated = wasmGenerated.find(g => g.outDir === `${dep.dir}/generated/wasm`);
    if (generated && !builtRuntimeDeps.has(dep.name)) packages.push([generated.name, `${dep.name}/generated/wasm=`]);
  }
  return rule("node_modules", [
    ["name", q(`wasm_${selection.name}_node_modules`)],
    ["directory", selection.runtimeDirectory ? q(selection.runtimeDirectory) : undefined],
    ["packages", starlarkDict(packages)],
    ["visibility", q(["//visibility:public"])],
  ]);
}).join("\n");
/** @type {Map<string,string|null>} */
const files = new Map([
  ["cargo.MODULE.bazel", moduleFile()],
  ["workspace-metadata.json", workspaceMetadata],
  ["BUILD.bazel", `${HEADER}
load("@rules_cc//cc:defs.bzl", "cc_toolchain")
${wasmNodeModulesRules ? 'load("//:node_modules.bzl", "node_modules")\nload("//:proto_js.bzl", "proto_js")\nload("//:typescript_packages.bzl", "typescript_packages")\n' : ""}${libcConstraints}${platformRules}${windowsPlatform}${macosPlatform}${wasmPlatform}${linuxMusl}${toolchainFilegroups}${workspaceManifests}${windowsCcToolchain()}${runtimeFilegroups}${runtimeBuild}${wasmNodeModulesRules.slice(runtimeFilegroups.length)}${sourcesRule}${exportsRule("")}`],
  // The Windows C toolchain's config and files read repositories only the root module creates, so
  // they are their own package: a module that depends on this one never loads it.
  [`${WINDOWS_TOOLCHAIN_PACKAGE}/BUILD.bazel`, `${HEADER}
${windowsLoads}${windowsToolchain()}`],
]);
if (wasm) {
  files.set("harness-tools.bzl", HEADER + "\nHARNESS_TOOLS = {\n" + [...harnessTools].map(label =>
    `    ${q(label)}: Label(${q(label)}),\n`).join("") + "}\n");
  const modules = wasm.modules.map(module => {
    const pkg = [...packages.values()].find(pkg => pkg.name === module.selection[module.selection.indexOf("-p") + 1]);
    return {name:module.name, package:module.outputPackage, directory:module.directory, outName:module.outName,
      module:`${label(pkg)}:${crateName(libOf(pkg))}_wasm_cdylib`, label:`//wasm:${module.name}`};
  });
  files.set("wasm/BUILD.bazel", HEADER + '\nload("//:wasm.bzl", "wasm_bindings")\n\n' + modules.map(module =>
    rule("wasm_bindings", [["name", q(module.name)], ["module", q(module.module)], ["out_name", q(module.outName)],
      ["exec_compatible_with", list(["@platforms//os:linux", "@platforms//cpu:x86_64"])],
      ["visibility", q(["//visibility:public"])]] )).join("\n"));
  files.set("wasm-targets.json", JSON.stringify(modules,null,2) + "\n");
  const {packageClosure} = await import(pathToFileURL(path.join(root,"scripts/package-closure.mjs")).href);
  const closure = packageClosure(root,"harness-actor");
  files.set("harness-wasm.bzl", HEADER + "\nHARNESS_WASM = {\n" + modules.filter(module => module.name === 'hosted-actors' || closure.has(module.package)).map(module =>
    `    ${q(module.package ?? module.name)}: struct(module = Label(${q(module.label)}), directory = ${q(module.directory)}),\n`).join("") + "}\n");
}
for (const [owner, body] of bodies) files.set(`${owner}/BUILD.bazel`, body + exportsRule(owner));
// Every file ends in exactly one newline, cut once here; check then fails on any other ending.
for (const [file, text] of files) if (text !== null) files.set(file, text.replace(/\n*$/, "\n"));
for (const [file, text] of files) if (text !== null) assertCacheable(file, text);
// A generated file whose package left the workspace is stale.
for (const file of trackedFiles(root, ["*BUILD.bazel"])) {
  if (!files.has(file) && fs.readFileSync(path.join(root, file), "utf8").startsWith(HEADER)) files.set(file, null);
}
const stale = [];
for (const [file, text] of files) {
  const target = path.join(root, file), current = fs.existsSync(target) ? fs.readFileSync(target, "utf8") : null;
  if (current === text) continue;
  stale.push(file);
  if (mode === "check" || output) continue;
  if (text === null) fs.rmSync(target);
  else fs.mkdirSync(path.dirname(target), { recursive: true }), fs.writeFileSync(target, text);
}
if (output) fs.writeFileSync(output, JSON.stringify(Object.fromEntries(files)) + "\n");
if (mode === "check" && stale.length) {
  throw new Error(`STEP_FAILED bazel-build-files: stale ${stale.join(", ")}; recovery: node scripts/bazel-build-files.mjs write`);
}
// This generator owns the module files, so it also owns the resolution they produce. A write refreshes the
// committed lockfile; every other Bazel command runs with --lockfile_mode=error and refuses a stale one.
if (mode === "write" && !output && fs.existsSync(path.join(root, "MODULE.bazel"))) {
  execFileSync(bazelCommand(), ["mod", "deps", "--lockfile_mode=update"], { cwd: root, stdio: "inherit" });
}
