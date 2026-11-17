import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { contractModules, loadContractModules } from "./generate-contracts.mjs";
import { memberDirectory, workspaceMembers } from "./workspace-manifests.mjs";
import { writeChanged } from "./generated-bindings.mjs";

// `generate.mjs` in write mode is one action: its outputs are a function of the files below, the toolchain and the
// selection. An entry records every file the action projected (`writeChanged`) and every directory it pruned, so a hit
// leaves the checkout exactly as a run would. Entries are immutable, published by rename and verified on every read:
// a damaged entry is an error, never a hit. One store serves every checkout and job that can reach it.
export const generationStore = () => process.env.SDK_GENERATION_CACHE
  || join(process.env.XDG_CACHE_HOME || join(homedir(), ".cache"), "acyclic-sdk", "generation");

// Non-Rust inputs: the generation scripts, the Bazel module and BUILD files that build the generators, every crate's
// protobuf schemas (buf reads them across crates), compatibility artifacts, vendored crates, Cargo and package locks and
// manifests (every workspace member manifest), and the root files crates include. Rust inputs are the workspace crates
// the generators compile, from the manifests' declared path dependencies. A traced generation proves the list: every
// repository file it reads is an input or output.
const inputPaths = ["scripts", "tools", "proto", ":(glob)rust/crates/*/proto/**", "compatibility", "patches", "rust/vendor", ".cargo",
  "Cargo.lock", ":(glob)**/Cargo.toml", "rust-toolchain.toml", "MODULE.bazel", "MODULE.bazel.lock", "cargo.MODULE.bazel",
  ".bazelrc", ".bazelversion", ":(glob)*.bzl", ":(glob)*.patch", ":(glob)**/BUILD.bazel", "package.json", "bun.lock", "buf.yaml", "buf.gen.yaml",
  "buf.lock", "tsconfig.json", "tsconfig.base.json", "LICENSE", "README.md",
  ":(glob)typescript/packages/*/package.json", ":(glob)typescript/packages/*/tsconfig.json"];
const generatorCrates = ["rust/crates/auth", "rust/crates/proto-codegen", "rust/crates/actors", "rust/crates/manifest"];

const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const git = (root, args) => execFileSync("git", args, { cwd: root, encoding: "utf8", maxBuffer: Infinity }).split("\0").filter(Boolean);

// Each present input file's Git blob id, the one content identity whatever the file's Git state: a commit, a stage or a
// deletion that leaves the bytes as they are leaves the key as it is. The index already holds a clean file's id; only
// files whose working copy differs from the index, and untracked files, are hashed, through Git's own filters.
export function digests(root, paths) {
  const files = new Map();
  for (const line of git(root, ["ls-files", "-s", "-z", "--", ...paths])) {
    const tab = line.indexOf("\t");
    files.set(line.slice(tab + 1), line.slice(0, tab).split(" ")[1]);
  }
  const changed = [];
  for (const record of git(root, ["status", "--porcelain=v2", "-z", "--untracked-files=all", "--no-renames", "--", ...paths])) {
    // `1 XY sub mH mI mW hH hI path`, `u XY sub m1 m2 m3 mW h1 h2 h3 path`, `? path`.
    const fields = record.startsWith("1 ") ? 8 : record.startsWith("u ") ? 10 : record.startsWith("? ") ? 1 : -1;
    if (fields < 0) continue;
    const path = record.split(" ").slice(fields).join(" ");
    files.delete(path);
    if (lstatSync(join(root, path), { throwIfNoEntry: false })) changed.push(path);
  }
  if (changed.length > 0) {
    const ids = execFileSync("git", ["hash-object", "--stdin-paths"], { cwd: root, input: changed.join("\n"), encoding: "utf8", maxBuffer: Infinity }).split("\n");
    changed.forEach((path, index) => files.set(path, ids[index]));
  }
  return files;
}

const digestOf = files => sha256([...files].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([path, digest]) => `${path}\0${digest}\n`).join(""));

// The generators and every crate a contract module builds: its targets' Bazel packages and any it compiles itself.
const generationRoots = async root => [...generatorCrates,
  ...(await loadContractModules(contractModules())).flatMap(module => [...(module.rust ?? []).map(label => label.slice(2, label.indexOf(":"))), ...(module.crates ?? []).map(name => memberDirectory(root, name))])];

// The workspace crates an action compiles from its root crate directories. They follow from the manifests and scripts
// alone, so the result is kept under those inputs' digest and the action's label.
async function rustCrates(root, inputs, { label, roots: rootNames }) {
  const cache = join(generationStore(), "crates", `${sha256(`${digestOf(inputs)}\0${label}`)}.json`);
  try { return JSON.parse(readFileSync(cache, "utf8")); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  const rootDirs = [...new Set(await rootNames(root))];
  // Declared path dependencies, optional ones included, need no registry resolution; vendored crates are inputs whole.
  const local = workspaceMembers(root);
  const unknown = rootDirs.filter(directory => !local.has(directory));
  if (unknown.length) throw new Error(`STEP_FAILED generation-inputs: ${unknown.join(", ")} is not a workspace member`);
  // A generator's examples and tests build its dev-dependencies; its dependencies build only theirs.
  const seen = new Set(), pending = [...rootDirs];
  while (pending.length > 0) {
    const directory = pending.pop();
    if (seen.has(directory)) continue;
    seen.add(directory);
    for (const dep of local.get(directory).dependencies) {
      if (rootDirs.includes(directory) || dep.kind !== "dev") pending.push(dep.directory);
    }
  }
  const crates = [...seen].sort();
  mkdirSync(dirname(cache), { recursive: true });
  const partial = `${cache}.${process.pid}.partial`;
  writeFileSync(partial, JSON.stringify(crates));
  renameSync(partial, cache);
  return crates;
}

const generation = { label: "generation", roots: generationRoots };

async function inputDigests(root, action) {
  const inputs = digests(root, [...inputPaths, ...action.paths ?? []]);
  for (const [path, digest] of digests(root, await rustCrates(root, inputs, action))) inputs.set(path, digest);
  return inputs;
}

/** Every source file an action's key covers, tracked or untracked but not ignored, repository-relative and sorted. */
export async function generationInputs(root, action = generation) {
  return [...(await inputDigests(root, action)).keys()].sort();
}

/** The key of one action over the toolchain and its input files. Generation and each WASM build are actions of this one
 * mechanism: an action names its root crates (its Rust inputs are their workspace closure), any further input paths, and
 * any further parts that select its outputs.
 * @param {string} root @param {{label: string, roots: () => Promise<string[]>, paths?: string[], parts?: string[]}} [action] */
// Every environment setting that selects a compiler or changes what it emits: the compiler Cargo runs, its flags, Cargo's
// profile, build and per-target configuration. Directories (CARGO_TARGET_DIR, CARGO_BUILD_TARGET_DIR) place outputs that
// are remapped away, parallelism (CARGO_BUILD_JOBS) only schedules work, and a linker or runner never runs for a
// generated source or a wasm module, so none selects an entry.
const buildSetting = name => /^(?:ACYCLIC_CARGO_BIN|RUSTC|RUSTFLAGS|RUSTC_BOOTSTRAP|CARGO_ENCODED_RUSTFLAGS|CARGO_PROFILE_\w+|CARGO_BUILD_\w+|CARGO_TARGET_\w+)$/.test(name)
  && !name.endsWith("_DIR") && name !== "CARGO_BUILD_JOBS" && !/^CARGO_TARGET_\w+_(?:LINKER|RUNNER)$/.test(name);

export async function generationKey(root, action = generation) {
  const tools = [`sdk-generation-v1\0${process.version}\0${process.platform}-${process.arch}`,
    execFileSync(process.env.RUSTC || "rustc", ["-vV"], { cwd: root, encoding: "utf8" }),
    ...Object.keys(process.env).filter(buildSetting).sort().map(name => `${name}=${process.env[name]}`),
    action.label, ...action.parts ?? []];
  return sha256(`${tools.map(part => `${part.length}:${part}`).join("")}${digestOf(await inputDigests(root, action))}`);
}

// A request is served by any entry whose generation covered at least its packages and native outputs.
const covers = (entry, request) => request.packages.every(name => entry.covers.packages.includes(name))
  && (!request.native || entry.covers.native);

function readEntry(directory) {
  const entry = JSON.parse(readFileSync(join(directory, "entry.json"), "utf8"));
  const files = entry.files.map(file => {
    const bytes = readFileSync(join(directory, "files", file.path));
    if (sha256(bytes) !== file.sha256) throw new Error(`STEP_FAILED generation-cache: damaged entry ${directory} (${file.path})`);
    return [file.path, bytes];
  });
  return { ...entry, contents: files };
}

function restore(root, entry, owns) {
  const outputs = new Set(entry.files.map(file => file.path));
  for (const directory of entry.pruned) {
    let names;
    try { names = readdirSync(join(root, directory), { recursive: true, withFileTypes: true }); }
    catch (error) { if (error.code === "ENOENT") continue; throw error; }
    for (const item of names) {
      const path = relative(root, join(item.parentPath, item.name)).replaceAll("\\", "/");
      if (!item.isDirectory() && !outputs.has(path) && owns(path)) rmSync(join(root, path));
    }
  }
  for (const [path, bytes] of entry.contents) writeChanged(join(root, path), bytes);
}

function recorded(root, journal, owns) {
  const writes = new Set(), pruned = new Set();
  for (const line of readFileSync(journal, "utf8").split("\n").filter(Boolean)) {
    const [kind, path] = line.split("\t");
    const name = relative(root, path).replaceAll("\\", "/");
    if (name.startsWith("..") || name.startsWith("/")) continue;
    if (kind === "P") pruned.add(name); else if (owns(name)) writes.add(name);
  }
  // A projection a later prune removed is not an output.
  const files = [...writes].filter(name => { try { return lstatSync(join(root, name)).isFile(); } catch { return false; } }).sort();
  return { files, pruned: [...pruned].sort() };
}

/** @param {string} root @param {{name: string, packages: string[], native: boolean}} request @param {() => Promise<void>} generate
 * @param {{key?: string, owns?: (path: string) => boolean}} [options] An action owns every file it writes or prunes
 * unless `owns` names the repository-relative paths it does: a hit neither overwrites nor prunes any other file. */
export async function memoizedGeneration(root, request, generate, { key: given, owns = () => true } = {}) {
  const started = performance.now();
  const key = given ?? await generationKey(root);
  const keyed = performance.now();
  const store = join(generationStore(), key);
  mkdirSync(store, { recursive: true });
  const report = (result, name) => console.error(`SDK_GENERATION ${result} key=${key.slice(0, 16)} selection=${request.name} entry=${name} key_ms=${Math.round(keyed - started)} ms=${Math.round(performance.now() - started)}`);
  for (const name of readdirSync(store).filter(name => !name.includes(".partial-")).sort()) {
    const covered = JSON.parse(readFileSync(join(store, name, "entry.json"), "utf8"));
    if (!covers(covered, request)) continue;
    restore(root, readEntry(join(store, name)), owns);
    return report("hit", name);
  }
  const scratch = mkdtempSync(join(tmpdir(), "sdk-generation-journal-"));
  const partial = mkdtempSync(join(store, `${request.name}.partial-`));
  try {
    const journal = join(scratch, "journal");
    writeFileSync(journal, "");
    process.env.SDK_GENERATION_JOURNAL = journal;
    try { await generate(); } finally { delete process.env.SDK_GENERATION_JOURNAL; }
    const { files, pruned } = recorded(root, journal, owns);
    const manifest = files.map(path => {
      const bytes = readFileSync(join(root, path));
      mkdirSync(dirname(join(partial, "files", path)), { recursive: true });
      writeFileSync(join(partial, "files", path), bytes);
      return { path, sha256: sha256(bytes) };
    });
    writeFileSync(join(partial, "entry.json"), JSON.stringify({ key, covers: { packages: request.packages, native: request.native }, files: manifest, pruned }));
    // Rename is the publication decision: the first entry under a key serves every later hit. A concurrent publisher
    // keeps its own outputs. Its bytes may differ only where outputs name their own checkout (a WASM receipt records
    // its build's paths), which the key leaves out so that every checkout shares the entry.
    try { renameSync(partial, join(store, request.name)); }
    catch (error) { if (!["EEXIST", "ENOTEMPTY", "EPERM"].includes(error.code)) throw error; }
    report("miss", request.name);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
    rmSync(partial, { recursive: true, force: true });
  }
}
