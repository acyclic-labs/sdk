import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { contractModules, loadContractModules } from "./generate-contracts.mjs";
import { writeChanged } from "./generated-bindings.mjs";

// `generate.mjs` in write mode is one action: its outputs are a function of the files below, the toolchain and the
// selection. An entry records every file the action projected (`writeChanged`) and every directory it pruned, so a hit
// leaves the checkout exactly as a run would. Entries are immutable, published by rename and verified on every read:
// a damaged entry is an error, never a hit. One store serves every checkout and job that can reach it.
export const generationStore = () => process.env.SDK_GENERATION_CACHE
  || join(process.env.XDG_CACHE_HOME || join(homedir(), ".cache"), "acyclic-sdk", "generation");

// Non-Rust inputs: the generation scripts, every crate's protobuf schemas (buf reads them across crates), compatibility
// artifacts, vendored crates, Cargo and package locks and manifests (every workspace member manifest: Cargo reads them
// all to resolve), and the root files crates include. Rust inputs are the workspace crates the generators compile, from
// Cargo's own resolution. A traced generation proves the list: every repository file it reads is an input or output.
const inputPaths = ["scripts", "proto", ":(glob)rust/crates/*/proto/**", "compatibility", "patches", "rust/vendor", ".cargo",
  "Cargo.lock", ":(glob)**/Cargo.toml", "rust-toolchain.toml", "package.json", "bun.lock", "buf.yaml", "buf.gen.yaml",
  "buf.lock", "tsconfig.json", "tsconfig.base.json", "LICENSE", "README.md",
  ":(glob)typescript/packages/*/package.json", ":(glob)typescript/packages/*/tsconfig.json"];
const generatorPackages = ["acyclic-auth", "sdk-proto-codegen", "acyclic-actors", "acyclic-manifest"];

const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const git = (root, args) => execFileSync("git", args, { cwd: root, encoding: "utf8", maxBuffer: Infinity }).split("\0").filter(Boolean);

// Each input file's digest without reading clean files: the index already holds a tracked file's blob id. Only files
// whose working copy differs from the index, and untracked files, are read and hashed.
function digests(root, paths) {
  const files = new Map();
  for (const line of git(root, ["ls-files", "-s", "-z", "--", ...paths])) {
    const tab = line.indexOf("\t");
    files.set(line.slice(tab + 1), `blob:${line.slice(0, tab).split(" ")[1]}`);
  }
  for (const record of git(root, ["status", "--porcelain=v2", "-z", "--untracked-files=all", "--no-renames", "--", ...paths])) {
    // `1 XY sub mH mI mW hH hI path`, `u XY sub m1 m2 m3 mW h1 h2 h3 path`, `? path`.
    const fields = record.startsWith("1 ") ? 8 : record.startsWith("u ") ? 10 : record.startsWith("? ") ? 1 : -1;
    if (fields < 0) continue;
    const path = record.split(" ").slice(fields).join(" ");
    let content;
    try { content = `file:${sha256(readFileSync(join(root, path)))}`; }
    catch (error) { if (error.code !== "ENOENT") throw error; content = "deleted"; }
    files.set(path, content);
  }
  return files;
}

const digestOf = files => sha256([...files].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([path, digest]) => `${path}\0${digest}\n`).join(""));

// The workspace crates the generators compile. They follow from the manifests and the generation scripts alone, so the
// result is kept under those inputs' digest and Cargo runs only when one of them changes.
async function rustCrates(root, inputs) {
  const cache = join(generationStore(), "crates", `${digestOf(inputs)}.json`);
  try { return JSON.parse(readFileSync(cache, "utf8")); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  const contracts = await loadContractModules(contractModules());
  const roots = new Set([...generatorPackages, ...contracts.flatMap(module => (module.rust ?? []).map(([name]) => name))]);
  // Declared path dependencies, optional ones included, need no registry resolution; vendored crates are inputs whole.
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"],
    { cwd: root, encoding: "utf8", maxBuffer: Infinity, stdio: ["ignore", "pipe", "inherit"] }));
  const local = new Map(metadata.packages.map(item => [resolve(dirname(item.manifest_path)), item]));
  const rootDirs = [...local].filter(([, item]) => roots.has(item.name)).map(([directory]) => directory);
  if (rootDirs.length !== roots.size) throw new Error(`STEP_FAILED generation-inputs: unknown generator package in ${[...roots]}`);
  // A generator's examples and tests build its dev-dependencies; its dependencies build only theirs.
  const seen = new Set(), pending = [...rootDirs];
  while (pending.length > 0) {
    const directory = pending.pop();
    if (seen.has(directory)) continue;
    seen.add(directory);
    for (const dep of local.get(directory).dependencies) {
      if (dep.path && local.has(resolve(dep.path)) && (rootDirs.includes(directory) || dep.kind !== "dev")) pending.push(resolve(dep.path));
    }
  }
  const crates = [...seen].map(directory => relative(root, directory).replaceAll("\\", "/")).sort();
  mkdirSync(dirname(cache), { recursive: true });
  const partial = `${cache}.${process.pid}.partial`;
  writeFileSync(partial, JSON.stringify(crates));
  renameSync(partial, cache);
  return crates;
}

async function inputDigests(root) {
  const inputs = digests(root, inputPaths);
  for (const [path, digest] of digests(root, await rustCrates(root, inputs))) inputs.set(path, digest);
  return inputs;
}

/** Every source file the key covers, tracked or untracked but not ignored, repository-relative and sorted. */
export async function generationInputs(root) {
  return [...(await inputDigests(root)).keys()].sort();
}

export async function generationKey(root) {
  const tools = [`sdk-generation-v1\0${process.version}\0${process.platform}-${process.arch}`,
    execFileSync("rustc", ["-vV"], { cwd: root, encoding: "utf8" }),
    ...["SDK_PROTO_CODEGEN", "ACYCLIC_CARGO_BIN"].map(name => `${name}=${process.env[name] ?? ""}`),
    process.env.SDK_PROTO_CODEGEN ? sha256(readFileSync(process.env.SDK_PROTO_CODEGEN)) : ""];
  return sha256(`${tools.map(part => `${part.length}:${part}`).join("")}${digestOf(await inputDigests(root))}`);
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

function restore(root, entry) {
  const outputs = new Set(entry.files.map(file => file.path));
  for (const directory of entry.pruned) {
    let names;
    try { names = readdirSync(join(root, directory), { recursive: true, withFileTypes: true }); }
    catch (error) { if (error.code === "ENOENT") continue; throw error; }
    for (const item of names) {
      const path = relative(root, join(item.parentPath, item.name)).replaceAll("\\", "/");
      if (!item.isDirectory() && !outputs.has(path)) rmSync(join(root, path));
    }
  }
  for (const [path, bytes] of entry.contents) writeChanged(join(root, path), bytes);
}

function recorded(root, journal) {
  const writes = new Set(), pruned = new Set();
  for (const line of readFileSync(journal, "utf8").split("\n").filter(Boolean)) {
    const [kind, path] = line.split("\t");
    const name = relative(root, path).replaceAll("\\", "/");
    if (name.startsWith("..") || name.startsWith("/")) continue;
    (kind === "P" ? pruned : writes).add(name);
  }
  // A projection a later prune removed is not an output.
  const files = [...writes].filter(name => { try { return lstatSync(join(root, name)).isFile(); } catch { return false; } }).sort();
  return { files, pruned: [...pruned].sort() };
}

/** @param {string} root @param {{name: string, packages: string[], native: boolean}} request @param {() => Promise<void>} generate
 * @param {{key?: string}} [options] */
export async function memoizedGeneration(root, request, generate, { key: given } = {}) {
  const started = performance.now();
  const key = given ?? await generationKey(root);
  const keyed = performance.now();
  const store = join(generationStore(), key);
  mkdirSync(store, { recursive: true });
  const report = (result, name) => console.error(`SDK_GENERATION ${result} key=${key.slice(0, 16)} selection=${request.name} entry=${name} key_ms=${Math.round(keyed - started)} ms=${Math.round(performance.now() - started)}`);
  for (const name of readdirSync(store).filter(name => !name.includes(".partial-")).sort()) {
    const covered = JSON.parse(readFileSync(join(store, name, "entry.json"), "utf8"));
    if (!covers(covered, request)) continue;
    restore(root, readEntry(join(store, name)));
    return report("hit", name);
  }
  const scratch = mkdtempSync(join(tmpdir(), "sdk-generation-journal-"));
  const partial = mkdtempSync(join(store, `${request.name}.partial-`));
  try {
    const journal = join(scratch, "journal");
    writeFileSync(journal, "");
    process.env.SDK_GENERATION_JOURNAL = journal;
    try { await generate(); } finally { delete process.env.SDK_GENERATION_JOURNAL; }
    const { files, pruned } = recorded(root, journal);
    const manifest = files.map(path => {
      const bytes = readFileSync(join(root, path));
      mkdirSync(dirname(join(partial, "files", path)), { recursive: true });
      writeFileSync(join(partial, "files", path), bytes);
      return { path, sha256: sha256(bytes) };
    });
    writeFileSync(join(partial, "entry.json"), JSON.stringify({ key, covers: { packages: request.packages, native: request.native }, files: manifest, pruned }));
    // Rename is the publication decision. A concurrent publisher of the same selection must have produced the same bytes.
    try { renameSync(partial, join(store, request.name)); }
    catch (error) {
      if (!["EEXIST", "ENOTEMPTY", "EPERM"].includes(error.code)) throw error;
      const published = readEntry(join(store, request.name));
      if (JSON.stringify(published.files) !== JSON.stringify(manifest)) {
        throw new Error(`STEP_FAILED generation-cache: nondeterministic generation for ${request.name} at ${key}`, { cause: error });
      }
    }
    report("miss", request.name);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
    rmSync(partial, { recursive: true, force: true });
  }
}
