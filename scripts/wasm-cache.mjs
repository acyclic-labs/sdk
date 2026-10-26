#!/usr/bin/env node
// Content-addressed cache for `build-wasm.mjs PACKAGE`: one build per input
// tree, shared by every consumer on the build box.
//
//   bun scripts/wasm-cache.mjs fetch-or-build PACKAGE [--cache DIR]
//   bun scripts/wasm-cache.mjs key PACKAGE
//
// The key hashes the git tree ids of every build input (the Rust workspace,
// scripts, TypeScript packages and toolchain files), rustc's identity, the
// WASM profile and the package. A hit copies the cached outputs into place
// after verifying each file's sha256; a miss runs build-wasm.mjs and publishes
// the files it wrote with one atomic rename, so readers never see a partial
// entry. A dirty input tree bypasses the cache (it has no content key).
// DIR defaults to $ACYCLIC_WASM_CACHE, else /mnt/bb/artifacts/wasm when it
// exists; with neither, this is exactly build-wasm.mjs.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packageClosure } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const INPUTS = ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo", "rust", "scripts", "typescript/packages", "package.json", "bun.lock"];
const RETAINED = 24;

const capture = (command, args) => {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", windowsHide: true });
  if (result.error || result.status !== 0) throw new Error(`${command} ${args.join(" ")} failed: ${result.stderr || result.error}`);
  return result.stdout.trim();
};

export function cacheKey(name) {
  const present = INPUTS.filter(path => existsSync(join(root, path)));
  if (capture("git", ["status", "--porcelain=v1", "--untracked-files=normal", "--", ...present]) !== "") return null;
  const trees = present.map(path => `${path} ${capture("git", ["rev-parse", `HEAD:${path}`])}`);
  return createHash("sha256").update(JSON.stringify({
    schema: "acyclic.wasm-cache.v1", package: name, trees,
    rustc: capture("rustc", ["-vV"]),
    profile: process.env.ACYCLIC_WASM_PROFILE || "wasm-release",
  })).digest("hex");
}

// Every generated root the build may write for PACKAGE's closure.
function outputRoots(name) {
  const closure = [...packageClosure(root, name === "manifest" ? "workers" : name)];
  return [
    ...closure.flatMap(p => ["generated", "src/generated", "test/generated"].map(d => join("typescript/packages", p, d))),
    ...readdirSync(join(root, "rust/crates")).map(c => join("rust/crates", c, "src/generated")),
  ].filter(path => existsSync(join(root, path)));
}

// Untracked files only: tracked placeholders (package.json, .npmignore) are
// source, not build output.
function files(directory, tracked, out = []) {
  for (const entry of readdirSync(join(root, directory), { withFileTypes: true })) {
    const path = join(directory, entry.name).replaceAll("\\", "/");
    if (entry.isDirectory()) files(path, tracked, out);
    else if (entry.isFile() && !tracked.has(path)) out.push(path);
  }
  return out;
}

const sha256 = path => createHash("sha256").update(readFileSync(path)).digest("hex");

function fetch(entry) {
  const manifest = JSON.parse(readFileSync(join(entry, "manifest.json"), "utf8"));
  for (const { path, sha } of manifest.files) {
    const source = join(entry, "files", path);
    if (sha256(source) !== sha) throw new Error(`wasm cache entry ${entry} is corrupt at ${path}`);
  }
  for (const { path } of manifest.files) {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    copyFileSync(join(entry, "files", path), join(root, path));
  }
  const now = new Date();
  utimesSync(entry, now, now); // Recency for retention.
  return manifest.files.length;
}

function publish(cache, key, written) {
  const temporary = join(cache, `.tmp-${key}-${process.pid}`);
  rmSync(temporary, { recursive: true, force: true });
  const manifest = { schema: "acyclic.wasm-cache.v1", key, files: [] };
  for (const path of written.sort()) {
    mkdirSync(dirname(join(temporary, "files", path)), { recursive: true });
    copyFileSync(join(root, path), join(temporary, "files", path));
    manifest.files.push({ path, sha: sha256(join(root, path)) });
  }
  writeFileSync(join(temporary, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  try { renameSync(temporary, join(cache, key)); }
  catch (error) {
    // A concurrent builder of the same tree won; its entry is equivalent.
    rmSync(temporary, { recursive: true, force: true });
    if (!existsSync(join(cache, key, "manifest.json"))) throw error;
  }
  const entries = readdirSync(cache, { withFileTypes: true }).filter(e => e.isDirectory() && /^[0-9a-f]{64}$/.test(e.name))
    .map(e => ({ path: join(cache, e.name), at: statSync(join(cache, e.name)).mtimeMs })).sort((a, b) => b.at - a.at);
  for (const stale of entries.slice(RETAINED)) rmSync(stale.path, { recursive: true, force: true });
}

export function fetchOrBuild(name, cache) {
  const key = cache ? cacheKey(name) : null;
  const entry = key && join(cache, key);
  if (entry && existsSync(join(entry, "manifest.json"))) {
    console.log(`WASM_CACHE_HIT package=${name} key=${key} files=${fetch(entry)}`);
    return;
  }
  console.log(cache ? (key ? `WASM_CACHE_MISS package=${name} key=${key}` : `WASM_CACHE_BYPASS package=${name}: input tree is not clean`) : `WASM_CACHE_NONE package=${name}`);
  const result = spawnSync(process.execPath, [join(root, "scripts/build-wasm.mjs"), name], { cwd: root, stdio: "inherit", windowsHide: true });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  if (!key) return;
  const roots = outputRoots(name);
  const tracked = new Set(roots.length ? capture("git", ["ls-files", "--", ...roots]).split("\n").filter(Boolean) : []);
  const written = roots.flatMap(path => files(path, tracked));
  if (!written.length) throw new Error(`build-wasm.mjs ${name} wrote no generated outputs`);
  mkdirSync(cache, { recursive: true });
  publish(cache, key, written);
  console.log(`WASM_CACHE_PUBLISHED package=${name} key=${key} files=${written.length}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [verb, name, ...rest] = process.argv.slice(2);
  const cacheIndex = rest.indexOf("--cache");
  const cache = cacheIndex >= 0 ? resolve(rest[cacheIndex + 1])
    : process.env.ACYCLIC_WASM_CACHE ? resolve(process.env.ACYCLIC_WASM_CACHE)
    : existsSync("/mnt/bb/artifacts") ? "/mnt/bb/artifacts/wasm" : null;
  if (!name || !["fetch-or-build", "key"].includes(verb)) {
    console.error("usage: wasm-cache.mjs fetch-or-build|key PACKAGE [--cache DIR]");
    process.exit(2);
  }
  if (verb === "key") console.log(cacheKey(name) ?? "dirty");
  else fetchOrBuild(name, cache);
}
