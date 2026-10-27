#!/usr/bin/env node
// Content-addressed cache for `build-wasm.mjs PACKAGE`: one build per input
// tree, shared by every consumer on the build box.
//
//   bun scripts/wasm-cache.mjs fetch-or-build PACKAGE [--cache DIR]
//   bun scripts/wasm-cache.mjs key PACKAGE
//
// The key hashes every top-level entry of the HEAD tree except documentation
// and repository chrome (EXCLUDED), plus rustc's identity, the WASM profile and
// the package, so any build input (manifests,
// buf.gen.yaml, compatibility families, scripts, sources) changes it. A dirty
// input tree, or any compiler/flag/bindgen override, bypasses the cache.
// A hit restores every file the build generated (all ignored outputs under the
// package closure and the Rust generated sources), verifying each sha256 as it
// copies and binding producer receipts to HEAD; a broken entry is moved
// aside and rebuilt. A miss runs
// build-wasm.mjs and publishes with one atomic rename of a private temporary
// directory, so readers never see a partial entry. DIR defaults to
// $ACYCLIC_WASM_CACHE, else /mnt/bb/artifacts/wasm when /mnt/bb/artifacts
// exists; with neither, this is exactly build-wasm.mjs.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, renameSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packageClosure } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const EXCLUDED = new Set([".agents", ".github", ".gitleaks.toml", ".gitleaksignore", ".jscpd.json", "LICENSE", "NOTICE", "docs", "llms.txt"]);
const RETAINED = 24;
const IDLE_MS = 60 * 60 * 1000; // Entries and temporaries younger than this are never pruned.
const SCHEMA = "acyclic.wasm-cache.v3";
const RECEIPT = "acyclic.wasm-build-receipt.v1";

// The only build output that names a commit is a family producer receipt.
// Its inputs are exactly the keyed tree, so a hit binds it to this HEAD the
// same way build-wasm.mjs re-stamps a validated cached receipt.
export function restamp(path, bytes, head) {
  if (!path.endsWith("/producer-receipt.json")) return bytes;
  const receipt = JSON.parse(bytes.toString("utf8"));
  if (receipt.schema !== RECEIPT || !/^[0-9a-f]{40,64}$/u.test(receipt.source_commit ?? "")) throw new Error(`unexpected receipt ${path}`);
  receipt.source_commit = head;
  return Buffer.from(`${JSON.stringify(receipt, null, 2)}\n`);
}

const capture = (command, args) => {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", windowsHide: true, maxBuffer: 64 * 1024 * 1024 });
  if (result.error || result.status !== 0) throw new Error(`${command} ${args.join(" ")} failed: ${result.stderr || result.error}`);
  return result.stdout;
};

const inputs = () => capture("git", ["ls-tree", "HEAD"]).split("\n").filter(Boolean)
  .map(line => ({ line, name: line.slice(line.indexOf("\t") + 1) }))
  .filter(({ name }) => !EXCLUDED.has(name) && !name.endsWith(".md"));

// Selections that change what build-wasm.mjs would produce or admit beyond the
// key. Any of them bypasses the cache: the build's own producer admission and
// receipt checks stay the single provenance authority.
const OVERRIDES = /^(RUSTC(?:_WORKSPACE_WRAPPER)?|RUSTFLAGS|RUSTDOCFLAGS|CARGO_ENCODED_RUSTFLAGS|CARGO_BUILD_RUSTFLAGS|CARGO_BUILD_TARGET|CARGO_TARGET_WASM32_\w+|ACYCLIC_CARGO_BIN|ACYCLIC_WASM_BINDGEN_BIN|ACYCLIC_WASM_GROUPS_PREPARED)$/;
// Only the selected WASM profile's settings matter; build hosts routinely set
// dev/test profile variables (bb2 exports CARGO_PROFILE_DEV_* for every job).
const profileOverride = environment => new RegExp(`^CARGO_PROFILE_${(environment.ACYCLIC_WASM_PROFILE || "wasm-release").toUpperCase().replaceAll("-", "_")}_\\w+$`);
export const overrides = (environment = process.env) => Object.keys(environment)
  .filter(name => (OVERRIDES.test(name) || profileOverride(environment).test(name)) && environment[name] !== "").sort();

export function cacheKey(name) {
  if (overrides().length) return null;
  const entries = inputs();
  // Untracked files at the root (job scripts) are outside every listed path.
  const status = capture("git", ["status", "--porcelain=v1", "--untracked-files=normal", "--", ...entries.map(e => `:(literal)${e.name}`)]);
  if (status.trim() !== "") return null;
  // Content, not the commit: build boxes give every job a fresh synthetic
  // commit of the same tree. A hit re-stamps producer receipts (restamp).
  return createHash("sha256").update(JSON.stringify({
    schema: SCHEMA, package: name, tree: entries.map(e => e.line),
    rustc: capture("rustc", ["-vV"]).trim(), profile: process.env.ACYCLIC_WASM_PROFILE || "wasm-release",
  })).digest("hex");
}

// Every file the build generated: ignored outputs in the package closure and
// the Rust generated sources, never dependencies or later build products.
function outputs(name) {
  const closure = [...packageClosure(root, name === "manifest" ? "workers" : name)];
  const listed = capture("git", ["ls-files", "-z", "--others", "--ignored", "--exclude-standard", "--",
    ...closure.map(p => `typescript/packages/${p}`), ":(glob)rust/crates/*/src/generated/**",
    ":(exclude,glob)**/node_modules/**", ":(exclude,glob)**/dist/**", ":(exclude,glob)**/*.tsbuildinfo"]);
  return listed.split("\0").filter(Boolean).sort();
}

const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");

// Restores an entry; null when it is absent. A present entry that is partial,
// unreadable or corrupt is moved aside atomically so the rebuild republishes it.
function fetch(entry) {
  try {
    const now = new Date();
    utimesSync(entry, now, now); // Recency first, so pruning skips an active reader.
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
  const head = capture("git", ["rev-parse", "HEAD"]).trim();
  try {
    const manifest = JSON.parse(readFileSync(join(entry, "manifest.json"), "utf8"));
    if (manifest.schema !== SCHEMA) throw new Error(`schema ${manifest.schema}`);
    const files = manifest.files.map(({ path, sha }) => {
      const bytes = readFileSync(join(entry, "files", path));
      if (sha256(bytes) !== sha) throw new Error(`corrupt ${path}`);
      return [path, restamp(path, bytes, head)];
    });
    for (const [path, bytes] of files) {
      mkdirSync(dirname(join(root, path)), { recursive: true });
      writeFileSync(join(root, path), bytes);
    }
    return files.length;
  } catch (error) {
    if (error.code && error.code !== "ENOENT") throw error; // I/O trouble other than a missing file.
    console.error(`WASM_CACHE_BROKEN entry=${entry} ${error.message}`);
    try { renameSync(entry, mkdtempSync(join(dirname(entry), ".tmp-broken-"))); } catch {}
    return null;
  }
}

function prune(cache) {
  const now = Date.now();
  const entries = [];
  for (const name of readdirSync(cache)) {
    let at;
    try { at = statSync(join(cache, name)).mtimeMs; } catch { continue; } // Removed concurrently.
    if (name.startsWith(".tmp-")) { if (now - at > IDLE_MS) rmSync(join(cache, name), { recursive: true, force: true }); }
    else if (/^[0-9a-f]{64}$/.test(name)) entries.push({ name, at });
  }
  entries.sort((a, b) => b.at - a.at);
  for (const stale of entries.slice(RETAINED)) {
    if (now - stale.at > IDLE_MS) rmSync(join(cache, stale.name), { recursive: true, force: true });
  }
}

function publish(cache, key, written) {
  const temporary = mkdtempSync(join(cache, `.tmp-${key.slice(0, 16)}-`));
  try {
    const manifest = { schema: SCHEMA, key, files: [] };
    for (const path of written) {
      const bytes = readFileSync(join(root, path));
      mkdirSync(dirname(join(temporary, "files", path)), { recursive: true });
      writeFileSync(join(temporary, "files", path), bytes);
      manifest.files.push({ path, sha: sha256(bytes) });
    }
    writeFileSync(join(temporary, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
    renameSync(temporary, join(cache, key));
  } catch (error) {
    rmSync(temporary, { recursive: true, force: true });
    // A concurrent builder of the same tree won; its entry is equivalent.
    if (!existsSync(join(cache, key, "manifest.json"))) throw error;
  }
  prune(cache);
}

export function fetchOrBuild(name, cache) {
  const key = cache ? cacheKey(name) : null;
  const restored = key && fetch(join(cache, key));
  if (restored) {
    console.log(`WASM_CACHE_HIT package=${name} key=${key} files=${restored}`);
    return;
  }
  console.log(!cache ? `WASM_CACHE_NONE package=${name}`
    : key ? `WASM_CACHE_MISS package=${name} key=${key}`
    : `WASM_CACHE_BYPASS package=${name}: ${overrides().length ? `build overrides ${overrides().join(",")}` : "input tree is not clean"}`);
  const result = spawnSync(process.execPath, [join(root, "scripts/build-wasm.mjs"), name], { cwd: root, stdio: "inherit", windowsHide: true });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  if (!key) return;
  const written = outputs(name);
  if (!written.length) throw new Error(`build-wasm.mjs ${name} wrote no generated outputs`);
  mkdirSync(cache, { recursive: true });
  publish(cache, key, written);
  console.log(`WASM_CACHE_PUBLISHED package=${name} key=${key} files=${written.length}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [verb, name, ...rest] = process.argv.slice(2);
  const flag = rest.indexOf("--cache");
  const cache = flag >= 0 && rest[flag + 1] ? resolve(rest[flag + 1])
    : process.env.ACYCLIC_WASM_CACHE ? resolve(process.env.ACYCLIC_WASM_CACHE)
    : existsSync("/mnt/bb/artifacts") ? "/mnt/bb/artifacts/wasm" : null;
  if (!name || !["fetch-or-build", "key"].includes(verb) || (flag >= 0 && !rest[flag + 1])) {
    console.error("usage: wasm-cache.mjs fetch-or-build|key PACKAGE [--cache DIR]");
    process.exit(2);
  }
  if (verb === "key") console.log(cacheKey(name) ?? "dirty");
  else fetchOrBuild(name, cache);
}
