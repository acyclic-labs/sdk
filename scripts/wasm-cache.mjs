#!/usr/bin/env node
// Content-addressed cache for `build-wasm.mjs PACKAGE`: one build per input
// tree, shared by every consumer on the build box.
//
//   bun scripts/wasm-cache.mjs fetch-or-build PACKAGE [--cache DIR]
//   bun scripts/wasm-cache.mjs key PACKAGE
//
// The key hashes every top-level entry of the HEAD tree except documentation
// and repository chrome (EXCLUDED), plus rustc's identity, the WASM profile and
// the package, so any build input (manifests, buf.gen.yaml, compatibility
// families, scripts, sources) changes it. A dirty input tree bypasses the cache.
// A hit restores every file the build generated (all ignored outputs under the
// package closure and the Rust generated sources), verifying each sha256 as it
// copies; any missing or corrupt entry falls back to building. A miss runs
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
const SCHEMA = "acyclic.wasm-cache.v2";

const capture = (command, args) => {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", windowsHide: true, maxBuffer: 64 * 1024 * 1024 });
  if (result.error || result.status !== 0) throw new Error(`${command} ${args.join(" ")} failed: ${result.stderr || result.error}`);
  return result.stdout;
};

const inputs = () => capture("git", ["ls-tree", "HEAD"]).split("\n").filter(Boolean)
  .map(line => ({ line, name: line.slice(line.indexOf("\t") + 1) }))
  .filter(({ name }) => !EXCLUDED.has(name) && !name.endsWith(".md"));

export function cacheKey(name) {
  const entries = inputs();
  // Untracked files at the root (job scripts) are outside every listed path.
  const status = capture("git", ["status", "--porcelain=v1", "--untracked-files=normal", "--", ...entries.map(e => `:(literal)${e.name}`)]);
  if (status.trim() !== "") return null;
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

// Restores an entry; null when it is missing, partial or corrupt.
function fetch(entry) {
  let manifest;
  try {
    const now = new Date();
    utimesSync(entry, now, now); // Recency first, so pruning skips an active reader.
    manifest = JSON.parse(readFileSync(join(entry, "manifest.json"), "utf8"));
    if (manifest.schema !== SCHEMA) return null;
    const files = manifest.files.map(({ path, sha }) => {
      const bytes = readFileSync(join(entry, "files", path));
      if (sha256(bytes) !== sha) throw Object.assign(new Error(`corrupt ${path}`), { corrupt: true });
      return [path, bytes];
    });
    for (const [path, bytes] of files) {
      mkdirSync(dirname(join(root, path)), { recursive: true });
      writeFileSync(join(root, path), bytes);
    }
    return files.length;
  } catch (error) {
    if (error.corrupt) {
      // Move it aside atomically; the rebuild republishes a good entry.
      try { renameSync(entry, mkdtempSync(join(dirname(entry), ".tmp-corrupt-"))); } catch {}
    } else if (error.code !== "ENOENT") throw error;
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
    : key ? `WASM_CACHE_MISS package=${name} key=${key}` : `WASM_CACHE_BYPASS package=${name}: input tree is not clean`);
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
