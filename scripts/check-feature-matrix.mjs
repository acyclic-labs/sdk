#!/usr/bin/env node
// Type-checks every published crate's library in each feature set a consumer
// can select on its own: no default features, the defaults, every feature,
// and each feature alone. Every set is checked in its own Cargo invocation
// for one package, selected by its manifest, so no other workspace member's
// features unify into it and hide a missing gate. The crates and sets come
// from the manifests, so a new crate or feature is covered without editing
// this script.
//
// Usage: check-feature-matrix.mjs [WORKERS] [--list]
// Every extra worker uses its own target directory, so Cargo's build lock never
// serializes them; the compiler cache still deduplicates their dependencies.
import { spawn } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { metadata } from "./publish-cargo-crates.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/** The Cargo argument sets for one package from its `cargo metadata` entry. */
export function featureSets(item) {
  const own = Object.keys(item.features).filter(name => name !== "default").sort();
  const sets = [["--no-default-features"], [], ["--all-features"]];
  for (const feature of own) sets.push(["--no-default-features", "--features", feature]);
  return sets.map(set => ["check", "--locked", "--manifest-path", item.manifest_path, "--lib", ...set]);
}

async function main() {
  const args = process.argv.slice(2);
  const list = args.includes("--list");
  const workers = Number(args.find(arg => /^\d+$/u.test(arg)) ?? 1);
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("WORKERS must be a positive integer");
  // Every publishable local crate, in publication order (the publish check
  // already proves the order lists each exactly once).
  const sets = [...metadata().values()].flatMap(featureSets);
  if (list) {
    for (const set of sets) console.log(`cargo ${set.join(" ")}`);
    return;
  }
  const failures = [];
  let passed = 0;
  async function worker(index) {
    const env = index === 0 ? process.env
      : { ...process.env, CARGO_TARGET_DIR: `${process.env.CARGO_TARGET_DIR ?? join(root, "target")}-features-${index}` };
    for (let set = sets.shift(); set; set = sets.shift()) {
      const child = spawn("cargo", set, { cwd: root, env, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
      let output = "";
      child.stdout.on("data", chunk => (output += chunk));
      child.stderr.on("data", chunk => (output += chunk));
      const status = await new Promise((done, reject) => child.on("error", reject).on("close", done));
      if (status === 0) passed += 1;
      else {
        failures.push(`cargo ${set.join(" ")}`);
        process.stderr.write(`FAILED cargo ${set.join(" ")}\n${output}\n`);
      }
    }
  }
  const started = Date.now();
  const total = sets.length;
  await Promise.all(Array.from({ length: workers }, (_, index) => worker(index)));
  // Count the sets that ran and passed, so a set that never ran is a failure.
  console.log(`feature matrix: ${passed}/${total} sets ok in ${((Date.now() - started) / 1000).toFixed(1)}s`);
  for (const failure of failures) console.log(`FAILED ${failure}`);
  process.exitCode = passed === total ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
