#!/usr/bin/env node
// Type-checks every published crate's library in each feature set a consumer
// can select on its own: no default features, the defaults and every feature,
// and with --each-feature each feature alone as well. Every set is checked in its own Cargo invocation
// for one package, selected by its manifest, so no other workspace member's
// features unify into it and hide a missing gate. The crates and sets come
// from the manifests, so a new crate or feature is covered without editing
// this script.
//
// Usage: check-feature-matrix.mjs [WORKERS] [--each-feature] [--list]
// The merge gate runs the three sets per crate on every PR and adds each
// feature alone on main (gate-checks.sh --each-feature); the policy
// qualification lane runs every set.
// Every extra worker uses its own target directory, so Cargo's build lock never
// serializes them; the compiler cache still deduplicates their dependencies.
import { spawn } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { metadata } from "./publish-cargo-crates.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/** Everything `features` enables in this package: its own features and the
 *  optional dependencies and dependency features they name, transitively. */
export function enabled(item, features) {
  const seen = new Set();
  const pending = [...features];
  while (pending.length) {
    const entry = pending.pop();
    if (seen.has(entry)) continue;
    seen.add(entry);
    if (Object.hasOwn(item.features, entry)) pending.push(...item.features[entry]);
  }
  // No code here gates on `feature = "default"`, so it names no code of its own.
  seen.delete("default");
  return [...seen].sort().join(" ");
}

/** The Cargo argument sets for one package from its `cargo metadata` entry.
 *  Sets that enable exactly the same features and optional dependencies (a
 *  lone feature that is the whole default set, defaults that are every
 *  feature, no defaults at all) compile the same code, so each is checked
 *  once. */
export function featureSets(item, eachFeature = true) {
  const own = Object.keys(item.features).filter(name => name !== "default").sort();
  const defaults = Object.hasOwn(item.features, "default") ? ["default"] : [];
  const sets = [
    [["--no-default-features"], []],
    [[], defaults],
    [["--all-features"], Object.keys(item.features)],
    ...(eachFeature ? own : []).map(feature => [["--no-default-features", "--features", feature], [feature]]),
  ];
  const checked = new Set();
  return sets
    .filter(([, features]) => {
      const key = enabled(item, features);
      return !checked.has(key) && checked.add(key);
    })
    .map(([set]) => ["check", "--locked", "--manifest-path", item.manifest_path, "--lib", ...set]);
}

async function main() {
  const args = process.argv.slice(2);
  const list = args.includes("--list");
  const workers = Number(args.find(arg => /^\d+$/u.test(arg)) ?? 1);
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("WORKERS must be a positive integer");
  // Every publishable local crate, in publication order (the publish check
  // already proves the order lists each exactly once).
  const eachFeature = args.includes("--each-feature");
  const sets = [...metadata().values()].flatMap(item => featureSets(item, eachFeature));
  if (list) {
    for (const set of sets) console.log(`cargo ${set.join(" ")}`);
    return;
  }
  const failures = [];
  let passed = 0;
  // Set k always runs in worker k mod WORKERS, and so in the same target
  // directory, so a later run finds what it built there; taking sets from a
  // shared queue sent them to whichever worker was free and a warm run
  // rebuilt nearly everything (199 s against 8 s on bb2 for the PR sets).
  const queues = Array.from({ length: workers }, (_, index) => sets.filter((_, set) => set % workers === index));
  async function worker(index) {
    const env = index === 0 ? process.env
      : { ...process.env, CARGO_TARGET_DIR: `${process.env.CARGO_TARGET_DIR ?? join(root, "target")}-features-${index}` };
    for (const set of queues[index]) {
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
