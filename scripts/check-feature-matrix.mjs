#!/usr/bin/env node
// Checks isolated libraries, then all targets, in each feature set a consumer
// can select on its own: no default features, the defaults and every feature,
// each feature alone and the feature groups requested by local dependents.
// Every set is checked in its own Cargo invocation
// for one package, selected by its manifest, so no other workspace member's
// features unify into it and hide a missing gate. The crates and sets come
// from the manifests, so a new crate or feature is covered without editing
// this script.
//
// Usage: check-feature-matrix.mjs [WORKERS] [--list]
//        [--windows-msvc | --target=TRIPLE] [--package=NAME ...]
// Every extra worker uses its own target directory, so Cargo's build lock never
// serializes them; the compiler cache still deduplicates their dependencies.
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { localCargoPackages } from "./publish-cargo-crates.mjs";
import { featureSets, selectCargoFeatures, cargoFeatureArguments } from "./cargo-feature-selection.mjs";
import { runCommand } from "./run-command.mjs";
export { enabled, featureSets } from "./cargo-feature-selection.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

// Keep each (package, feature selection) in the same cache when another crate
// or feature is added, or when PR and main use different matrix sizes. Absolute
// manifest paths differ between checkouts and must not be part of the key.
export function featureWorker(name, args, workers) {
  return selectionWorker(name, args.slice(5), workers);
}
// One (package, feature flags) -> worker map, shared with consumer codegen so a
// selection's check and build reuse the same worker target.
export function selectionWorker(name, flags, workers) {
  const key = [name, ...flags].join("\0");
  return createHash("sha256").update(key).digest().readUInt32LE(0) % workers;
}
export function workerEnv(index, env = process.env) {
  return index === 0 ? { ...env } : { ...env, CARGO_TARGET_DIR: `${env.CARGO_TARGET_DIR ?? join(root, "target")}-features-${index}` };
}

// Keep the consumer library isolated from test dependencies, then compile
// cfg(test) and other targets with the same feature selection. An all-features
// test run cannot detect a feature-only field used by a test-visible caller.
export function linuxCommands(selection, target) {
  return ["lib", "all-targets"].map(targets => ["check", "--locked",
    ...cargoFeatureArguments(selection, { manifest: true, targets, target })]);
}

export function windowsCommands(selection) {
  return ["lib", "all-targets"].map(targets => ["xwin", "check", "--locked",
    ...cargoFeatureArguments(selection, { manifest: true, target: "x86_64-pc-windows-msvc", targets })]);
}

export function selectFeaturePackages(items, selected) {
  const names = new Set(items.map(item => item.name));
  const missing = selected.filter(name => !names.has(name));
  if (items.length === 0 || missing.length > 0) {
    throw new Error(`STEP_FAILED feature-selection: ${items.length === 0 ? "empty package inventory" : `unknown packages ${JSON.stringify(missing)}`}; recovery: node scripts/check-feature-matrix.mjs --list`);
  }
  return items.filter(item => !selected.length || selected.includes(item.name));
}

async function main() {
  const args = process.argv.slice(2);
  const list = args.includes("--list");
  const windows = args.includes("--windows-msvc");
  const target = args.find(arg => arg.startsWith("--target="))?.slice(9);
  if (windows && target) throw new Error("--windows-msvc and --target are mutually exclusive");
  const selected = args.filter(arg => arg.startsWith("--package=")).map(arg => arg.slice(10));
  const workers = Number(args.find(arg => /^\d+$/u.test(arg)) ?? 1);
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("WORKERS must be a positive integer");
  // Discover every publishable local crate independently of release ordering.
  // Private workspace consumers still contribute their dependency requests.
  const consumers = localCargoPackages();
  const inventory = new Map(consumers.filter(item => item.source === null
    && (item.publish === null || item.publish.includes("crates-io"))).map(item => [item.name, item]));
  const items = [...inventory.values()];
  const sets = selectFeaturePackages(items, selected).flatMap(item => featureSets(item, consumers)
    .map(args => ({ name: item.name, args, selection: selectCargoFeatures(inventory, [item.name], args.slice(5)) })));
  if (list) {
    for (const { selection } of sets) for (const command of windows ? windowsCommands(selection) : linuxCommands(selection, target)) console.log(`cargo ${command.join(" ")}`);
    return;
  }
  const failures = [];
  let passed = 0;
  const queues = Array.from({ length: workers }, () => []);
  for (const unit of sets) queues[featureWorker(unit.name, unit.args, workers)].push(unit);
  async function worker(index) {
    const env = workerEnv(index);
    if (windows) {
      const cacheRoot = process.env.XWIN_CACHE_ROOT;
      if (cacheRoot) env.XWIN_CACHE_DIR = join(cacheRoot, `worker-${index}`);
    }
    for (const { args: set, selection } of queues[index]) {
      const checkStarted = Date.now();
      let succeeded = true;
      for (const command of windows ? windowsCommands(selection) : linuxCommands(selection, target)) {
        const status = await runCommand("cargo", command, { cwd: root, env });
        if (status !== 0) {
          succeeded = false;
          failures.push(`cargo ${command.join(" ")}`);
          process.stderr.write(`FAILED cargo ${command.join(" ")}\n`);
        }
      }
      console.log(`feature check: worker=${index} seconds=${((Date.now() - checkStarted) / 1000).toFixed(1)} manifest=${set[3]} flags=${set.slice(5).join(" ") || "defaults"}`);
      if (succeeded) passed += 1;
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
