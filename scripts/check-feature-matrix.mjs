#!/usr/bin/env node
// Checks isolated libraries, then all targets, in each feature set a consumer
// can select on its own: no default features, the defaults and every feature,
// and with --each-feature each feature alone as well. Every set is checked in its own Cargo invocation
// for one package, selected by its manifest, so no other workspace member's
// features unify into it and hide a missing gate. The crates and sets come
// from the manifests, so a new crate or feature is covered without editing
// this script.
//
// Usage: check-feature-matrix.mjs [WORKERS] [--each-feature] [--list]
//        [--windows-msvc] [--package=NAME ...]
//        --consumers [--package=NAME ...] [--list]
//        [--external-only] [--target=TRIPLE]
// Use --each-feature for isolated feature checks.
// Every extra worker uses its own target directory, so Cargo's build lock never
// serializes them; the compiler cache still deduplicates their dependencies.
import { createHash } from "node:crypto";
import { copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { localCargoPackages, metadata } from "./publish-cargo-crates.mjs";
import { enabled, featureSets, selectCargoFeatures, cargoFeatureArguments } from "./cargo-feature-selection.mjs";
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
export function linuxCommands(selection) {
  return ["lib", "all-targets"].map(targets => ["check", "--locked",
    ...cargoFeatureArguments(selection, { manifest: true, targets })]);
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

// Project each dependency declaration independently: target-specific and dev
// consumers must never fund another consumer's feature requirements. Include
// each declared consumer feature, with and without defaults, and their full
// union. Cargo's metadata expands workspace dependency inheritance for us.
export function consumerSelections(packages, selected = []) {
  const published = packages.filter(item => item.source === null
    && (item.publish === null || item.publish.includes("crates-io")));
  const inventory = new Map(published.map(item => [item.name, item]));
  const sets = new Map(selectFeaturePackages(published, selected).map(item => [item.name, new Map()]));
  function add(name, features) {
    if (!sets.has(name)) return;
    const names = [...new Set(features)].sort();
    const key = enabled(inventory.get(name), names);
    if (!sets.get(name).has(key)) sets.get(name).set(key,
      selectCargoFeatures(inventory, [name], ["--no-default-features",
        ...(names.length ? ["--features", names.join(",")] : [])]));
  }
  for (const item of published) {
    add(item.name, []);
    if (Object.hasOwn(item.features, "default")) add(item.name, ["default"]);
  }
  for (const consumer of packages) {
    const own = Object.keys(consumer.features);
    const defaults = own.includes("default") ? ["default"] : [];
    const roots = [[], defaults, own, ...own.flatMap(name => [[name], [...defaults, name]])];
    const closures = new Set(roots.map(features => enabled(consumer, features)));
    for (const closure of closures) {
      const tokens = closure.split(" ").filter(Boolean);
      for (const dependency of consumer.dependencies) {
        if (!dependency.path || !sets.has(dependency.name)) continue;
        const alias = dependency.rename ?? dependency.name;
        const active = !dependency.optional || tokens.includes(`dep:${alias}`)
          || tokens.some(token => token.startsWith(`${alias}/`));
        if (!active) continue;
        const features = [...dependency.features];
        if (dependency.uses_default_features && Object.hasOwn(inventory.get(dependency.name).features, "default")) features.push("default");
        for (const token of tokens) {
          if (token.startsWith(`${alias}/`) || token.startsWith(`${alias}?/`)) features.push(token.slice(token.indexOf("/") + 1));
        }
        add(dependency.name, features);
      }
    }
  }
  return [...sets.values()].flatMap(choices => [...choices.values()]);
}

export function consumerCommand(selection) {
  const library = selection.packages[0].targets.some(target =>
    target.kind.some(kind => ["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"].includes(kind)));
  // Local publication dependencies can own a workspace excluded from the root.
  // Select the defining manifest so their features resolve in that workspace.
  return ["check", "--locked", ...cargoFeatureArguments(selection, { manifest: true, targets: library ? "lib" : undefined })];
}

// A path dependency in a separate workspace never inherits the SDK root's dev
// dependencies. Check both an ordinary downstream dependency and one opting out
// of defaults; the latter must compile filesystem without native-watch or test.
export function externalConsumerManifest(item, defaults, patches) {
  return `[package]\nname = "feature-matrix-consumer"\nversion = "0.0.0"\nedition = "2024"\npublish = false\n\n[workspace]\nresolver = "2"\n\n[dependencies]\n${JSON.stringify(item.name)} = { path = ${JSON.stringify(dirname(item.manifest_path).replaceAll("\\", "/"))}${defaults ? "" : ", default-features = false"} }\n\n${patches}`;
}

async function externalConsumers(packages, selected, { list, target }) {
  const published = packages.filter(item => item.source === null
    && (item.publish === null || item.publish.includes("crates-io")));
  const items = selectFeaturePackages(published, selected);
  // Preserve the repository's dependency sources, including patched proc macros.
  const patches = [...readFileSync(join(root, "Cargo.toml"), "utf8")
    .matchAll(/^\[patch\.[^\n]+\]\r?\n[^\[]*/gm)]
    .map(match => match[0].replace(/path\s*=\s*"([^"]+)"/g,
      (_, path) => `path = ${JSON.stringify(resolve(root, path).replaceAll("\\", "/"))}`)).join("\n");
  const directory = list ? null : mkdtempSync(join(tmpdir(), "sdk-feature-consumer-"));
  const env = { ...process.env, CARGO_TARGET_DIR: resolve(process.env.CARGO_TARGET_DIR ?? join(root, "target")) };
  try {
    if (directory) {
      mkdirSync(join(directory, "src"));
      writeFileSync(join(directory, "src/lib.rs"), "// Dependencies are checked in production, without cfg(test).\n");
    }
    for (const item of items) {
      if (!item.targets.some(entry => entry.kind.some(kind =>
        ["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"].includes(kind)))) {
        console.log(`external consumer: ${item.name} has no library; covered by standalone package check`);
        continue;
      }
      for (const defaults of [false, true]) {
        console.log(`external consumer: ${item.name} defaults=${defaults} target=${target ?? "host"}`);
        if (list) continue;
        writeFileSync(join(directory, "Cargo.toml"), externalConsumerManifest(item, defaults, patches));
        // Seed the separate workspace from the checked-in resolution. Its new
        // root needs a lockfile entry, so --locked cannot be used on this check.
        copyFileSync(join(root, "Cargo.lock"), join(directory, "Cargo.lock"));
        const status = await runCommand("cargo", ["check", "--offline", "--manifest-path", join(directory, "Cargo.toml"),
          ...(target ? ["--target", target] : [])], { cwd: root, env });
        if (status !== 0) return status;
      }
    }
    return 0;
  } finally {
    if (directory) rmSync(directory, { recursive: true, force: true });
  }
}

async function main() {
  const args = process.argv.slice(2);
  const list = args.includes("--list");
  const windows = args.includes("--windows-msvc");
  const selected = args.filter(arg => arg.startsWith("--package=")).map(arg => arg.slice(10));
  const workers = Number(args.find(arg => /^\d+$/u.test(arg)) ?? 1);
  const target = args.find(arg => arg.startsWith("--target="))?.slice(9);
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("WORKERS must be a positive integer");
  if (args.includes("--consumers")) {
    if (workers !== 1 || windows || args.includes("--each-feature")) throw new Error("consumer checks require one shared target directory and native Cargo");
    // Serial, standalone checks share Cargo's existing target directory. Check
    // libraries without dev dependencies so test-support cannot mask a defect.
    const packages = localCargoPackages();
    const commands = args.includes("--external-only") ? [] : consumerSelections(packages, selected).map(selection =>
      [...consumerCommand(selection), ...(target ? ["--target", target] : [])]);
    for (const command of commands) {
      console.log(`cargo ${command.join(" ")}`);
      if (!list) {
        const status = await runCommand("cargo", command, { cwd: root });
        if (status !== 0) { process.exitCode = status; return; }
      }
    }
    console.log(`consumer feature matrix: ${commands.length} standalone selections${list ? " listed" : " passed"}`);
    process.exitCode = await externalConsumers(packages, selected, { list, target });
    return;
  }
  // Every publishable local crate, in publication order (the publish check
  // already proves the order lists each exactly once).
  const eachFeature = args.includes("--each-feature");
  const inventory = metadata();
  const items = [...inventory.values()];
  const sets = selectFeaturePackages(items, selected).flatMap(item => featureSets(item, eachFeature)
    .map(args => ({ name: item.name, args, selection: selectCargoFeatures(inventory, [item.name], args.slice(5)) })));
  if (list) {
    for (const { selection } of sets) for (const command of windows ? windowsCommands(selection) : linuxCommands(selection)) console.log(`cargo ${command.join(" ")}`);
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
      for (const command of windows ? windowsCommands(selection) : linuxCommands(selection)) {
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
