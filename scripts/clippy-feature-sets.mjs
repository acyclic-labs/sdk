// Lints, with warnings denied, every crate feature set the SDK ships,
// documents, or builds against. The workspace-wide --all-features run covers
// the union; these are the reduced sets it cannot see. Clippy type-checks
// every target it lints, so a set that does not build fails here too.
//
// Clippy lints every workspace member a selection builds, in the features
// Cargo unifies for that selection. Each member's own selections (no default
// features, its defaults and every feature, library and all targets) are
// resolved by `cargo tree`, so each feature set a dependent reaches on another
// member is linted by at least one run. A selection runs only when it reaches
// a (member, features) unit no earlier run lints.
//
// Libraries are linted in every such set. Their tests are linted wherever the
// set includes the in-memory backend the test suites run on: a Cargo test
// build unifies dev-dependency features, so only a library-only run proves
// that the library itself builds without them.
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { localCargoPackages } from "./publish-cargo-crates.mjs";
import { selectCargoFeatures, cargoFeatureArguments } from "./cargo-feature-selection.mjs";
import { runCommand } from "./run-command.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const native = ["acyclic-fs", "acyclic-objects", "acyclic-stream", "acyclic-harness",
  "acyclic-harness-core", "acyclic-harness-search"].map(name => ["-p", name, "--no-default-features"]);

// Library and tests.
const withTests = [
  ...native,
  // Inference without its gRPC host, as the browser build uses it.
  ["-p", "acyclic-inference", "--no-default-features"],
  // The conformance runner and its native-mount qualification binaries.
  ["-p", "acyclic-conformance"],
  ["-p", "acyclic-conformance", "--features", "local-runner"],
];

// Library only: each acyclic-fs capability on its own.
const libraryOnly = [
  ...native,
  ["-p", "acyclic-native-runtime", "--features", "test-support"],
];

// Explicit shipped combinations share defining-package admission with the
// generated matrix. They remain independent compiler invocations, and run
// before the derived selections.
function explicitSelections(inventory) {
  return [...withTests.map(set => ({ set, targets: "all-targets" })), ...libraryOnly.map(set => ({ set, targets: undefined }))]
    .map(({ set, targets }) => {
      const names = [];
      let index = 0;
      while (set[index] === "-p") { names.push(set[index + 1]); index += 2; }
      return { selection: selectCargoFeatures(inventory, names, set.slice(index)), targets };
    });
}

/** The units one selection lints: each member it builds, keyed by its unified
 * features, and the selected packages' tests when it lints all targets.
 * @param {(selection: any, targets: string | undefined) => string[]} units */
export function clippySelections(inventory, units = () => []) {
  const linted = new Set();
  const chosen = [];
  const consider = (candidate, always) => {
    const reached = units(candidate.selection, candidate.targets);
    if (!always && reached.every(unit => linted.has(unit))) return;
    for (const unit of reached) linted.add(unit);
    chosen.push({ ...candidate, units: reached });
  };
  for (const candidate of explicitSelections(inventory)) consider(candidate, true);
  const members = [...inventory.values()].filter(item => item.workspace_member).sort((a, b) => a.name.localeCompare(b.name));
  for (const flags of [["--all-features"], [], ["--no-default-features"]]) {
    for (const targets of ["all-targets", undefined]) {
      for (const item of members) consider({ selection: selectCargoFeatures(inventory, [item.name], passingFlags(item, flags)), targets }, false);
    }
  }
  return chosen;
}

/** A manifest names the features that exist to fail compilation under
 * `[package.metadata.acyclic] compiler-negative-features`; their own checker
 * expects that failure, so the passing matrix never enables them. */
function passingFlags(item, flags) {
  const negative = new Set(item.metadata?.acyclic?.["compiler-negative-features"] ?? []);
  if (flags[0] !== "--all-features" || negative.size === 0) return flags;
  const passing = Object.keys(item.features ?? {}).filter(name => name !== "default" && !negative.has(name));
  return passing.length ? ["--features", passing.join(",")] : [];
}

/** Parses `cargo tree --prefix none --format "{p}|{f}"` into member units. */
export function treeUnits(stdout, members, selected, targets) {
  const units = new Set();
  for (const line of stdout.split(/\r?\n/u)) {
    const match = /^(\S+) v\S+[^|]*\|([^ ]*)/u.exec(line);
    if (!match || !members.has(match[1])) continue;
    const features = match[2].split(",").filter(Boolean).sort().join(",");
    units.add(`${match[1]}|${features}`);
    if (targets === "all-targets" && selected.includes(match[1])) units.add(`${match[1]}|${features}|tests`);
  }
  return [...units].sort();
}

/** Cargo's own feature unification for one selection on the host. */
export function cargoTreeUnits(inventory) {
  const members = new Set([...inventory.values()].filter(item => item.workspace_member).map(item => item.name));
  return (selection, targets) => {
    const edges = targets === "all-targets" ? "normal,build,dev" : "normal,build";
    const result = spawnSync("cargo", ["tree", "--locked", "-e", edges, "--prefix", "none", "--format", "{p}|{f}",
      ...cargoFeatureArguments(selection)], { cwd: root, encoding: "utf8", maxBuffer: Infinity });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`STEP_FAILED clippy-feature-units: cargo tree ${cargoFeatureArguments(selection).join(" ")} exited ${result.status}: ${result.stderr}; recovery: node scripts/clippy-feature-sets.mjs --list`);
    }
    return treeUnits(result.stdout, members, selection.packages.map(item => item.name), targets);
  };
}

async function main() {
  // Each extra worker gets its own target directory; Cargo owns cache freshness.
  const list = process.argv[2] === "--list";
  const workers = list ? 1 : Number(process.argv[2] ?? 1);
  if (process.argv.length > 3 || !Number.isSafeInteger(workers) || workers < 1) {
    throw new Error("STEP_FAILED clippy-workers: WORKERS must be a positive safe integer; recovery: node scripts/clippy-feature-sets.mjs 1");
  }
  const inventory = new Map(localCargoPackages().map(item => [item.name, item]));
  const sets = clippySelections(inventory, cargoTreeUnits(inventory));
  if (list) {
    for (const set of sets) console.log(`cargo clippy ${cargoFeatureArguments(set.selection, { targets: set.targets }).join(" ")} units=${set.units.join(" ")}`);
    return;
  }
  let failed = 0;

  async function worker(index) {
    const target = `${process.env.CARGO_TARGET_DIR ?? "target"}-clippy-${index}`;
    const env = index === 0 ? process.env : { ...process.env, CARGO_TARGET_DIR: target };
    for (let set = sets.shift(); set && !failed; set = sets.shift()) {
      const command = ["clippy", ...cargoFeatureArguments(set.selection, { targets: set.targets }), "--locked", "--", "-D", "warnings"];
      process.stderr.write(`cargo ${command.join(" ")}\n`);
      const status = await runCommand("cargo", command, { cwd: root, env });
      if (status !== 0) failed ||= status;
    }
  }

  await Promise.all(Array.from({ length: workers }, (_, index) => worker(index)));
  process.exitCode = failed;
}

if (import.meta.main) await main();
