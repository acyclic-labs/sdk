import assert from "node:assert/strict";
import test from "node:test";
import { selectCargoFeatures, cargoFeatureArguments } from "./cargo-feature-selection.mjs";
import { clippySelections } from "./clippy-feature-sets.mjs";
import { localCargoPackages } from "./publish-cargo-crates.mjs";
import { featureSets, featureWorker, linuxCommands, windowsCommands, selectFeaturePackages } from "./check-feature-matrix.mjs";

test("feature package selection refuses missing members and empty inventories", () => {
  const items = [{ name: "alpha" }, { name: "beta" }];
  assert.deepEqual(selectFeaturePackages(items, []), items);
  assert.deepEqual(selectFeaturePackages(items, ["beta"]), [items[1]]);
  for (const selected of [["missing"], ["alpha", "missing"], [""]]) {
    assert.throws(() => selectFeaturePackages(items, selected), /STEP_FAILED feature-selection: unknown packages.*recovery: node scripts\/check-feature-matrix\.mjs --list/);
  }
  assert.throws(() => selectFeaturePackages([], []), /STEP_FAILED feature-selection: empty package inventory/);
});

const selection = (item, args) => selectCargoFeatures(new Map([["crate", { ...item, name: "crate" }]]), ["crate"], args.slice(5));

const sets = features => featureSets({ manifest_path: "c/Cargo.toml", features }, true)
  .map(args => args.slice(5).join(" ") || "(defaults)");

test("Linux checks test cfgs without losing consumer library isolation", () => {
  const item = { manifest_path: "c/Cargo.toml", features: { default: ["distributed"], distributed: [], browser: [] } };
  const requireTestCfg = commands => {
    assert.ok(commands.some(args => args.includes("--all-targets") && args.includes("--no-default-features") && !args.includes("--features")),
      "no-default test cfg must compile before a feature matrix can pass");
  };
  for (const args of featureSets(item, true)) {
    const commands = linuxCommands(selection(item, args));
    assert.deepEqual(commands.map(command => command.slice(0, 4)), [args.slice(0, 4), args.slice(0, 4)]);
    assert.deepEqual(commands.map(command => command[4]), ["--lib", "--all-targets"]);
    for (const command of commands) assert.deepEqual(command.slice(5), args.slice(5));
  }
  const args = featureSets(item, true)[0];
  requireTestCfg(linuxCommands(selection(item, args)));
  // Actual previous command selection: isolated --lib alone misses cfg(test).
  assert.throws(() => requireTestCfg([args]), /no-default test cfg must compile/);
});

test("MSVC checks isolated libraries and all targets for the identical feature matrix", () => {
  const item = { manifest_path: "c/Cargo.toml", features: { default: ["a"], a: [], b: [], c: ["dep:d"] } };
  for (const args of featureSets(item, true)) {
    const commands = windowsCommands(selection(item, args));
    assert.deepEqual(commands.map(command => command.slice(0, 7)), Array(2).fill([
      "xwin", "check", "--locked", "--manifest-path", "c/Cargo.toml", "--target", "x86_64-pc-windows-msvc",
    ]));
    assert.deepEqual(commands.map(command => command[7]), ["--lib", "--all-targets"]);
    for (const command of commands) assert.deepEqual(command.slice(8), args.slice(5));
  }
});

test("checks every distinct feature selection once", () => {
  assert.deepEqual(sets({ default: ["a"], a: [], b: ["dep:x"] }), [
    "--no-default-features",
    "(defaults)",
    "--all-features",
    "--no-default-features --features a",
    "--no-default-features --features b",
  ]);
});

test("drops a lone feature that selects what --all-features selects", () => {
  assert.deepEqual(sets({ a: ["b"], b: [] }), [
    "--no-default-features",
    "--all-features",
    "--no-default-features --features b",
  ]);
  // Negative control: with a third feature, the forwarding feature alone is a distinct selection.
  assert.deepEqual(sets({ a: ["b"], b: [], c: [] }).length, 5);
});

test("a crate without features is checked once", () => {
  assert.deepEqual(sets({}), ["--no-default-features"]);
});

test("the default matrix checks each crate once with --all-features; the full matrix is explicit", () => {
  const item = { manifest_path: "c/Cargo.toml", features: { default: ["a"], a: [], b: [] } };
  assert.deepEqual(featureSets(item), [["check", "--locked", "--manifest-path", "c/Cargo.toml", "--lib", "--all-features"]]);
  assert.deepEqual(featureSets({ ...item, features: {} }), featureSets(item));
  // Every selected configuration compiles its test cfg too.
  assert.deepEqual(linuxCommands(selection(item, featureSets(item)[0])).map(command => command[4]), ["--lib", "--all-targets"]);
  assert.equal(featureSets(item, true).length, 5);
});

test("PR and full matrices reuse the same worker for their shared sets", () => {
  const item = { name: "crate", manifest_path: "/main/crate/Cargo.toml", features: { default: ["a"], a: [], b: [], c: [] } };
  const full = featureSets(item, true).map(args => [args.slice(5).join(" "), featureWorker(item.name, args, 6)]);
  for (const args of featureSets({ ...item, manifest_path: "/pr/crate/Cargo.toml" })) {
    assert.equal(featureWorker(item.name, args, 6), full.find(([key]) => key === args.slice(5).join(" "))[1]);
  }
  // Inserting another crate or feature cannot renumber existing sets' caches.
  const expanded = featureSets({ ...item, features: { ...item.features, extra: [] } }, true);
  for (const args of expanded) {
    const previous = full.find(([key]) => key === args.slice(5).join(" "));
    if (previous) assert.equal(featureWorker(item.name, args, 6), previous[1]);
  }
});

test("shipped feature admission uses actual defining packages and preserves independent target cfgs", () => {
  // Original locked Cargo output, not a handwritten metadata fixture. Mutate
  // only the admitted package boundary for the refusal controls below.
  const inventory = new Map(localCargoPackages().map(item => [item.name, item]));
  const shipped = clippySelections(inventory);
  for (const { selection, targets } of shipped) {
    const args = cargoFeatureArguments(selection, { targets });
    for (const item of selection.packages) {
      assert.ok(args.includes(item.name));
      assert.equal(selection.roots.find(root => root.id === item.id)?.id, item.id);
    }
    assert.deepEqual(args.slice(-selection.flags.length || args.length), selection.flags);
  }

  const fs = inventory.get("acyclic-fs");
  const defaults = selectCargoFeatures(inventory, [fs.name], []);
  assert.ok(defaults.roots[0].features.includes("default"));
  // The full matrix must not erase cfg(feature = "default") by treating it
  // as only an alias for the features it forwards.
  const expanded = featureSets(fs, true).map(args => selectCargoFeatures(inventory, [fs.name], args.slice(5)));
  assert.ok(expanded.some(selection => !selection.roots[0].features.includes("default")));
  assert.ok(expanded.some(selection => selection.roots[0].features.includes("default")));
  const commands = linuxCommands(defaults);
  assert.notDeepEqual(commands[0], commands[1]);
  assert.deepEqual(commands.map(args => args[4]), ["--lib", "--all-targets"]);
  const msvc = windowsCommands(defaults);
  assert.ok(msvc.every(args => args.includes("x86_64-pc-windows-msvc")));
  assert.ok(commands.every(args => !args.includes("--target")));
  const proofTarget = cargoFeatureArguments(defaults, { manifest: true, target: "wasm32-unknown-unknown", targets: "lib" });
  assert.ok(proofTarget.includes("wasm32-unknown-unknown"));
  assert.notDeepEqual(proofTarget, commands[0].slice(2));

  const missing = new Map(inventory);
  missing.delete(fs.name);
  assert.throws(() => clippySelections(missing), /STEP_FAILED cargo-feature-selection: unknown package acyclic-fs/);
  const renamed = new Map(inventory);
  const features = { ...fs.features };
  delete features["native-watch"];
  renamed.set(fs.name, { ...fs, features });
  // A feature on another root cannot supply the defining crate's cfg.
  const stream = inventory.get("acyclic-stream");
  renamed.set(stream.name, { ...stream, features: { ...stream.features, "native-watch": [] } });
  assert.throws(() => clippySelections(renamed), /unknown selected-root feature native-watch/);
  assert.throws(() => selectCargoFeatures(inventory, [fs.name], ["--features"]), /missing feature names/);
  assert.throws(() => selectCargoFeatures(inventory, [fs.name], ["--features", `${stream.name}/local`]), /unknown selected-root feature/);
  // Neither cfg(test) nor a dev dependency can admit an unknown root feature.
  assert.throws(() => selectCargoFeatures(inventory, [fs.name], ["--features", "not-a-defined-root-feature"]), /unknown selected-root feature/);
});
