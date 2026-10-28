import assert from "node:assert/strict";
import test from "node:test";
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

const sets = features => featureSets({ manifest_path: "c/Cargo.toml", features }, true)
  .map(args => args.slice(5).join(" ") || "(defaults)");

test("Linux checks test cfgs without losing consumer library isolation", () => {
  const item = { manifest_path: "c/Cargo.toml", features: { default: ["distributed"], distributed: [], browser: [] } };
  const requireTestCfg = commands => {
    assert.ok(commands.some(args => args.includes("--all-targets") && args.includes("--no-default-features") && !args.includes("--features")),
      "no-default test cfg must compile before a feature matrix can pass");
  };
  for (const args of featureSets(item, true)) {
    const commands = linuxCommands(args);
    assert.deepEqual(commands.map(command => command.slice(0, 4)), [args.slice(0, 4), args.slice(0, 4)]);
    assert.deepEqual(commands.map(command => command[4]), ["--lib", "--all-targets"]);
    for (const command of commands) assert.deepEqual(command.slice(5), args.slice(5));
  }
  const args = featureSets(item, true)[0];
  requireTestCfg(linuxCommands(args));
  // Actual previous command selection: isolated --lib alone misses cfg(test).
  assert.throws(() => requireTestCfg([args]), /no-default test cfg must compile/);
});

test("MSVC checks isolated libraries and all targets for the identical feature matrix", () => {
  for (const args of featureSets({manifest_path: "c/Cargo.toml", features: {default: ["a"], a: [], b: [], c: ["dep:d"]}}, true)) {
    const commands = windowsCommands(args);
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

test("the gate checks each crate once with --all-features; the full matrix is explicit", () => {
  const item = { manifest_path: "c/Cargo.toml", features: { default: ["a"], a: [], b: [] } };
  assert.deepEqual(featureSets(item), [["check", "--locked", "--manifest-path", "c/Cargo.toml", "--lib", "--all-features"]]);
  assert.deepEqual(featureSets({ ...item, features: {} }), featureSets(item));
  // Every gated selection compiles its test cfg too.
  assert.deepEqual(linuxCommands(featureSets(item)[0]).map(command => command[4]), ["--lib", "--all-targets"]);
  assert.equal(featureSets(item, true).length, 4);
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
