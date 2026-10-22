import assert from "node:assert/strict";
import test from "node:test";
import { featureSets, featureWorker } from "./check-feature-matrix.mjs";

const sets = features => featureSets({ manifest_path: "c/Cargo.toml", features })
  .map(args => args.slice(5).join(" ") || "(defaults)");

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

test("each feature alone is added only on request", () => {
  const item = { manifest_path: "c/Cargo.toml", features: { default: ["a"], a: [], b: [] } };
  assert.equal(featureSets(item, false).length, 3);
  assert.equal(featureSets(item).length, 4);
});

test("PR and full matrices reuse the same worker for their shared sets", () => {
  const item = { name: "crate", manifest_path: "/main/crate/Cargo.toml", features: { default: ["a"], a: [], b: [], c: [] } };
  const full = featureSets(item).map(args => [args.slice(5).join(" "), featureWorker(item.name, args, 6)]);
  for (const args of featureSets({ ...item, manifest_path: "/pr/crate/Cargo.toml" }, false)) {
    assert.equal(featureWorker(item.name, args, 6), full.find(([key]) => key === args.slice(5).join(" "))[1]);
  }
  // Inserting another crate or feature cannot renumber existing sets' caches.
  const expanded = featureSets({ ...item, features: { ...item.features, extra: [] } });
  for (const args of expanded) {
    const previous = full.find(([key]) => key === args.slice(5).join(" "));
    if (previous) assert.equal(featureWorker(item.name, args, 6), previous[1]);
  }
});
