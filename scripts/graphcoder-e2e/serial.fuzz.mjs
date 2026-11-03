import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { collectPartitions } from "./serial.mjs";

// These are generated internal receipts, never captured native tool output.
// Arbitrary input varies the identities; the original collector owns admission.
export function fuzz(data) {
  const cases = [{ id: "first" }, { id: "second" }], coverage = { pass: true };
  const hash = value => createHash("sha256").update(data).update(value).digest("hex");
  const binding = { binary: hash("binary"), mock: hash("mock"),
    executables: { NODE: hash("node"), E2E_BUN: hash("bun") },
    cases: createHash("sha256").update(JSON.stringify(cases)).digest("hex"), source: { "serial.mjs": hash("source") } };
  const bundles = cases.map((item, index) => ({ index, count: 2, platform: "linux", binding,
    inventory: cases.map(item => item.id), selected: [item.id], coverage,
    receipts: [{ code: 0, execution: { expired: false, cancelled: false, signal: null },
      report: { platform: "linux", selected: [item.id], qualification: false,
      coverage, seconds: 0.1, limit_seconds: 10,
      results: [{ case: item.id, platform: "linux", status: "pass" }] } }] }));
  const report = collectPartitions(cases, [...bundles].reverse(), "linux", coverage, binding);
  assert.equal(report.pass, true);
  assert.deepEqual(JSON.parse(JSON.stringify(report)).binding, binding);
  assert.throws(() => collectPartitions(cases, bundles, "linux", coverage), /expected acceptance identity/);
  // Equal historical receipts are still stale against the intended source.
  for (const edit of [value => value.binary = hash("old binary"),
    value => value.mock = hash("old mock"), value => value.source["serial.mjs"] = hash("old source"),
    value => value.executables.NODE = hash("old Node"),
    value => value.executables.E2E_BUN = hash("old Bun"),
    value => value.hosts = { pi: { entry: hash("old entry") } }]) {
    const stale = bundles.map(bundle => structuredClone(bundle));
    for (const bundle of stale) edit(bundle.binding);
    assert.throws(() => collectPartitions(cases, stale, "linux", coverage, binding),
      /expected acceptance identity/);
  }
  assert.throws(() => collectPartitions(cases, [bundles[0]], "linux", coverage, binding), /every partition/);
  for (const edit of [bundle => bundle.index = 0, bundle => bundle.count = 3,
    bundle => bundle.platform = "win32", bundle => bundle.binding.binary = hash("changed"),
    bundle => bundle.binding.source["serial.mjs"] = hash("changed"),
    bundle => bundle.binding.executables.E2E_BUN = hash("changed"),
    bundle => bundle.inventory.reverse(), bundle => bundle.selected = ["first"],
    bundle => bundle.receipts = [], bundle => bundle.coverage = { pass: false }]) {
    // Give each bundle its own binding, as real receipt files do.
    const changed = bundles.map(bundle => structuredClone(bundle)); edit(changed[1]);
    assert.throws(() => collectPartitions(cases, changed, "linux", coverage, binding));
  }
  const changed = structuredClone(cases); changed[0].changed = true;
  assert.throws(() => collectPartitions(changed, bundles, "linux", coverage, binding), /definitions/);
  const missing = bundles.map(bundle => structuredClone(bundle)); delete missing[0].binding;
  assert.throws(() => collectPartitions(cases, missing, "linux", coverage, binding), /identity/);
  const missingRuntime = bundles.map(bundle => structuredClone(bundle));
  for (const bundle of missingRuntime) delete bundle.binding.executables.NODE;
  assert.throws(() => collectPartitions(cases, missingRuntime, "linux", coverage, binding), /Node identity/);
  // Agreement between partitions cannot turn absent provenance into a digest.
  // Both bundles carry the same malformed value, so equality alone accepts it.
  for (const source of [[], [hash("source")], { "serial.mjs": null },
    { "serial.mjs": true }, { "serial.mjs": "not-a-sha256" }, { "": hash("source") }]) {
    const invalid = bundles.map(bundle => structuredClone(bundle));
    for (const bundle of invalid) bundle.binding.source = source;
    assert.throws(() => collectPartitions(cases, invalid, "linux", coverage, binding), /acceptance source/);
  }
  const red = structuredClone(bundles); red[1].receipts[0].code = 1;
  assert.equal(collectPartitions(cases, red, "linux", coverage, binding).pass, false);
  // An original owner terminal remains authoritative even if a late report
  // and its outer process exit both claim success.
  for (const execution of [{ expired: true }, { cancelled: true }, { signal: "SIGTERM" }]) {
    const ended = structuredClone(bundles);
    ended[1].receipts[0].execution = execution;
    const report = collectPartitions(cases, ended, "linux", coverage, binding);
    assert.equal(report.pass, false, "original owner failure must fence a late passing report");
    assert.deepEqual(report.suites[1].execution, execution);
    assert.equal(report.results[1].status, "fail");
  }
}
