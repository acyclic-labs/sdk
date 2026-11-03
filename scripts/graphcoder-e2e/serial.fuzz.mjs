import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { collect, collectPartitions, acceptanceBinding } from "./serial.mjs";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { readFileSync } from "node:fs";

const nativeFailure = JSON.parse(readFileSync(new URL("./fixtures/native-execution-failure.json", import.meta.url))).capture;

// These are generated internal receipts, never captured native tool output.
// Arbitrary input varies the identities; the original collector owns admission.
export function fuzz(data) {
  const cases = [{ id: "first" }, { id: "second" }], coverage = { pass: true };
  const hash = value => createHash("sha256").update(data).update(value).digest("hex");
  const binding = { binary: hash("binary"), mock: hash("mock"),
    executables: { NODE: hash("node"), E2E_BUN: hash("bun"), E2E_GIT_BINARY: hash("git") },
    cases: createHash("sha256").update(JSON.stringify(cases)).digest("hex"), source: { "serial.mjs": hash("source") } };
  const bundles = cases.map((item, index) => ({ index, count: 2, platform: "linux", binding,
    inventory: cases.map(item => item.id), selected: [item.id], coverage,
    receipts: [{ code: 0, execution: { code: 0, expired: false, cancelled: false, signal: null },
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
    value => value.executables.E2E_GIT_BINARY = hash("old Git"),
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
  for (const status of ["refused", "incomplete"]) {
    const ended = structuredClone(bundles), receipt = ended[1].receipts[0];
    receipt.code = nativeFailure.code;
    receipt.execution = nativeFailure;
    receipt.report.results[0].status = status;
    const report = collectPartitions(cases, ended, "linux", coverage, binding);
    assert.equal(report.pass, false);
    assert.equal(report.results[1].status, status, "retain the original non-success disposition");
    assert.deepEqual(report.suites[1].execution, nativeFailure);
  }
  // An original owner terminal remains authoritative even if a late report
  // and its outer process exit both claim success.
  for (const execution of [{ code: 0, signal: null, expired: true },
    { code: 0, signal: null, cancelled: true }, { code: 0, signal: "SIGTERM" },
    undefined, { signal: null }, nativeFailure, { ...nativeFailure, code: 0 }]) {
    const ended = structuredClone(bundles);
    ended[1].receipts[0].execution = execution;
    const report = collectPartitions(cases, ended, "linux", coverage, binding);
    assert.equal(report.pass, false, "original owner failure must fence a late passing report");
    assert.deepEqual(report.suites[1].execution, execution);
    assert.equal(report.results[1].status, "fail");
  }
  // Generated internal suite metadata is checked by the same original collector.
  {
    const platform = ["win32", "linux", "darwin"][(data[0] ?? 0) % 3];
    const id = "case:" + Buffer.from(data).toString("hex");
    const cases = [{ id, ntfs: true }], coverage = { pass: true };
    const ids = platform === "win32" ? [id, id + "-ntfs"] : [id];
    // Each filesystem variant is its own suite and so owns its own envelope.
    const receipts = ids.map(id => ({ code: 0, execution: { code: 0, signal: null },
      report: { platform, selected: [id],
      qualification: false, coverage, seconds: 0.2, limit_seconds: 10,
      results: [{ case: id, platform, status: "pass" }] } }));
    assert.equal(collect(cases, receipts, platform, coverage).pass, true);
    assert.throws(() => collect(cases, [], platform, coverage), /every case variant/);
    if (platform === "win32") {
      const shared = structuredClone(receipts[0]);
      shared.report.results.push({ case: id + "-ntfs", platform, status: "pass" });
      assert.throws(() => collect(cases, [shared], platform, coverage), /every case variant/,
        "variants sharing one suite envelope cannot qualify");
    }
    for (const edit of [report => report.results.pop(), report => report.results[0].platform = "foreign", report => report.results.push(report.results[0]),
      report => report.platform = "foreign", report => report.selected = [],
      report => report.qualification = true, report => report.coverage = { pass: false },
      report => report.limit_seconds = 11, report => report.seconds = null]) {
      const changed = structuredClone(receipts); edit(changed[0].report);
      assert.equal(collect(cases, changed, platform, coverage).pass, false);
    }
    for (const seconds of [undefined, null, 0, -1, "0.2", Infinity, NaN]) {
      const changed = structuredClone(receipts); changed[0].report.seconds = seconds;
      assert.equal(collect(cases, changed, platform, coverage).pass, false);
    }
    for (const edit of [receipt => receipt.code = 1, receipt => receipt.report.seconds = 10,
      receipt => receipt.report.results[0].status = "fail"]) {
      const changed = structuredClone(receipts); edit(changed[0]);
      assert.equal(collect(cases, changed, platform, coverage).pass, false);
    }
    const incomplete = { pass: false }, changed = structuredClone(receipts);
    changed[0].report.coverage = incomplete;
    assert.equal(collect(cases, changed, platform, incomplete).pass, false);
  }
}

// Generated identity inputs exercise the original source/file admission. They
// are never launched as native binaries or claimed as captured tool output.
export async function fuzzAdmission(data) {
 const root=await mkdtemp(join(tmpdir(),'acceptance-binding-'));
 try {
  const directory=join(root,'scripts/graphcoder-e2e'),hosts=join(root,'plugin/tests/hosts');
  for(const path of [directory,hosts,join(root,'scripts/graphcoder-pty')])await mkdir(path,{recursive:true});
  const binary=join(root,'graphcoder'),mock=join(root,'mock'),plugin=join(root,'plugin-bin'),codex=join(root,'codex');
  for(const path of [binary,mock,plugin,codex])await writeFile(path,'original');
  const pluginBytes=Buffer.alloc(2*1024*1024+1,data[0]??0x37);
  await writeFile(plugin,pluginBytes);
  const helper=join(hosts,'qualify-scripted.mjs');await writeFile(helper,'original helper');
  // These identity-only inputs refer to the actual running executable; no
  // invented native-tool output or executable image is needed for the check.
  const env={E2E_PLUGIN_BINARY:plugin,E2E_CODEX:codex,E2E_BUN:process.execPath,
    E2E_PROCESS_HOST:process.execPath,E2E_CONPTY_HOST:process.execPath,E2E_GIT_BINARY:process.execPath};
  const before=await acceptanceBinding(directory,binary,mock,[],env);
  assert.equal(before.executables.E2E_PLUGIN_BINARY,createHash('sha256').update(pluginBytes).digest('hex'));
  assert.equal(before.source['file-digest.mjs'],createHash('sha256')
    .update(await readFile(new URL('../file-digest.mjs',import.meta.url))).digest('hex'));
  assert.match(before.executables.NODE,/^[a-f0-9]{64}$/);
  for(const name of ['E2E_BUN','E2E_PROCESS_HOST','E2E_CONPTY_HOST','E2E_GIT_BINARY'])
    assert.equal(before.executables[name],before.executables.NODE,`${name} must bind its admitted executable`);
  pluginBytes[pluginBytes.length-1]^=1;
  await writeFile(plugin,pluginBytes);
  const changedPlugin=await acceptanceBinding(directory,binary,mock,[],env);
  assert.notDeepEqual(changedPlugin,before,'mixed plugin binaries must not share provenance');
  await writeFile(codex,'different native host');
  const changedHost=await acceptanceBinding(directory,binary,mock,[],env);
  assert.notDeepEqual(changedHost,changedPlugin,'mixed native hosts must not share provenance');
  await writeFile(helper,'different helper');
  assert.notDeepEqual(await acceptanceBinding(directory,binary,mock,[],env),changedHost,'mixed host helpers must not share provenance');
  await rm(plugin);
  await assert.rejects(acceptanceBinding(directory,binary,mock,[],env),{code:'ENOENT'});
 }finally{await rm(root,{recursive:true,force:true});}
}
