import assert from "node:assert/strict";
import test from "node:test";
import { createHash } from "node:crypto";
import { collect, runSuites, partition, collectPartitions,acceptanceBinding, variants } from "./serial.mjs";
import {mkdtemp,mkdir,readFile,writeFile,rename,rm} from 'node:fs/promises';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {nativeHost} from '../../plugin/tests/hosts/native-host.mjs';
import {installedHost} from '../../plugin/tests/hosts/native-model.mjs';
import {Processes} from './process.mjs';
import {tmpdir} from 'node:os';
import { schedule } from "./process.mjs";

test('installed host admission launches package entries and binds their actual closure',async()=>{
 const directory=dirname(fileURLToPath(import.meta.url)),scope=new Processes();
 const cases=['codex','claude-code','pi'].map(host=>({inputs:[{format:'installed-host',host}]}));
 let entry,dependency,retained;
 try {
  for(const {inputs:[{host}]} of cases) {
   const native=await nativeHost(host,{});
   const result=await scope.run(native.program,[...native.args,'--version']);
   assert.equal(result.code,0,result.stderr);
   assert.ok(result.stdout.trim(),'actual host version must be nonempty');
  }
  ({entry}=await nativeHost('pi',{}));
  const env={E2E_PI:entry,E2E_CODEX:(await nativeHost('codex',{})).entry};
  const before=await acceptanceBinding(directory,process.execPath,process.execPath,cases,env);
  assert.ok(Object.values(before.hosts).every(host=>host.closure),'every override must bind its dependency tree');
  // The pinned Pi 1.1.0 dist/bundle/cli.js loads this real module through
  // createRequire(import.meta.url)("./cli-runtime.js"). Replace the directory
  // entry rather than modifying an inode shared with another installation.
  dependency=join(dirname(entry),'cli-runtime.js');
  const original=await readFile(dependency);
  retained=dependency+'.retained';await rename(dependency,retained);
  await writeFile(dependency,Buffer.concat([original,Buffer.from('\n')]));
  const changed=await acceptanceBinding(directory,process.execPath,process.execPath,cases,env);
  assert.deepEqual(changed.hosts,before.hosts,'the executable entries did not change');
  assert.notDeepEqual(changed,before,'changed imported bytes must fence partition composition');
  await rename(entry,entry+'.withdrawn');
  try {await assert.rejects(nativeHost('pi',{}),/step=admit E2E_PI/);}
  finally {await rename(entry+'.withdrawn',entry);}
  await assert.rejects(nativeHost('pi',{E2E_PI:process.execPath}),/step=admit E2E_PI/);
  await assert.rejects(nativeHost('pi',{E2E_PI:dependency}),/declared package bin/);
  // Pinned Pi's actual unsupported provider must refuse before staging any
  // package, creating workspace files or starting the original process scope.
  await assert.rejects(installedHost({host:'pi',fixture:{env},
   processes:{runOwned(){assert.fail('unsupported Pi started a process');}},
   capture(){assert.fail('unsupported Pi produced a process result');}}),
   error=>error.code==='UNSUPPORTED_NATIVE_KEYLESS_PROVIDER');
 } finally {
  if(retained) {await rm(dependency,{force:true});await rename(retained,dependency);}
  await scope.close();
 }
});

test('partition provenance includes plugin executables and host helper source',async()=>{
 const root=await mkdtemp(join(tmpdir(),'acceptance-binding-'));
 try {
  const directory=join(root,'scripts/graphcoder-e2e'),hosts=join(root,'plugin/tests/hosts');
  for(const path of [directory,hosts,join(root,'scripts/graphcoder-pty')])await mkdir(path,{recursive:true});
  const binary=join(root,'graphcoder'),mock=join(root,'mock'),plugin=join(root,'plugin-bin'),codex=join(root,'codex');
  for(const path of [binary,mock,plugin,codex])await writeFile(path,'original');
  const helper=join(hosts,'qualify-scripted.mjs');await writeFile(helper,'original helper');
  // These identity-only inputs refer to the actual running executable; no
  // invented native-tool output or executable image is needed for the check.
  const env={E2E_PLUGIN_BINARY:plugin,E2E_CODEX:codex,E2E_BUN:process.execPath,
    E2E_PROCESS_HOST:process.execPath,E2E_CONPTY_HOST:process.execPath};
  const before=await acceptanceBinding(directory,binary,mock,[],env);
  assert.match(before.executables.NODE,/^[a-f0-9]{64}$/);
  for(const name of ['E2E_BUN','E2E_PROCESS_HOST','E2E_CONPTY_HOST'])
    assert.equal(before.executables[name],before.executables.NODE,`${name} must bind its admitted executable`);
  await writeFile(plugin,'different plugin');
  const changedPlugin=await acceptanceBinding(directory,binary,mock,[],env);
  assert.notDeepEqual(changedPlugin,before,'mixed plugin binaries must not share provenance');
  await writeFile(codex,'different native host');
  const changedHost=await acceptanceBinding(directory,binary,mock,[],env);
  assert.notDeepEqual(changedHost,changedPlugin,'mixed native hosts must not share provenance');
  await writeFile(helper,'different helper');
  assert.notDeepEqual(await acceptanceBinding(directory,binary,mock,[],env),changedHost,'mixed host helpers must not share provenance');
 }finally{await rm(root,{recursive:true,force:true});}
});

test("Windows starts the next case only after the previous case has exited", async () => {
  for (const platform of ["win32", "linux", "darwin"]) {
    const releases = [Promise.withResolvers(), Promise.withResolvers()], started = [];
    const running = schedule([0, 1], platform, async index => {
      started.push(index);
      await releases[index].promise;
      return index;
    });
    assert.deepEqual(started, platform === "win32" ? [0] : [0, 1]);
    for (const release of releases) release.resolve();
    assert.deepEqual(await running, [0, 1]);
  }
});

test("serial qualification requires every OS case, volume variant, exit and bounded suite", () => {
  const cases = [{ id: "case", ntfs: true }], coverage = { pass: true };
  assert.deepEqual(variants(cases, "win32").map(test => test.id), ["case", "case-ntfs"]);
  assert.deepEqual(variants(cases, "linux").map(test => test.id), ["case"]);
  // Each filesystem variant is its own suite and so owns its own envelope.
  const receipts = ["case", "case-ntfs"].map(id => ({ code: 0, report: { platform: "win32", selected: [id],
    qualification: false, coverage, seconds: 0.2, limit_seconds: 10,
    results: [{ case: id, platform: "win32", status: "pass" }] } }));
  assert.equal(collect(cases, receipts, "win32", coverage).pass, true);
  assert.throws(() => collect(cases, [], "win32", coverage), /every case variant/);
  const shared = structuredClone(receipts[0]);
  shared.report.results.push({ case: "case-ntfs", platform: "win32", status: "pass" });
  assert.throws(() => collect(cases, [shared], "win32", coverage), /every case variant/,
    "variants sharing one suite envelope cannot qualify");
  for (const edit of [report => report.results.pop(), report => report.results.push(report.results[0]),
    report => report.platform = "linux", report => report.selected = [],
    report => report.qualification = true, report => report.coverage = { pass: false },
    report => report.limit_seconds = 11, report => report.seconds = null]) {
    const changed = structuredClone(receipts); edit(changed[0].report);
    assert.equal(collect(cases, changed, "win32", coverage).pass, false);
  }
  for (const edit of [receipt => receipt.code = 1, receipt => receipt.report.seconds = 10,
    receipt => receipt.report.results[0].status = "fail"]) {
    const changed = structuredClone(receipts); edit(changed[0]);
    assert.equal(collect(cases, changed, "win32", coverage).pass, false);
  }
  const incomplete = { pass: false }, changed = structuredClone(receipts);
  changed[0].report.coverage = incomplete;
  assert.equal(collect(cases, changed, "win32", incomplete).pass, false);
});

test("a missing or malformed suite receipt cannot stop later cases or erase the aggregate", async () => {
  const cases = ["missing", "malformed", "later"].map(id => ({ id }));
  const launched = [];
  const receipts = await runSuites(cases, async test => {
    launched.push(test.id);
    return { code: test.id === "missing" ? 1 : 0,
      stdout: `${test.id}: complete capture`, stderr: `${test.id}: diagnostic` };
  }, async test => {
    if (test.id === "missing") throw new Error("ENOENT: missing matrix.json");
    if (test.id === "malformed") return JSON.parse("{");
    return { platform: "linux", selected: [test.id], qualification: false,
      coverage: { pass: true }, seconds: 0.1, limit_seconds: 10,
      results: [{ case: test.id, platform: "linux", status: "pass" }] };
  });
  assert.deepEqual(launched, ["missing", "malformed", "later"]);
  const report = collect(cases, receipts, "linux", { pass: true });
  assert.equal(report.pass, false);
  assert.deepEqual(report.results.map(result => result.status), ["fail", "fail", "pass"]);
  assert.match(report.results[0].error, /ENOENT/);
  assert.equal(report.suites.length, 3);
  assert.equal(report.suites[0].execution.stdout, "missing: complete capture");
  assert.equal(report.suites[1].execution.stderr, "malformed: diagnostic");
  assert.doesNotThrow(() => JSON.parse(JSON.stringify(report)));
});

test("partitions preserve the complete ordered inventory without empty or overlapping work", () => {
  const cases = Array.from({ length: 5 }, (_, index) => ({ id: String(index) }));
  assert.deepEqual(partition(cases).cases, cases);
  for (const count of [1, 2, 3, 5]) {
    const parts = Array.from({ length: count }, (_, index) => partition(cases, `${index}/${count}`));
    assert.deepEqual(parts.flatMap(part => part.cases), cases);
    assert.ok(parts.every(part => part.cases.length > 0));
  }
  for (const value of ["1/1", "0/0", "0/6", "-1/2", "0.5/2", "0/9007199254740992"])
    assert.throws(() => partition(cases, value));
});

test("partition collection fences runtime, source, definitions and exact case receipts", () => {
  const cases = [{ id: "first" }, { id: "second" }], coverage = { pass: true };
  const hash = value => createHash("sha256").update(value).digest("hex");
  const binding = { binary: hash("binary"), mock: hash("mock"),
    executables: { NODE: hash("node"), E2E_BUN: hash("bun") },
    cases: hash(JSON.stringify(cases)), source: { "serial.mjs": hash("source") } };
  const bundles = cases.map((item, index) => ({ index, count: 2, platform: "linux", binding,
    inventory: cases.map(item => item.id), selected: [item.id], coverage,
    receipts: [{ code: 0, report: { platform: "linux", selected: [item.id], qualification: false,
      coverage, seconds: 0.1, limit_seconds: 10,
      results: [{ case: item.id, platform: "linux", status: "pass" }] } }] }));
  assert.equal(collectPartitions(cases, [...bundles].reverse(), "linux", coverage).pass, true);
  assert.throws(() => collectPartitions(cases, [bundles[0]], "linux", coverage), /every partition/);
  for (const edit of [bundle => bundle.index = 0, bundle => bundle.count = 3,
    bundle => bundle.platform = "win32", bundle => bundle.binding.binary = hash("changed"),
    bundle => bundle.binding.source["serial.mjs"] = hash("changed"),
    bundle => bundle.binding.executables.E2E_BUN = hash("changed"),
    bundle => bundle.inventory.reverse(), bundle => bundle.selected = ["first"],
    bundle => bundle.receipts = [], bundle => bundle.coverage = { pass: false }]) {
    // Give each bundle its own binding, as real receipt files do.
    const changed = bundles.map(bundle => structuredClone(bundle)); edit(changed[1]);
    assert.throws(() => collectPartitions(cases, changed, "linux", coverage));
  }
  const changed = structuredClone(cases); changed[0].changed = true;
  assert.throws(() => collectPartitions(changed, bundles, "linux", coverage), /definitions/);
  const missing = bundles.map(bundle => structuredClone(bundle)); delete missing[0].binding;
  assert.throws(() => collectPartitions(cases, missing, "linux", coverage), /identity/);
  const missingRuntime = bundles.map(bundle => structuredClone(bundle));
  for (const bundle of missingRuntime) delete bundle.binding.executables.NODE;
  assert.throws(() => collectPartitions(cases, missingRuntime, "linux", coverage), /Node identity/);
  // Agreement between partitions cannot turn absent provenance into a digest.
  // Both bundles carry the same malformed value, so equality alone accepts it.
  for (const source of [[], [hash("source")], { "serial.mjs": null },
    { "serial.mjs": true }, { "serial.mjs": "not-a-sha256" }, { "": hash("source") }]) {
    const invalid = bundles.map(bundle => structuredClone(bundle));
    for (const bundle of invalid) bundle.binding.source = source;
    assert.throws(() => collectPartitions(cases, invalid, "linux", coverage), /acceptance source/);
  }
  const red = structuredClone(bundles); red[1].receipts[0].code = 1;
  assert.equal(collectPartitions(cases, red, "linux", coverage).pass, false);
});
