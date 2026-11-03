import assert from "node:assert/strict";
import test from "node:test";
import { createHash } from "node:crypto";
import { collect, runSuites, partition,acceptanceBinding, variants } from "./serial.mjs";
import {mkdtemp,mkdir,readFile,writeFile,rename,rm,copyFile,symlink} from 'node:fs/promises';
import {dirname,join} from 'node:path';
import {nativeHost} from '../../plugin/tests/hosts/native-host.mjs';
import {Processes} from './process.mjs';
import {tmpdir} from 'node:os';
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { schedule } from "./process.mjs";
import { inventory, readCoverage } from "./coverage.mjs";

test("serial collection and command rejection retain original native failure evidence", async () => {
  const root = await mkdtemp(join(tmpdir(), "serial-failure-"));
  const scope = new Processes({ onProgress(event) {
    if (event.phase === "node-spawn") readFileSync(join(root, "missing-progress-sink"));
  } });
  try {
    const cases = [{ id: "native" }];
    let original;
    const receipts = await runSuites(cases, async () => {
      try { return await scope.run(process.execPath, ["-e", "process.exitCode=7"]); }
      catch (error) { original = error; throw error; }
    }, () => assert.fail("a failed capture cannot read a suite receipt"));
    const matrix = JSON.parse(JSON.stringify(collect(cases, receipts, process.platform, { pass: true })));
    assert.equal(matrix.pass, false);
    assert.equal(scope.children.size, 0, "collection follows original physical retirement");
    assert.equal(original.name, "ProcessObservationFailed");
    assert.equal(original.cause.code, "ENOENT");
    for (const result of [matrix.suites[0], matrix.results[0]]) {
      assert.equal(result.receipt_failure.error, original.stack);
      assert.match(result.receipt_failure.cause, /ENOENT/);
      assert.match(result.receipt_failure.cause, /missing-progress-sink/);
    }
    const command = new Processes();
    try {
      const args = [fileURLToPath(new URL("serial.mjs", import.meta.url)), process.execPath, process.execPath, "1/1"];
      const result = await command.run(process.execPath, args, { env: { ...process.env,
        E2E_SCRATCH_ROOT: root, E2E_OUTPUT: join(root, "matrix"), E2E_WORKSPACE_ROOT: "", E2E_NTFS_ROOT: "" } });
      assert.equal(result.code, 1, result.stderr);
      const failure = JSON.parse(result.stderr.trim());
      assert.match(failure.error, /AssertionError.*partition is empty or outside the inventory/s);
      assert.equal(failure.assertion.actual, "false");
      assert.equal(failure.assertion.expected, "true");
      assert.equal(failure.step, "serial matrix admission or collection");
      assert.deepEqual(failure.recovery.slice(1), args);
      assert.equal(command.children.size, 0);
    } finally { await command.close(); }
  } finally {
    await assert.rejects(scope.close(), error => error.name === "ProcessObservationFailed");
    await rm(root, { recursive: true, force: true });
  }
});

test("serial qualification retains the first assertion when a suite receipt is missing", async () => {
  const cases = [{ id: "case" }], coverage = { pass: true };
  let original;
  const receipts = await runSuites(cases, async () => {
    try { assert.equal("original result", "original expectation", "original operation"); }
    catch (error) { original = error; throw error; }
  }, async () => { throw new Error("unreachable receipt read"); });
  const matrix = JSON.parse(JSON.stringify(collect(cases, receipts, process.platform, coverage)));
  assert.equal(matrix.pass, false);
  for (const result of [matrix.suites[0], matrix.results[0]]) {
    assert.equal(result.receipt_failure.error, original.stack);
    assert.equal(result.receipt_failure.assertion.operator, original.operator);
    assert.match(result.receipt_failure.assertion.actual, /original result/);
    assert.match(result.receipt_failure.assertion.expected, /original expectation/);
  }
});

test('installed host admission launches the actual package entries',async()=>{
 const scope=new Processes();
 const cases=['codex','claude-code','pi'].map(host=>({inputs:[{format:'installed-host',host}]}));
 try {
  for(const {inputs:[{host}]} of cases) {
   const native=await nativeHost(host,{});
   const result=await scope.run(native.program,[...native.args,'--version']);
   assert.equal(result.code,0,result.stderr);
   assert.ok(result.stdout.trim(),'actual host version must be nonempty');
  }
  const {entry}=await nativeHost('pi',{});
  await assert.rejects(nativeHost('pi',{E2E_PI:process.execPath}),/step=admit E2E_PI/);
  await assert.rejects(nativeHost('pi',{E2E_PI:join(dirname(entry),'cli-runtime.js')}),/declared package bin/);
 }finally{await scope.close();}
});

test('installed host admission binds second trees, nested hidden files and link edges',async()=>{
 const root=await mkdtemp(join(tmpdir(),'host-closure-'));
 try {
  const directory=join(root,'scripts/graphcoder-e2e'),hosts=join(root,'plugin/tests/hosts');
  for(const path of [directory,hosts,join(root,'scripts/graphcoder-pty')])await mkdir(path,{recursive:true});
  await copyFile(new URL('../../plugin/tests/hosts/package.json',import.meta.url),join(hosts,'package.json'));
  // Copy real pinned package manifests and entry bytes for identity admission;
  // these reduced trees are never launched as installed runtimes.
  async function packageEntry(host,pkg,modules) {
   const native=await nativeHost(host,{}),source=join(native.closure,pkg);
   const manifest=JSON.parse(await readFile(join(source,'package.json'),'utf8'));
   const name=host==='codex'?'codex':'pi',bin=typeof manifest.bin==='string'?manifest.bin:manifest.bin[name];
   const target=join(modules,pkg),entry=join(target,bin);
   await mkdir(dirname(entry),{recursive:true});
   await copyFile(join(source,'package.json'),join(target,'package.json'));
   await copyFile(native.entry,entry);
   // The pinned Pi entry loads this captured module through
   // createRequire(import.meta.url)("./cli-runtime.js").
   if(host==='pi')await copyFile(join(dirname(native.entry),'cli-runtime.js'),join(dirname(entry),'cli-runtime.js'));
   return entry;
  }
  const first=await packageEntry('codex','@openai/codex',join(hosts,'node_modules'));
  const secondModules=join(root,'second/node_modules');
  const second=await packageEntry('pi','@earendil-works/pi-coding-agent',secondModules);
  const imported=join(dirname(second),'cli-runtime.js');
  const nested=join(secondModules,'.retained/node_modules/runtime.js');
  await mkdir(dirname(nested),{recursive:true});await copyFile(second,nested);
  // Directory junctions retain link-edge coverage on Windows without requiring
  // Developer Mode or the account's file-symlink privilege.
  const link=join(secondModules,'runtime-link'),type=process.platform==='win32'?'junction':'dir';
  await symlink(dirname(second),link,type);
  const cases=['codex','pi'].map(host=>({inputs:[{format:'installed-host',host}]}));
  const env={E2E_CODEX:first,E2E_PI:second};
  const bind=()=>acceptanceBinding(directory,process.execPath,process.execPath,cases,env);
  const before=await bind(),key='installed-hosts/pi/.retained/node_modules/runtime.js';
  const importedKey='installed-hosts/pi/@earendil-works/pi-coding-agent/dist/bundle/cli-runtime.js';
  assert.notEqual(before.hosts.codex.closure,before.hosts.pi.closure,'distinct installations need distinct provenance');
  assert.match(before.source[key],/^[a-f0-9]{64}$/,'second-tree nested hidden bytes must be retained');
  assert.match(before.source[importedKey],/^[a-f0-9]{64}$/,'the actual imported module must be retained');
  await writeFile(nested,Buffer.concat([await readFile(nested),Buffer.from('\n')]));
  await writeFile(imported,Buffer.concat([await readFile(imported),Buffer.from('\n')]));
  const changed=await bind();
  assert.deepEqual(changed.hosts,before.hosts,'both package entries stay unchanged');
  assert.notEqual(changed.source[key],before.source[key],'second-tree dependency changes must fence composition');
  assert.notEqual(changed.source[importedKey],before.source[importedKey],'the changed import itself must have a different digest');
  await rename(second,second+'.withdrawn');
  try {await assert.rejects(nativeHost('pi',env),/step=admit E2E_PI/);}
  finally {await rename(second+'.withdrawn',second);}
  await rm(link);await symlink(dirname(nested),link,type);
  const retargeted=await bind(),edge='installed-hosts/pi/runtime-link';
  assert.notEqual(retargeted.source[edge],changed.source[edge],'internal link retargeting must fence composition');
  await rm(link);await symlink(dirname(process.execPath),link,type);
  await assert.rejects(bind(),/step=admit pi dependency tree:.*recovery=npm ci/,
   'a link outside the admitted installation must refuse');
 }finally{await rm(root,{recursive:true,force:true});}
});

test('partition provenance includes plugin executables and host helper source',async()=>{
 const root=await mkdtemp(join(tmpdir(),'acceptance-binding-'));
 try {
  const directory=join(root,'scripts/graphcoder-e2e'),hosts=join(root,'plugin/tests/hosts');
  for(const path of [directory,hosts,join(root,'scripts/graphcoder-pty')])await mkdir(path,{recursive:true});
  const binary=join(root,'graphcoder'),mock=join(root,'mock'),plugin=join(root,'plugin-bin'),codex=join(root,'codex');
  for(const path of [binary,mock,plugin,codex])await writeFile(path,'original');
  const pluginBytes=Buffer.alloc(2*1024*1024+1,0x37);
  await writeFile(plugin,pluginBytes);
  const helper=join(hosts,'qualify-scripted.mjs');await writeFile(helper,'original helper');
  // These identity-only inputs refer to the actual running executable; no
  // invented native-tool output or executable image is needed for the check.
  const env={E2E_PLUGIN_BINARY:plugin,E2E_CODEX:codex,E2E_BUN:process.execPath,
    E2E_PROCESS_HOST:process.execPath,E2E_CONPTY_HOST:process.execPath};
  const before=await acceptanceBinding(directory,binary,mock,[],env);
  assert.equal(before.executables.E2E_PLUGIN_BINARY,createHash('sha256').update(pluginBytes).digest('hex'));
  assert.equal(before.source['file-digest.mjs'],createHash('sha256')
    .update(await readFile(new URL('../file-digest.mjs',import.meta.url))).digest('hex'));
  assert.match(before.executables.NODE,/^[a-f0-9]{64}$/);
  for(const name of ['E2E_BUN','E2E_PROCESS_HOST','E2E_CONPTY_HOST'])
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

test("serial qualification requires every OS case, volume variant, exit and completed suite", () => {
  const cases = [{ id: "case", ntfs: true }], coverage = { pass: true };
  assert.deepEqual(variants(cases, "win32").map(test => test.id), ["case", "case-ntfs"]);
  assert.deepEqual(variants(cases, "linux").map(test => test.id), ["case"]);
  // Each filesystem variant is its own suite and so owns its own envelope.
  const receipts = ["case", "case-ntfs"].map(id => ({ code: 0, report: { platform: "win32", selected: [id],
    qualification: false, coverage, seconds: 0.2,
    results: [{ case: id, platform: "win32", status: "pass" }] } }));
  assert.equal(collect(cases, receipts, "win32", coverage).pass, true);
  for (const seconds of [0.2, 10, 30, 1e6]) {
    const elapsed = structuredClone(receipts);
    for (const receipt of elapsed) receipt.report.seconds = seconds;
    assert.equal(collect(cases, elapsed, "win32", coverage).pass, true,
      `completion is unchanged by elapsed time (${seconds}s)`);
  }
  assert.throws(() => collect(cases, [], "win32", coverage), /every case variant/);
  const shared = structuredClone(receipts[0]);
  shared.report.results.push({ case: "case-ntfs", platform: "win32", status: "pass" });
  assert.throws(() => collect(cases, [shared], "win32", coverage), /every case variant/,
    "variants sharing one suite envelope cannot qualify");
  for (const edit of [report => report.results.pop(), report => report.results.push(report.results[0]),
    report => report.platform = "linux", report => report.selected = [],
    report => report.qualification = true, report => report.coverage = { pass: false },
    report => report.seconds = null]) {
    const changed = structuredClone(receipts); edit(changed[0].report);
    assert.equal(collect(cases, changed, "win32", coverage).pass, false);
  }
  for (const edit of [receipt => receipt.code = 1,
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
      coverage: { pass: true }, seconds: 0.1,
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

test("matrix command rejects stale receipts against its intended inputs", async () => {
  const root = await mkdtemp(join(tmpdir(), "matrix-identity-")), scope = new Processes();
  const directory = dirname(fileURLToPath(import.meta.url)), cases = inventory(), coverage = readCoverage(cases);
  const env = { ...process.env };
  for (const name of Object.keys(env)) if (name.startsWith("E2E_")
    && !["E2E_CODEX", "E2E_CLAUDE", "E2E_PI"].includes(name)) delete env[name];
  try {
    // Identity-only inputs use the actual Node executable. Collection launches
    // no case; this generated stale receipt cannot claim native acceptance.
    const digest = "0".repeat(64), binding = { binary: digest, mock: digest,
      cases: createHash("sha256").update(JSON.stringify(cases)).digest("hex"),
      executables: { NODE: digest }, source: { "serial.mjs": digest } };
    const input = join(root, "partition.json"), destination = join(root, "stale");
    await writeFile(input, JSON.stringify({ index: 0, count: 1, platform: process.platform,
      binding, coverage }));
    const args = [join(directory, "serial.mjs"), "--collect", process.execPath, process.execPath, destination, input];
    const result = await scope.run(process.execPath, args, { env });
    assert.equal(result.code, 1);
    const failure = JSON.parse(result.stderr);
    assert.match(failure.error, /expected acceptance identity/);
    assert.equal(failure.step, "serial matrix admission or collection");
    assert.deepEqual(failure.recovery.slice(1), args);
    await assert.rejects(readFile(join(destination, "matrix.json")), { code: "ENOENT" });
  } finally {
    await scope.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("matrix command binds its intended executables and retains refused receipts", async () => {
  const root = await mkdtemp(join(tmpdir(), "matrix-identity-")), scope = new Processes();
  const directory = dirname(fileURLToPath(import.meta.url)), cases = inventory(), coverage = readCoverage(cases);
  const env = { ...process.env };
  for (const name of Object.keys(env)) if (name.startsWith("E2E_")
    && !["E2E_CODEX", "E2E_CLAUDE", "E2E_PI"].includes(name)) delete env[name];
  try {
    // Identity-only inputs use the actual Node executable. Collection launches
    // no case; these generated internal refusals never claim native acceptance.
    const binding = await acceptanceBinding(directory, process.execPath, process.execPath, cases, env);
    const bundle = { index: 0, count: 1, platform: process.platform, binding, coverage,
      inventory: cases.map(test => test.id), selected: cases.map(test => test.id), seconds: .1,
      receipts: variants(cases, process.platform).map(test => ({ code: 1, report: {
        platform: process.platform, selected: [test.id], qualification: false, coverage,
        seconds: .1, limit_seconds: 10,
        results: [{ case: test.id, platform: process.platform, status: "refused" }] } })) };
    const input = join(root, "partition.json");
    await writeFile(input, JSON.stringify(bundle));
    const command = destination => scope.run(process.execPath,
      [join(directory, "serial.mjs"), "--collect", process.execPath, process.execPath, destination, input], { env });
    const destination = join(root, "collected"), result = await command(destination);
    assert.equal(result.code, 1, result.stderr);
    const report = JSON.parse(await readFile(join(destination, "matrix.json"), "utf8"));
    assert.deepEqual(JSON.parse(result.stdout), report);
    assert.deepEqual(report.binding, binding);
    assert.equal(report.pass, false);
    assert.deepEqual(report.results.map(row => row.status), bundle.receipts.map(() => "refused"));
  } finally {
    await scope.close();
    await rm(root, { recursive: true, force: true });
  }
});
