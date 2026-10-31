// A matrix is a collection of bounded case suites. Windows executes them serially.
import assert from "node:assert/strict";
import { mkdir, readFile, readdir, realpath, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { inventory, readCoverage } from "./coverage.mjs";
import { Processes } from "./process.mjs";
import { publishReport } from "./report.mjs";
import { environment, scratchDirectory } from "./scratch.mjs";
import { nativeHost } from "../../plugin/tests/hosts/native-host.mjs";

// Each filesystem variant is its own suite, so each owns one whole envelope.
export function variants(cases, platform) {
  return cases.flatMap(test => [{ ...test, variant: "primary" },
    ...(platform === "win32" && test.ntfs ? [{ ...test, id: `${test.id}-ntfs`, variant: "ntfs" }] : [])]);
}

export function collect(cases, receipts, platform, coverage) {
  const suites = [], results = [], expectedSuites = variants(cases, platform);
  assert.equal(receipts.length, expectedSuites.length, "every case variant needs a suite receipt");
  for (let index = 0; index < expectedSuites.length; index++) {
    const test = expectedSuites[index], { code, report, error, seconds, execution } = receipts[index];
    const expected = [test.id];
    try {
      assert.ok(report, error ?? "missing suite receipt");
      assert.equal(report.platform, platform, "suite OS mismatch");
      assert.deepEqual(report.selected, [test.id], "suite must select exactly its case");
      assert.equal(report.qualification, false, "selected suite is diagnostic");
      assert.deepEqual(report.coverage, coverage, "suite coverage changed during the matrix");
      assert.deepEqual(report.results.map(result => result.case), expected, "missing or duplicate case result");
      assert.ok(report.results.every(result => result.platform === platform), "case OS mismatch");
      assert.ok(Number.isFinite(report.seconds) && report.seconds > 0, "missing suite timing");
      assert.equal(report.limit_seconds, 10, "suite budget changed");
      suites.push({ case: test.id, seconds: report.seconds, code, execution });
      results.push(...report.results);
    } catch (failure) {
      suites.push({ case: test.id, seconds: seconds ?? null, code: code === 0 ? 1 : code,
        error: failure.stack, execution });
      results.push(...expected.map(id => ({ case: id, platform, status: "fail", error: failure.stack })));
    }
  }
  return { platform, qualification: true, coverage, suites, results,
    pass: coverage.pass && suites.every(suite => suite.code === 0 && suite.seconds < 10)
      && results.every(result => result.status === "pass") };
}

export async function runSuites(cases, run, read) {
  const receipts = [];
  for (const test of cases) {
    const start = performance.now();
    let result;
    try {
      result = await run(test);
      const report = await read(test);
      receipts.push({ code: result.code, report, execution: result,
        seconds: (performance.now() - start) / 1000 });
    } catch (error) {
      receipts.push({ code: result?.code ?? 1, report: null, error: error.stack,
        execution: result, seconds: (performance.now() - start) / 1000 });
    }
  }
  return receipts;
}

// Partitioning only splits a matrix into selections. It never qualifies a
// subset, changes a case budget or retries a previously executed case.
export function partition(cases, value) {
  if (value === undefined) return { index: 0, count: 1, cases };
  assert.match(value, /^\d+\/[1-9]\d*$/, "partition must be zero-based INDEX/COUNT");
  const [index, count] = value.split("/").map(Number);
  assert.ok(Number.isSafeInteger(index) && Number.isSafeInteger(count)
    && index < count && count <= cases.length, "partition is empty or outside the inventory");
  return { index, count, cases: cases.slice(Math.floor(index * cases.length / count),
    Math.floor((index + 1) * cases.length / count)) };
}

export function collectPartitions(cases, bundles, platform, coverage) {
  assert.ok(bundles.length > 0, "no partition receipts supplied");
  const ordered = [...bundles].sort((a, b) => a.index - b.index), first = ordered[0];
  for (const key of ["binary", "mock", "cases"])
    assert.match(first.binding?.[key] ?? "", /^[a-f0-9]{64}$/, `missing partition ${key} identity`);
  assert.ok(first.binding.executables && !Array.isArray(first.binding.executables),
    "missing partition executable identities");
  assert.match(first.binding.executables.NODE ?? "", /^[a-f0-9]{64}$/, "missing partition Node identity");
  for (const [name, digest] of Object.entries(first.binding.executables))
    assert.ok(name && typeof digest === "string" && /^[a-f0-9]{64}$/.test(digest),
      `invalid partition executable identity: ${name}`);
  assert.equal(first.binding.cases, createHash("sha256").update(JSON.stringify(cases)).digest("hex"),
    "partition case definitions changed");
  assert.ok(first.binding.source && typeof first.binding.source === "object"
    && !Array.isArray(first.binding.source) && Object.keys(first.binding.source).length > 0,
    "missing partition acceptance source");
  for (const [name, digest] of Object.entries(first.binding.source))
    assert.ok(name.length > 0 && typeof digest === "string" && /^[a-f0-9]{64}$/.test(digest),
      `invalid partition acceptance source digest: ${name}`);
  assert.equal(ordered.length, first.count, "every partition needs a receipt");
  for (let index = 0; index < ordered.length; index++) {
    const bundle = ordered[index];
    assert.equal(bundle.index, index, "duplicate or missing partition");
    assert.equal(bundle.count, first.count, "partition plan changed");
    assert.equal(bundle.platform, platform, "partition OS changed");
    assert.deepEqual(bundle.binding, first.binding, "partition executable or acceptance source changed");
    assert.deepEqual(bundle.coverage, coverage, "partition coverage changed");
    assert.deepEqual(bundle.inventory, cases.map(test => test.id), "partition inventory changed");
    const selected = partition(cases, `${index}/${first.count}`).cases;
    assert.deepEqual(bundle.selected, selected.map(test => test.id), "partition selection changed");
    assert.equal(bundle.receipts.length, variants(selected, platform).length, "partition lost a suite receipt");
  }
  return collect(cases, ordered.flatMap(bundle => bundle.receipts), platform, coverage);
}

async function digest(path) {
  const hash = createHash("sha256");
  for await (const bytes of createReadStream(path)) hash.update(bytes);
  return hash.digest("hex");
}

export async function acceptanceBinding(directory, binary, mock, cases, env=process.env) {
  const digests = new Map();
  const executable = path => {
    const absolute = resolve(path);
    if (!digests.has(absolute)) digests.set(absolute, digest(absolute));
    return digests.get(absolute);
  };
  const source = {};
  async function sources(root,prefix,closure) {
    const entries=(await readdir(root,{withFileTypes:true}))
      .filter(entry=>closure||!(entry.name.startsWith('.') || entry.name==='node_modules' || entry.name==='__pycache__'))
      .sort((a,b)=>a.name.localeCompare(b.name));
    // Bound open readers by one page, independent of installed tree size.
    // Finish every reader before descending or reporting an admission failure.
    for(let start=0;start<entries.length;start+=64) {
      const page=entries.slice(start,start+64);
      const results=await Promise.allSettled(page.map(async entry=>{
        const name=prefix+entry.name,path=join(root,entry.name);
        if(entry.isFile())return await digest(path);
        if(entry.isDirectory()||!closure)return;
        const target=await realpath(path),within=relative(closure,target);
        assert.ok(entry.isSymbolicLink()&&!isAbsolute(within)&&within!=='..'&&!within.startsWith('..'+sep),
          `step=admit host dependency ${name}; recovery=npm ci --prefix plugin/tests/hosts with no external dependency links`);
        // The target is visited at its ordinary path; only bind the link edge.
        return createHash('sha256').update(within).digest('hex');
      }));
      for(const result of results)if(result.status==='rejected')throw result.reason;
      for(let index=0;index<page.length;index++) {
        const entry=page[index],name=prefix+entry.name;
        if(entry.isDirectory())await sources(join(root,entry.name),name+'/',closure);
        else if(results[index].value!==undefined)source[name]=results[index].value;
      }
    }
  }
  await sources(directory,'graphcoder-e2e/');
  await sources(join(directory,'../../plugin/tests/hosts'),'plugin/tests/hosts/');
  await sources(join(directory,'../graphcoder-pty'),'graphcoder-pty/');
  const executables={ NODE: await executable(process.execPath) };
  for(const name of ['E2E_BUN','E2E_PROCESS_HOST','E2E_CONPTY_HOST',
    'E2E_PLUGIN_BINARY','E2E_CODEX','E2E_CLAUDE','E2E_PI','E2E_PLUGIN_TEST_BINARY','E2E_PROVIDER_TEST_BINARY'])
    if(env[name])executables[name]=await executable(env[name]);
  const hosts={},closures=new Map();
  for(const test of cases)for(const input of test.inputs??[]) {
    if(input.format!=='installed-host'||hosts[input.host])continue;
    const native=await nativeHost(input.host,env,join(directory,'../../plugin/tests/hosts'));
    let prefix=closures.get(native.closure);
    if(prefix===undefined) {
      prefix='installed-hosts/'+input.host+'/';
      closures.set(native.closure,prefix);
      try {await sources(native.closure,prefix,native.closure);}
      catch(cause) {
        throw new Error(`step=admit ${input.host} dependency tree: ${cause.message}; recovery=npm ci --prefix plugin/tests/hosts with no external dependency links`,{cause});
      }
    }
    hosts[input.host]={program:await executable(native.program),entry:await executable(native.entry),args:native.args.length,closure:prefix};
  }
  // Admission binds the installed transitive JS/native closure once, outside
  // every host turn. A lockfile alone cannot identify the installed bytes.
  return { binary: await executable(binary), mock: await executable(mock), source,executables,hosts,
    cases: createHash("sha256").update(JSON.stringify(cases)).digest("hex") };
}

async function main() {
  const [binary, mock, ...extra] = process.argv.slice(2);
  if (binary === "--collect") {
    assert.ok(mock && extra.length, "usage: node serial.mjs --collect FRESH_OUTPUT PARTITION.json ...");
    const cases = inventory(), coverage = readCoverage(cases);
    const bundles = await Promise.all(extra.map(async path => JSON.parse(await readFile(path, "utf8"))));
    const report = collectPartitions(cases, bundles, process.platform, coverage);
    report.seconds = bundles.reduce((total, bundle) => total + bundle.seconds, 0);
    const output = resolve(mock); await mkdir(output);
    await publishReport(join(output, "matrix.json"), report);
    process.exitCode = report.pass ? 0 : 1;
    return;
  }
  assert.ok(binary && mock && extra.length <= 1, "usage: node serial.mjs GRAPHCODER MOCK_MODEL [INDEX/COUNT]");
  const directory = dirname(fileURLToPath(import.meta.url));
  const scratch = scratchDirectory("graphcoder-e2e-matrix-");
  const output = resolve(process.env.E2E_OUTPUT ?? join(scratch, "results"));
  await mkdir(dirname(output), { recursive: true });
  await mkdir(output); // A completed or partial evidence identity is never rerun.
  const all = inventory(), plan = partition(all, extra[0]), cases = plan.cases;
  const coverage = readCoverage(all), processes = new Processes();
  const binding = await acceptanceBinding(directory, binary, mock, all);
  let receipts;
  const begin = performance.now();
  try {
    receipts = await runSuites(variants(cases, process.platform), async test => {
      const artifact = join(output, test.id);
      const result = await processes.run(process.execPath, [join(directory, "run.mjs"), resolve(binary), resolve(mock), test.id],
        { env: { ...environment(join(scratch, test.id)), E2E_SCRATCH_ROOT: scratch, E2E_OUTPUT: artifact } });
      console.log(`${test.id}: exit ${result.code}`);
      return result;
    }, async test => JSON.parse(await readFile(join(output, test.id, "matrix.json"), "utf8")));
  } finally { await processes.close(); }
  const seconds = (performance.now() - begin) / 1000;
  await writeFile(join(output, "partition.json"), JSON.stringify({ index: plan.index, count: plan.count,
    platform: process.platform, inventory: all.map(test => test.id), selected: cases.map(test => test.id),
    coverage, binding, receipts, seconds }, null, 2), { flag: "wx" });
  const report = { ...collect(cases, receipts, process.platform, coverage), seconds,
    qualification: plan.count === 1 };
  await publishReport(join(output, "matrix.json"), report);
  process.exitCode = report.pass ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(); }
  catch (error) {
    console.error(`step=serial matrix admission or collection: ${error.message}; recovery=${JSON.stringify(process.argv)}; inspect retained receipts and correct the rejected input before rerunning`);
    process.exitCode = 1;
  }
}
