import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { admitArtifacts, admitFiles, readAdmittedManifest } from "../graphcoder-e2e/artifacts.mjs";
import { Processes } from "../graphcoder-e2e/process.mjs";
import { admit, markdown, plan, report } from "./matrix.mjs";

// One process owns an experiment's volume and learned patterns. Cold, warm and
// hinted are ordered phases of that experiment, never reused across workers.
export function experiments(requests) {
  const groups = new Map(), identities = new Set();
  for (const request of requests) {
    assert.ok(!identities.has(request.id), "duplicate experiment request");
    identities.add(request.id);
    const group = groups.get(request.experiment_id) ?? [];
    assert.ok(!group.some(prior => prior.state === request.state), "duplicate experiment phase");
    group.push(request);
    groups.set(request.experiment_id, group);
  }
  for (const group of groups.values()) {
    const first = group[0];
    assert.ok(group.every(request => request.source_pin === first.source_pin
      && JSON.stringify(request.backend) === JSON.stringify(first.backend)
      && JSON.stringify(request.workload) === JSON.stringify(first.workload)), "mixed experiment inputs");
    assert.equal(group[0].state, "cold", "an experiment starts cold");
    const order = { cold: 0, warm: 1, hinted: 2 };
    assert.ok(group.every((request, index) => !index || order[request.state] > order[group[index - 1].state]),
      "experiment phases must preserve learning order");
  }
  return [...groups.values()];
}

export function admitExperiment(requests, raw, identity) {
  assert.equal(raw.version, 1, "unknown driver response version");
  assert.equal(raw.experiment_id, requests[0].experiment_id, "driver answered another experiment");
  assert.equal(raw.fixture_id, identity.fixture_id, "driver reused another fixture");
  assert.ok(Array.isArray(raw.measurements) && raw.measurements.length === requests.length,
    "driver must measure every requested phase");
  return requests.map((request, index) => {
    const value = raw.measurements[index];
    assert.equal(value.request_id, request.id, "driver omitted or reordered a phase");
    // Bind observations to the exact admitted binary and source closure here.
    // The worker cannot substitute a different artifact or older result.
    return admit(request, { ...value, version: 1, source_pin: request.source_pin,
      experiment_id: request.experiment_id, fixture_id: identity.fixture_id,
      executable_sha256: identity.executable_sha256, source_manifest_sha256: identity.source_manifest_sha256 });
  });
}

function fixtureEnvironment(inherited, fixture) {
  const env = Object.fromEntries(Object.entries(inherited).filter(([key]) => {
    const name = key.toUpperCase();
    return !name.endsWith("API_KEY") && !name.includes("AUTH_TOKEN")
      && !name.endsWith("ACCESS_TOKEN") && !name.endsWith("SECRET") && name !== "RUST_MIN_STACK";
  }));
  return { ...env, HOME: join(fixture, "home"), USERPROFILE: join(fixture, "home"),
    CODEX_HOME: join(fixture, "home", ".codex-fixture"),
    LOCALAPPDATA: join(fixture, "home", "AppData", "Local"), APPDATA: join(fixture, "home", "AppData", "Roaming"),
    TMPDIR: join(fixture, "tmp"), TEMP: join(fixture, "tmp"), TMP: join(fixture, "tmp"),
    RUST_TEST_THREADS: "4", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: join(fixture, "home", ".gitconfig") };
}

// Host adapters differ only in the data sent to this one executable. Cloud
// routing/latency injection and native mount ownership belong to that driver;
// they are not emulated by timing a different local operation in the runner.
export async function runPlan(requests, configuration, processes = new Processes()) {
  const groups = experiments(requests);
  assert.ok(groups.length, "empty benchmark run");
  const { driver, source, records, backends } = configuration;
  assert.ok(isAbsolute(records), "records directory must be absolute");
  if (process.platform === "win32") {
    assert.match(records.replaceAll("\\", "/"), /^D:\/lane-scratch\/[^/]+\//i, "Windows records belong under D:/lane-scratch/<lane>/");
  }
  assert.equal(new Set(requests.map(value => value.source_pin)).size, 1, "mixed SDK source pins");
  const manifest = await readAdmittedManifest(source.manifest, source.sha256);
  assert.equal(manifest.version, 1, "unknown source manifest version");
  assert.equal(manifest.source_pin, requests[0].source_pin, "source closure belongs to another SDK pin");
  assert.deepEqual(manifest.files, source.expected_files, "producer source closure differs from admitted build inputs");
  await admitFiles(source.directory, manifest.files);
  const bundle = await admitArtifacts(driver.directory, driver.manifest_sha256);
  assert.equal(bundle.producer?.version, 1, "workload executable needs its producer receipt");
  assert.equal(bundle.producer.source_pin, manifest.source_pin, "binary and source SDK pins differ");
  assert.equal(bundle.producer.source_manifest_sha256, source.sha256, "binary and source come from different producer graphs");
  assert.match(bundle.producer.uploaded_snapshot_sha256, /^[a-f0-9]{64}$/, "uploaded source identity is required");
  const artifact = bundle.artifacts[driver.name];
  assert.ok(artifact && artifact.kind === "executable", "one Cargo-selected workload executable is required");
  assert.match(artifact.cargo_json_sha256, /^[a-f0-9]{64}$/, "Cargo selection receipt is required");
  for (const request of requests) assert.ok(Object.hasOwn(backends, request.backend.id), "missing backend preparation");
  await mkdir(records); // Existing results or fixture ownership are rejected.
  const receipts = [];
  let failure;
  try {
    for (const group of groups) {
      const fixtureId = randomUUID(), fixture = join(records, fixtureId);
      await mkdir(fixture);
      const env = fixtureEnvironment(process.env, fixture);
      for (const directory of new Set([env.CODEX_HOME, env.LOCALAPPDATA, env.APPDATA, env.TMPDIR])) {
        await mkdir(directory, { recursive: true });
      }
      const requestFile = join(fixture, "experiment.json"), resultFile = join(fixture, "measurements.json");
      const experiment = { version: 1, experiment_id: group[0].experiment_id, fixture_id: fixtureId,
        requests: group, backend: backends[group[0].backend.id], fixture };
      await writeFile(requestFile, JSON.stringify(experiment, null, 2) + "\n", { flag: "wx" });
      // Re-admit the executable before each experiment. A hash from another
      // producer graph, even at the same Git revision, is a different run.
      await admitArtifacts(driver.directory, driver.manifest_sha256);
      const started = performance.now();
      const result = await processes.runOwned(artifact.path, ["--experiment", requestFile, "--result", resultFile],
        { cwd: source.directory, env });
      await writeFile(join(fixture, "stdout.log"), result.stdout, { flag: "wx" });
      await writeFile(join(fixture, "stderr.log"), result.stderr, { flag: "wx" });
      await writeFile(join(fixture, "process.json"), JSON.stringify({ code: result.code, signal: result.signal,
        seconds: (performance.now() - started) / 1000, executable_sha256: artifact.sha256 }) + "\n", { flag: "wx" });
      assert.equal(result.code, 0, `workload process failed: ${fixtureId}`);
      await admitArtifacts(driver.directory, driver.manifest_sha256);
      const raw = JSON.parse(await readFile(resultFile, "utf8"));
      receipts.push(...admitExperiment(group, raw, { fixture_id: fixtureId,
        executable_sha256: artifact.sha256, source_manifest_sha256: source.sha256 }));
      await writeFile(join(fixture, "admitted.json"), JSON.stringify(receipts.slice(-group.length), null, 2) + "\n", { flag: "wx" });
    }
    await admitFiles(source.directory, manifest.files);
  } catch (error) { failure = error; }
  finally {
    await processes.close();
    await writeFile(join(records, "receipts.json"), JSON.stringify(receipts, null, 2) + "\n", { flag: "wx" });
    const summary = report(requests, receipts);
    if (failure) { summary.complete = false; summary.failure = failure.message; }
    await writeFile(join(records, "report.json"), JSON.stringify(summary, null, 2) + "\n", { flag: "wx" });
    await writeFile(join(records, "report.md"), markdown(summary), { flag: "wx" });
  }
  if (failure) throw failure;
  return receipts;
}

async function main() {
  const [matrixFile, sourcePin, configurationFile, ...extra] = process.argv.slice(2);
  assert.ok(matrixFile && sourcePin && configurationFile && !extra.length, "usage: runner.mjs MATRIX SDK_PIN CONFIGURATION");
  const matrix = JSON.parse(await readFile(matrixFile, "utf8"));
  const configuration = JSON.parse(await readFile(configurationFile, "utf8"));
  const requests = plan(matrix, sourcePin);
  const receipts = await runPlan(requests, configuration);
  if (!report(requests, receipts).complete) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await main();
