import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { setTimeout } from "node:timers/promises";
import { buildContractTargets } from "./contract-targets.mjs";

test("compiler artifact discovery rejects identity substitution and partial completion", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-artifacts-"));
  const put = (name, content) => writeFileSync(join(root, name), content);
  const pkg = name => ({ name, id: `path+file:///fixture/${name}#0.1.0`, targets: [{ name: "contract", kind: ["bin"] }] });
  const packages = [pkg("alpha"), pkg("beta")];
  const artifacts = packages.map(({ name, id }) => ({
    reason: "compiler-artifact", package_id: id, target: { name: "contract", kind: ["bin"] },
    executable: join(root, `${name}-contract`), fresh: true, features: [],
  }));
  const targets = packages.map(({ name }) => [name, "bin", "contract"]);
  try {
    put("metadata", 'process.stdout.write(require("node:fs").readFileSync("metadata.json"));');
    put("metadata.json", JSON.stringify({ target_directory: join(root, "target"), packages }));
    put("build", 'process.stdout.write(require("node:fs").readFileSync("artifacts.jsonl"));');
    const emit = messages => put("artifacts.jsonl", messages.map(item => JSON.stringify(item)).join("\n"));
    const build = () => buildContractTargets(root, targets, process.execPath);
    emit(artifacts);
    assert.deepEqual(build(), artifacts.map(item => item.executable));
    // The stale path exists, so path guessing would incorrectly accept it.
    put("beta-contract", "stale bytes");
    emit([artifacts[0]]);
    assert.throws(build, /Rust contract target was not built/);
    for (const replacement of [
      { package_id: "another-package" },
      { target: { name: "contract", kind: ["example"] } },
      { target: { name: "another-target", kind: ["bin"] } },
      { executable: null },
    ]) {
      emit([artifacts[0], { ...artifacts[1], ...replacement }]);
      assert.throws(build, /Rust contract target was not built/);
    }
    emit([...artifacts, artifacts[0]]);
    assert.throws(build, /duplicate Rust contract artifact/);
    put("artifacts.jsonl", "{not JSON\n");
    assert.throws(build, SyntaxError);
    put("build", 'process.exit(7);');
    assert.throws(build, /Rust contract command failed with status 7/);
    assert.equal(readFileSync(join(root, "beta-contract"), "utf8"), "stale bytes");
    assert.throws(() => buildContractTargets(root, [], process.execPath), /target set is empty/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the generator compares executed bytes and records failure without changing checked files", async () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-drift-"));
  const put = (name, content) => writeFileSync(join(root, name), content);
  const source = name => readFileSync(fileURLToPath(new URL(name, import.meta.url)), "utf8");
  let cancelled;
  let compilerPid;
  const stopCompiler = () => {
    if (!compilerPid) return;
    try { process.kill(compilerPid, "SIGKILL"); } catch (error) { if (!(error instanceof Error) || !("code" in error) || error.code !== "ESRCH") throw error; }
    compilerPid = undefined;
  };
  try {
    mkdirSync(join(root, "scripts"));
    const generator = source("generate-contracts.mjs");
    put("scripts/generate-contracts.mjs", generator);
    put("scripts/contract-targets.mjs", source("contract-targets.mjs"));
    for (const [, name] of generator.matchAll(/^  "([^"]+\.mjs)",/gm)) {
      put(`scripts/${name}`, name === "generate-runtime-routes.mjs"
        ? 'export const rust = [["fixture", "bin", "contract"]]; export const render = ([stdout]) => ({"contract.txt": stdout});'
        : 'export const render = () => ({});');
    }
    // Use Node as an executable CLI fixture, including a no-argument native
    // target invocation. Only the latter prints the candidate contract bytes.
    put("bootstrap.cjs", 'if (process.argv.length === 1) { process.stdout.write(require("node:fs").readFileSync("candidate.txt")); process.exit(0); }');
    const pkg = { name: "fixture", id: "fixture#0.1.0", targets: [{ name: "contract", kind: ["bin"] }] };
    put("metadata", `console.log(${JSON.stringify(JSON.stringify({ packages: [pkg], target_directory: join(root, "target") }))});`);
    put("build", `console.log(${JSON.stringify(JSON.stringify({ reason: "compiler-artifact", package_id: pkg.id, target: pkg.targets[0], executable: process.execPath, features: [], fresh: true }))});`);
    put("contract.txt", "current\n");
    put("candidate.txt", "current\n");
    const git = args => {
      const result = spawnSync("git", args, { cwd: root, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      return result.stdout.trim();
    };
    git(["init", "--quiet"]);
    git(["-c", "commit.gpgsign=false", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "--quiet", "-m", "Fixture"]);
    const sourceSha = git(["rev-parse", "HEAD"]);
    const env = { ...process.env, ACYCLIC_CARGO_BIN: process.execPath, SDK_TEMP_DIR: join(root, "diagnostics"), NODE_OPTIONS: `--require=${JSON.stringify(join(root, "bootstrap.cjs"))}`, CI_HEAD_SHA: sourceSha, GITHUB_RUN_ID: "fixture-run" };
    const run = () => spawnSync(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "check"], { cwd: root, env, encoding: "utf8" });
    const receipt = () => JSON.parse(readFileSync(join(root, "diagnostics/observability/generated-contracts.json"), "utf8"));
    const success = run();
    assert.equal(success.status, 0, success.stderr);
    assert.equal(receipt().status, "success");
    assert.equal(receipt().source_commit, sourceSha);
    assert.equal(receipt().artifacts.filter(item => item.requested).length, 1);
    // An executable selected under the correct Cargo identity can still emit
    // wrong bytes; the independent generated drift comparison must reject it.
    put("candidate.txt", "substituted\n");
    const substituted = run();
    assert.notEqual(substituted.status, 0);
    assert.match(substituted.stderr, /generated contracts are stale/);
    assert.equal(receipt().status, "failed");
    assert.equal(receipt().phase, "drift-check");
    assert.equal(readFileSync(join(root, "contract.txt"), "utf8"), "current\n");
    env.CI_HEAD_SHA = "another-source";
    assert.match(run().stderr, /contract checkout differs from CI_HEAD_SHA/);
    assert.equal(receipt().phase, "source");
    env.CI_HEAD_SHA = sourceSha;
    put("scripts/generate-observe.mjs", 'throw new Error("incomplete generator import");');
    assert.notEqual(run().status, 0);
    assert.equal(receipt().status, "failed");
    assert.equal(receipt().phase, "imports");
    const summary = spawnSync(process.execPath, [fileURLToPath(new URL("ci-summary.mjs", import.meta.url)), join(root, "diagnostics/observability")], {
      encoding: "utf8", env: { ...process.env, GITHUB_RUN_ID: "another-run", CI_HEAD_SHA: sourceSha },
    });
    assert.notEqual(summary.status, 0);
    assert.match(summary.stderr, /timings belong to another run\/source/);
    put("scripts/generate-observe.mjs", 'export const render = () => ({});');
    put("candidate.txt", "current\n");
    assert.equal(run().status, 0);
    const metadata = readFileSync(join(root, "metadata"));
    put("metadata", 'require("node:fs").writeFileSync("compiler.pid", String(process.pid)); setInterval(() => {}, 1000);');
    cancelled = spawn(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "check"], { cwd: root, env, stdio: "ignore", windowsHide: true });
    const deadline = performance.now() + 10_000;
    while (!existsSync(join(root, "compiler.pid")) && performance.now() < deadline) await setTimeout(25);
    assert.ok(existsSync(join(root, "compiler.pid")), "owned compiler fixture did not start");
    compilerPid = Number(readFileSync(join(root, "compiler.pid"), "utf8"));
    process.kill(compilerPid, 0); // Confirm the exact owned child is live.
    const exited = new Promise(resolve => cancelled.once("exit", resolve));
    assert.ok(cancelled.kill("SIGKILL"));
    await exited;
    stopCompiler();
    assert.equal(receipt().status, "incomplete", "abrupt termination must not reuse the previous success");
    const incompleteSummary = spawnSync(process.execPath, [fileURLToPath(new URL("ci-summary.mjs", import.meta.url)), join(root, "diagnostics/observability")], { env, encoding: "utf8" });
    assert.equal(incompleteSummary.status, 0, incompleteSummary.stderr);
    assert.match(incompleteSummary.stdout, /incomplete/);
    assert.match(incompleteSummary.stdout, /not observed/);
    for (const replacement of [
      { status: "success" },
      { source_commit: "another-source" },
      { run_id: "another-run" },
    ]) {
      const incomplete = receipt();
      put("diagnostics/observability/generated-contracts.json", JSON.stringify({ ...incomplete, ...replacement }));
      const rejected = spawnSync(process.execPath, [fileURLToPath(new URL("ci-summary.mjs", import.meta.url)), join(root, "diagnostics/observability")], { env, encoding: "utf8" });
      assert.notEqual(rejected.status, 0);
      assert.match(rejected.stderr, /timings belong to another run\/source/);
      put("diagnostics/observability/generated-contracts.json", JSON.stringify(incomplete));
    }
    assert.equal(readFileSync(join(root, "contract.txt"), "utf8"), "current\n");
    put("metadata", metadata);
    assert.equal(run().status, 0, "retry after cancellation must complete");
    assert.equal(receipt().status, "success");
  } finally {
    if (cancelled?.exitCode === null && cancelled.signalCode === null) cancelled.kill("SIGKILL");
    stopCompiler();
    rmSync(root, { recursive: true, force: true });
  }
});
