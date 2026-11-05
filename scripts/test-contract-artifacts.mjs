import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { once } from "node:events";
import { connect, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { createInterface } from "node:readline";

test("the generator compares executed bytes and records failure without changing checked files", async () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-drift-"));
  const put = (name, content) => writeFileSync(join(root, name), content);
  const source = name => readFileSync(fileURLToPath(new URL(name, import.meta.url)), "utf8");
  let cancelled;
  let compilerPid;
  let compilerServer, compilerSocket, generatorExit;
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
    put("scripts/package-closure.mjs", source("package-closure.mjs"));
    put("scripts/generated-bindings.mjs", source("generated-bindings.mjs"));
    put("package.json", JSON.stringify({ workspaces: [] }));
    for (const [, name] of generator.matchAll(/^  "([^"]+\.mjs)",/gm)) {
      put(`scripts/${name}`, name === "generate-actors-routes.mjs"
        ? 'export const rust = [["fixture", "bin", "contract"]]; export const render = ([stdout]) => ({"contract.txt": stdout});'
        : 'export const render = () => ({});');
    }
    // Execute the actual generator entry, Cargo metadata/artifact discovery and
    // selected Rust binary. No hand-written Cargo messages admit this exporter.
    const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
    put("Cargo.toml", '[package]\nname = "fixture"\nversion = "0.1.0"\nedition = "2024"\n[[bin]]\nname = "contract"\npath = "src/main.rs"\n');
    mkdirSync(join(root, "src"));
    put("src/main.rs", `use std::io::Write;
fn main() -> std::io::Result<()> {
    std::fs::OpenOptions::new().create(true).append(true).open("executions.txt")?.write_all(b"execute\\n")?;
    std::io::stdout().lock().write_all(&std::fs::read("candidate.txt")?)
}
`);
    const env = { ...process.env, ACYCLIC_CARGO_BIN: cargo, CARGO_TARGET_DIR: join(root, "target"),
      SDK_TEMP_DIR: join(root, "diagnostics"), GITHUB_RUN_ID: "fixture-run" };
    const locked = spawnSync(cargo, ["generate-lockfile", "--offline"], { cwd: root, env, encoding: "utf8" });
    assert.equal(locked.status, 0, locked.error?.message ?? locked.stderr);
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
    env.CI_HEAD_SHA = sourceSha;
    // Retain the exact real wire at the generator's existing decode boundary.
    // Later report/drift controls replay it against the SAME unchanged binary;
    // source/configuration freshness is covered by the real Cargo control.
    put("capture-wire.mjs", `import { appendFileSync, writeFileSync } from "node:fs";
const parse = JSON.parse;
JSON.parse = (text, ...args) => {
  const value = parse(text, ...args);
  if (value?.packages) writeFileSync("metadata.json", text);
  else if (value?.reason) appendFileSync("artifacts.jsonl", text + "\\n");
  return value;
};
`);
    env.NODE_OPTIONS = `${process.env.NODE_OPTIONS ?? ""} --import=${pathToFileURL(join(root, "capture-wire.mjs")).href}`;
    const run = () => spawnSync(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "check"], { cwd: root, env, encoding: "utf8" });
    const receipt = () => JSON.parse(readFileSync(join(root, "diagnostics/observability/generated-contracts.json"), "utf8"));
    const success = run();
    assert.equal(success.status, 0, success.stderr);
    // The real exporter repairs changed output and preserves matching output.
    // An old timestamp makes an unconditional write observable without a sleep.
    put("contract.txt", "stale\n");
    const write = () => spawnSync(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "write"], { cwd: root, env, encoding: "utf8" });
    assert.equal(write().status, 0);
    assert.equal(readFileSync(join(root, "contract.txt"), "utf8"), "current\n");
    utimesSync(join(root, "contract.txt"), 1, 1);
    const unchanged = statSync(join(root, "contract.txt"), { bigint: true }).mtimeNs;
    assert.equal(write().status, 0);
    assert.equal(statSync(join(root, "contract.txt"), { bigint: true }).mtimeNs, unchanged);
    assert.equal(receipt().status, "success");
    assert.equal(receipt().source_commit, sourceSha);
    assert.equal(receipt().artifacts.filter(item => item.requested).length, 1);
    env.NODE_OPTIONS = process.env.NODE_OPTIONS;
    put("metadata", 'process.stdout.write(require("node:fs").readFileSync("metadata.json"));');
    put("build", 'process.stdout.write(require("node:fs").readFileSync("artifacts.jsonl"));');
    env.ACYCLIC_CARGO_BIN = process.execPath;
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
    put("scripts/generate-inference-defaults.mjs", 'throw new Error("incomplete generator import");');
    assert.notEqual(run().status, 0);
    assert.equal(receipt().status, "failed");
    assert.equal(receipt().phase, "imports");
    const summary = spawnSync(process.execPath, [fileURLToPath(new URL("ci-summary.mjs", import.meta.url)), join(root, "diagnostics/observability")], {
      encoding: "utf8", env: { ...process.env, GITHUB_RUN_ID: "another-run", CI_HEAD_SHA: sourceSha },
    });
    assert.notEqual(summary.status, 0);
    assert.match(summary.stderr, /timings belong to another run\/source/);
    put("scripts/generate-inference-defaults.mjs", 'export const render = () => ({});');
    put("candidate.txt", "current\n");
    assert.equal(run().status, 0);
    const metadataReplay = readFileSync(join(root, "metadata"));
    const readinessFailure = step => Object.assign(new Error(`compiler-readiness step=${step}; recovery command: node --test scripts/test-contract-artifacts.mjs`), {
      code: "COMPILER_READINESS_FAILED", step,
    });
    let ready = Promise.withResolvers();
    ready.promise.catch(() => {}); // Listening failure can precede the await.
    // A separate native child stalls the metadata phase for the cancellation
    // control only. It supplies no metadata or compiler wire. Arm its channel
    // before starting it, including early connection loss.
    compilerServer = createServer(socket => {
      const admitted = ready;
      compilerSocket = socket;
      socket.once("error", admitted.reject);
      const lines = createInterface({ input: socket });
      lines.once("error", admitted.reject);
      lines.once("line", line => {
        const pid = Number(line);
        if (!/^\d+$/.test(line) || !Number.isSafeInteger(pid) || pid <= 0) {
          admitted.reject(readinessFailure("identity"));
        } else admitted.resolve(pid);
      });
      lines.once("close", () => admitted.reject(readinessFailure("source-ended")));
    });
    compilerServer.on("error", error => ready.reject(error));
    compilerServer.listen(0, "127.0.0.1");
    await once(compilerServer, "listening");
    const port = compilerServer.address().port;
    // Negative control: actual source EOF without an identity must settle.
    const lost = assert.rejects(ready.promise, { code: "COMPILER_READINESS_FAILED", step: "source-ended" });
    const departed = connect({ host: "127.0.0.1", port });
    departed.once("error", ready.reject);
    departed.end();
    await lost;
    // The actual TCP reset also reaches readline's error port. Losing its
    // producer before identity must reject this same wait, never throw outside it.
    ready = Promise.withResolvers();
    const resetLost = assert.rejects(ready.promise, { code: "ECONNRESET" });
    const reset = connect({ host: "127.0.0.1", port });
    reset.once("error", ready.reject);
    await once(reset, "connect");
    reset.resetAndDestroy();
    await resetLost;
    ready = Promise.withResolvers();
    put("metadata", `const socket = require("node:net").connect(${JSON.stringify({ host: "127.0.0.1", port })}, () => socket.write(String(process.pid) + "\\n"));`);
    cancelled = spawn(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "check"], { cwd: root, env, stdio: "ignore", windowsHide: true });
    generatorExit = once(cancelled, "exit");
    generatorExit.catch(() => {}); // The original join below observes spawn loss.
    cancelled.once("error", ready.reject);
    cancelled.once("exit", () => ready.reject(readinessFailure("generator-ended")));
    compilerPid = await ready.promise;
    process.kill(compilerPid, 0); // Confirm the exact owned child is live.
    assert.ok(cancelled.kill("SIGKILL"));
    await generatorExit;
    stopCompiler();
    assert.equal(receipt().status, "incomplete", "abrupt termination must not reuse the previous success");
    assert.equal(receipt().phase, "metadata");
    assert.equal(receipt().source_commit, sourceSha);
    const incompleteSummary = spawnSync(process.execPath, [fileURLToPath(new URL("ci-summary.mjs", import.meta.url)), join(root, "diagnostics/observability")], { env, encoding: "utf8" });
    assert.equal(incompleteSummary.status, 0, incompleteSummary.stderr);
    assert.match(incompleteSummary.stdout, /incomplete/);
    assert.match(incompleteSummary.stdout, /metadata/);
    assert.match(incompleteSummary.stdout, new RegExp(sourceSha));
    for (const replacement of [
      { status: "success", source_commit: null },
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
    put("metadata", metadataReplay);
    // Two renderers share the exact Cargo artifact and execute it once. Both
    // independent file comparisons still run after cancellation. Decoder
    // missing/malformed cases live in the shared contract-target controls.
    put("scripts/generate-actors-routes.mjs", 'export const rust = [["fixture", "bin", "contract"]]; export const rustPayload = "first"; export const render = ([stdout]) => ({"contract.txt": stdout});');
    put("scripts/generate-inference-defaults.mjs", 'export const rust = [["fixture", "bin", "contract"]]; export const rustPayload = "second"; export const render = ([stdout]) => ({"second.txt": stdout});');
    put("second.txt", "second\n");
    put("candidate.txt", JSON.stringify({ first: "current\n", second: "second\n" }));
    rmSync(join(root, "executions.txt"));
    const shared = run();
    assert.equal(shared.status, 0, shared.stderr);
    assert.equal(receipt().status, "success");
    assert.equal(readFileSync(join(root, "executions.txt"), "utf8"), "execute\n");
    assert.equal(receipt().artifacts.filter(item => item.requested).length, 1);
  } finally {
    if (cancelled?.exitCode === null && cancelled.signalCode === null) cancelled.kill("SIGKILL");
    stopCompiler();
    if (generatorExit) await generatorExit.catch(() => {});
    compilerSocket?.destroy();
    if (compilerServer?.listening) await new Promise((resolve, reject) => compilerServer.close(error => error ? reject(error) : resolve()));
    rmSync(root, { recursive: true, force: true });
  }
});
