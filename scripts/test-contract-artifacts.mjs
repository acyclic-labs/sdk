import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { once } from "node:events";
import { connect, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { createInterface } from "node:readline";
import { contractModules } from "./generate-contracts.mjs";

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
    symlinkSync(fileURLToPath(new URL("../node_modules", import.meta.url)), join(root, "node_modules"), "junction");
    const generator = source("generate-contracts.mjs");
    put("scripts/generate-contracts.mjs", generator);
    put("scripts/contract-targets.mjs", source("contract-targets.mjs"));
    put("scripts/bazel-executables.mjs", source("bazel-executables.mjs"));
    put("scripts/package-closure.mjs", source("package-closure.mjs"));
    put("scripts/generated-bindings.mjs", source("generated-bindings.mjs"));
    put("package.json", JSON.stringify({ workspaces: [] }));
    for (const name of contractModules()) {
      put(`scripts/${name}`, name === "generate-actors-routes.mjs"
        ? 'export const rust = ["//fixture:contract"]; export const render = ([stdout]) => ({"contract.txt": stdout});'
        : 'export const render = () => ({});');
    }
    // Bazel is this Node executable running the build, info and cquery files below; cquery names the same
    // executable as the contract target, which the imported producer turns into the exporter only when it runs
    // with no script. The generator entry, its receipt and drift comparison are the real ones.
    put("producer.mjs", `import { appendFileSync, readFileSync, writeSync } from "node:fs";
if (process.argv.length === 1) {
  appendFileSync("executions.txt", "execute\\n");
  writeSync(1, readFileSync("candidate.txt"));
}
`);
    put("build", "");
    put("info", "process.stdout.write(process.cwd() + '\\n');");
    put("cquery", `process.stdout.write("@@//fixture:contract\\t" + ${JSON.stringify(process.execPath)} + "\\n");`);
    const env = { ...process.env, ACYCLIC_BAZEL: process.execPath, SDK_TEMP_DIR: join(root, "diagnostics"), GITHUB_RUN_ID: "fixture-run",
      NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ""} --import=${pathToFileURL(join(root, "producer.mjs")).href}` };
    put("contract.txt", "current\n");
    put("candidate.txt", "current\n");
    const git = args => {
      const result = spawnSync("git", args, { cwd: root, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      return result.stdout.trim();
    };
    git(["init", "--quiet"]);
    git(["add", "contract.txt"]);
    git(["-c", "commit.gpgsign=false", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "--quiet", "-m", "Fixture"]);
    const sourceSha = git(["rev-parse", "HEAD"]);
    env.CI_HEAD_SHA = sourceSha;
    const run = () => spawnSync(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "check"], { cwd: root, env, encoding: "utf8" });
    const receipt = () => JSON.parse(readFileSync(join(root, "diagnostics/observability/generated-contracts.json"), "utf8"));
    const success = run();
    assert.equal(success.status, 0, success.stderr);
    assert.equal(receipt().status, "success");
    assert.equal(receipt().source_commit, sourceSha);
    assert.deepEqual(receipt().labels, ["//fixture:contract"]);
    // An executable cquery names for the requested label can still emit
    // wrong bytes; the independent generated drift comparison must reject it.
    put("candidate.txt", "substituted\n");
    const substituted = run();
    assert.notEqual(substituted.status, 0);
    assert.match(substituted.stderr, /stale artifacts contract\.txt/);
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
    put("scripts/generate-inference-defaults.mjs", 'export const render = () => ({});');
    put("candidate.txt", "current\n");
    assert.equal(run().status, 0);
    const readinessFailure = step => Object.assign(new Error(`compiler-readiness step=${step}; recovery command: node --test scripts/test-contract-artifacts.mjs`), {
      code: "COMPILER_READINESS_FAILED", step,
    });
    let ready = Promise.withResolvers();
    ready.promise.catch(() => {}); // Listening failure can precede the await.
    // A separate native child stalls the build phase for the cancellation
    // control only. It supplies no build output. Arm its channel
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
    put("build", `const socket = require("node:net").connect(${JSON.stringify({ host: "127.0.0.1", port })}, () => socket.write(String(process.pid) + "\\n"));`);
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
    assert.equal(receipt().phase, "build");
    assert.equal(receipt().source_commit, sourceSha);
    assert.equal(readFileSync(join(root, "contract.txt"), "utf8"), "current\n");
    put("build", "");
    // Two renderers share the exact Bazel executable and execute it once. Both
    // independent file comparisons still run after cancellation. Decoder
    // missing/malformed cases live in the shared contract-target controls.
    put("scripts/generate-actors-routes.mjs", 'export const rust = ["//fixture:contract"]; export const rustPayload = "first"; export const render = ([stdout]) => ({"contract.txt": stdout});');
    put("scripts/generate-inference-defaults.mjs", 'export const rust = ["//fixture:contract"]; export const rustPayload = "second"; export const render = ([stdout]) => ({"second.txt": stdout});');
    put("second.txt", "second\n");
    put("candidate.txt", JSON.stringify({ first: "current\n", second: "second\n" }));
    rmSync(join(root, "executions.txt"));
    const shared = run();
    assert.equal(shared.status, 0, shared.stderr);
    assert.equal(receipt().status, "success");
    assert.equal(readFileSync(join(root, "executions.txt"), "utf8"), "execute\n");
    assert.deepEqual(receipt().labels, ["//fixture:contract"]);
  } finally {
    if (cancelled?.exitCode === null && cancelled.signalCode === null) cancelled.kill("SIGKILL");
    stopCompiler();
    if (generatorExit) await generatorExit.catch(() => {});
    compilerSocket?.destroy();
    if (compilerServer?.listening) await new Promise((resolve, reject) => compilerServer.close(error => error ? reject(error) : resolve()));
    rmSync(root, { recursive: true, force: true });
  }
});
