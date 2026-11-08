import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { chmod, copyFile, mkdir, rename, rm, rmdir } from "node:fs/promises";
import { createServer } from "node:http";
import { delimiter, dirname, isAbsolute, join } from "node:path";
import test from "node:test";
import { Processes, Spool } from "../../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { environment, nativeEnvironment } from "../../../scripts/graphcoder-e2e/scratch.mjs";
import { withOwnedCase } from "../../../scripts/graphcoder-e2e/suite.mjs";
import { nativePackage } from "./native-model.mjs";
import { startWorkspaceOwner } from "./workspace-owner.mjs";

const recovery = "Set ACYCLIC_TEST_BINARY, E2E_GRAPHCODER and E2E_AGY to matching absolute executables; run node --test --test-concurrency=1 plugin/tests/hosts/agy-capture-admission.test.mjs";

// This is a native admission control, not a successful model-turn control. A
// real provider request is answered with an empty 401; no model is purchased.
test("native AGY refuses a failed shared capture before provider admission and admits after restoration", async t => {
  const binaries = ["ACYCLIC_TEST_BINARY", "E2E_GRAPHCODER", "E2E_AGY"].map(name => {
    const path = process.env[name];
    assert.ok(isAbsolute(path ?? ""), `step=admit ${name}; recovery=${recovery}`);
    return path;
  });
  const [binary, graphcoder, nativeAgy] = binaries;
  let root, provider, retained;
  let step = "create exclusive native profile";
  const scope = new Processes();
  try {
    root = hostFixtureDirectory("agy-capture-admission-");
    const workspace = join(root, "workspace with spaces"), profile = join(root, "profile with spaces");
    await mkdir(workspace);
    const bins = join(root, "bin with spaces"); await mkdir(bins);
    const agy = join(bins, process.platform === "win32" ? "agy.exe" : "agy");
    // The native host self-updates. Its mutable installation belongs to this
    // fixture; the caller's retained executable must stay unchanged.
    await copyFile(nativeAgy, agy); await chmod(agy, 0o700);
    const env = environment(profile, nativeEnvironment());
    env.PATH = dirname(agy) + delimiter + env.PATH;
    const run = async (program, args, input) => {
      // The workspace request has a bounded body and no child process. External
      // commands use the shared physical owner, whose stdin is cancellation.
      const result = await (input === undefined
        ? scope.runOwned(program, args, { cwd: workspace, env })
        : scope.run(program, args, { cwd: workspace, env }, input));
      t.diagnostic(JSON.stringify({ program, args, ...result }));
      return result;
    };
    const invoke = async args => {
      const running = await scope.startOwned(agy, args, { cwd: workspace, env });
      const events = new Spool(running.child.stdout, running.closed);
      let firstError, repetition;
      const unsubscribe = events.subscribe(event => {
        const step = event.step_update;
        if (event.event !== "step_update" || step?.step_type !== "error_message" || step.state !== "DONE") return;
        if (firstError === undefined) firstError = step.step_index;
        else if (step.step_index !== firstError && !repetition) {
          // A second injected error means Stop re-entered the same failed
          // admission. Reject that native progress failure on its event, then
          // join physical retirement before restoring the directory.
          repetition = new Error("native Stop repeated the failed PreInvocation admission");
          running.killExecutable();
        }
      });
      try {
        const result = await running.closed;
        t.diagnostic(JSON.stringify({ program: agy, args, ...result }));
        if (repetition) throw repetition;
        return result;
      } finally { unsubscribe(); }
    };
    await withOwnedCase({ processes: scope, run: async () => {
      step = "admit original native AGY interface";
      const version = await run(agy, ["--version"]);
      assert.equal(version.code, 0, version.stderr);
      assert.equal(version.stdout.trim(), "1.5.2", "use the native release that produced the retained hook corpus");
      step = "install the shipping package through the native plugin manager";
      const staged = await nativePackage(binary, join(root, "package with spaces"))();
      const installed = await run(staged.executable, ["install", "agy", "--plugin-root", staged.packageRoot]);
      assert.equal(installed.code, 0, installed.stderr);
      step = "start the original shared workspace owner";
      await startWorkspaceOwner({ scope, binary: graphcoder, workspace, state: join(root, "state"), root, env,
        capture: result => t.diagnostic(JSON.stringify({ role: "owner", ...result })) });
      const request = async action => {
        const result = await run(staged.executable, ["__workspace"], JSON.stringify(action));
        assert.equal(result.code, 0, result.stderr);
        return JSON.parse(result.stdout);
      };
      const opened = await request({ kind: "open", host: "agy", session: "capture-admission" });
      Object.assign(env, { ACYCLIC_TASK: opened.task, ACYCLIC_WORKSPACE: workspace });
      await request({ kind: "hook", host: "agy", event: "ToolBoundary", input: { task: opened.task } });

      const capability = randomBytes(32).toString("hex");
      let requests = 0;
      provider = createServer((request, response) => {
        requests++;
        // The native client sees a terminal authentication refusal, so a
        // successful capture can prove admission without a model fixture.
        request.resume();
        response.writeHead(401); response.end();
      });
      await new Promise((resolve, reject) => {
        provider.once("error", reject); provider.listen(0, "127.0.0.1", resolve);
      });
      Object.assign(env, { GEMINI_API_KEY: capability,
        GOOGLE_GEMINI_BASE_URL: `http://127.0.0.1:${provider.address().port}` });
      const args = ["--print=Capture the native pre-invocation hook without invoking a model.",
        "--output-format", "stream-json", "--sandbox", "--mode", "plan",
        "--settings", JSON.stringify({ modelProvider: "gemini" })];
      // The native host produces every callback and terminal. Print mode leaves
      // the physical owner's cancellation pipe separate from native input.
      const mountRoot = join(workspace, ".volumes", "agents");
      retained = join(root, "retained-mount-root");
      step = "replace the visible admitted directory while retaining its physical identity";
      await rename(mountRoot, retained);
      await mkdir(mountRoot);
      let refused;
      try {
        step = "await native PreInvocation capture refusal";
        refused = await invoke(args);
      } finally {
        await rmdir(mountRoot);
        await rename(retained, mountRoot); retained = undefined;
      }
      assert.notEqual(refused.code, 0, refused.stderr);
      assert.equal(requests, 0, "native PreInvocation admitted a provider request after the shared capture failed");
      assert.match(refused.stdout, /host-admitted mount root was replaced/);

      step = "admit the same native invocation after restoring the original directory";
      const admitted = await invoke(args);
      assert.notEqual(admitted.code, 0, "the fixture provider always refuses authentication");
      assert.equal(requests, 1, "restored shared capture must admit exactly the original native provider request");
      assert.doesNotMatch(admitted.stdout, /host-admitted mount root was replaced/);
    } });
  } catch (cause) {
    throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
  } finally {
    try {
      await scope.close();
      if (provider) { provider.closeAllConnections(); await new Promise(resolve => provider.close(resolve)); }
      // A failed restoration retains its directory and original native logs.
      if (root && !retained) await rm(root, { recursive: true });
    } catch (cause) {
      throw new Error(`step=retire native capture fixture: ${cause.message}; recovery=${recovery}`, { cause });
    }
  }
});
