import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmod, copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { delimiter, dirname, isAbsolute, join } from "node:path";
import { createServer } from "node:http";
import test from "node:test";
import { Processes } from "../../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { environment, nativeEnvironment } from "../../../scripts/graphcoder-e2e/scratch.mjs";
import { withOwnedCase } from "../../../scripts/graphcoder-e2e/suite.mjs";
import { nativePackage } from "./native-model.mjs";
import { startWorkspaceOwner } from "./workspace-owner.mjs";

const recovery = "Set ACYCLIC_TEST_BINARY, E2E_GRAPHCODER and E2E_OPENCODE to matching absolute executables; run node --test --test-concurrency=1 plugin/tests/hosts/opencode-workspace.test.mjs";

test("installed native OpenCode records its original session through the shared workspace", async t => {
  const [binary, graphcoder, native] = ["ACYCLIC_TEST_BINARY", "E2E_GRAPHCODER", "E2E_OPENCODE"].map(name => {
    const path = process.env[name];
    assert.ok(isAbsolute(path ?? ""), `step=admit ${name}; recovery=${recovery}`);
    return path;
  });
  const scope = new Processes();
  let root, provider, step = "admit original recorded provider bytes";
  try {
    // Original llama.cpp capture, also consumed by the shared conformance
    // suite and native terminal corpus. No generated provider responses.
    const body = await readFile(new URL("../../../rust/crates/conformance/fixtures/inference/llamacpp/text.body", import.meta.url));
    assert.equal(createHash("sha256").update(body).digest("hex"),
      "9291f29c22062298aff55dd764b31a3632d9e6560fd55da78a35ed1673c5bd54");
    root = hostFixtureDirectory("opencode-workspace-");
    const workspace = join(root, "workspace with spaces"), profile = join(root, "profile with spaces");
    const bins = join(root, "bin with spaces");
    await mkdir(workspace); await mkdir(bins);
    const opencode = join(bins, process.platform === "win32" ? "opencode.exe" : "opencode");
    await copyFile(native, opencode); await chmod(opencode, 0o700);
    const env = environment(profile, nativeEnvironment());
    env.PATH = dirname(opencode) + delimiter + env.PATH;
    const run = async (program, args, input) => {
      const result = input === undefined
        ? await scope.runOwned(program, args, { cwd: workspace, env })
        : await scope.run(program, args, { cwd: workspace, env }, input);
      t.diagnostic(JSON.stringify({ program, args, ...result }));
      return result;
    };
    const requests = [];
    provider = createServer(async (request, response) => {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      requests.push({ url: request.url, body: JSON.parse(Buffer.concat(chunks).toString("utf8")) });
      response.writeHead(200, { "content-type": "text/event-stream" }); response.end(body);
    });
    await new Promise((resolve, reject) => {
      provider.once("error", reject); provider.listen(0, "127.0.0.1", resolve);
    });
    await withOwnedCase({ processes: scope, run: async () => {
      step = "admit original native OpenCode version";
      const version = await run(opencode, ["--version"]);
      assert.equal(version.code, 0, version.stderr); assert.equal(version.stdout.trim(), "1.19.5");
      step = "install the shipping OpenCode extension";
      const staged = await nativePackage(binary, join(root, "package with spaces"))();
      const installed = await run(staged.executable, ["install", "opencode", "--plugin-root", staged.packageRoot]);
      assert.equal(installed.code, 0, installed.stderr);
      step = "start the original shared workspace owner";
      await startWorkspaceOwner({ scope, binary: graphcoder, workspace, state: join(root, "state"), root, env,
        capture: result => t.diagnostic(JSON.stringify({ role: "owner", ...result })) });
      const request = action => run(staged.executable, ["__workspace"], JSON.stringify(action));
      const opened = await request({ kind: "open", host: "opencode", session: "native-workspace" });
      assert.equal(opened.code, 0, opened.stderr);
      const { task } = JSON.parse(opened.stdout);
      Object.assign(env, { ACYCLIC_TASK: task, ACYCLIC_WORKSPACE: workspace });
      await writeFile(join(workspace, "opencode.json"), JSON.stringify({
        enabled_providers: ["fixture"], small_model: "fixture/tiny",
        provider: { fixture: { npm: "@ai-sdk/openai-compatible", name: "Recorded conformance",
          options: { baseURL: `http://127.0.0.1:${provider.address().port}/v1` },
          models: { tiny: { name: "tiny", limit: { context: 4096, output: 100 } } } } },
      }));
      step = "complete the native local turn with the installed extension";
      const completed = await run(opencode, ["run", "--format", "json", "--model", "fixture/tiny", "Reply with a greeting."]);
      assert.equal(completed.code, 0, completed.stderr);
      const events = completed.stdout.trim().split("\n").map(line => JSON.parse(line));
      const terminal = events.findLast(event => event.type === "step_finish");
      assert.equal(terminal?.part.reason, "stop", "native completed terminal absent");
      assert.equal(events.filter(event => event.type === "text").map(event => event.part.text).join(""), "Hello! 😊");
      assert.ok(requests.some(request => request.body.model === "tiny" &&
        JSON.stringify(request.body.messages).includes("Reply with a greeting.")), "original native provider request absent");
      step = "join the native session identity through the original shared writer";
      const recorded = await request({ kind: "hook", host: "opencode", event: "NativeSession", input: { task } });
      assert.equal(recorded.code, 0, "native OpenCode session was not recorded through shared workspace: " + recorded.stderr);
      assert.equal(JSON.parse(recorded.stdout).id, terminal.sessionID);
      step = "capture after the physically retired native process";
      const captured = await request({ kind: "capture", task });
      assert.equal(captured.code, 0, captured.stderr);
    } });
  } catch (cause) {
    throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
  } finally {
    await scope.close();
    if (provider) { provider.closeAllConnections(); await new Promise(resolve => provider.close(resolve)); }
    if (root) await rm(root, { recursive: true });
  }
});
