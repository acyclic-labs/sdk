import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmod, copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { delimiter, dirname, isAbsolute, join } from "node:path";
import { createServer } from "node:http";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { Processes, Spool } from "../../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { environment, nativeEnvironment } from "../../../scripts/graphcoder-e2e/scratch.mjs";
import { withOwnedCase } from "../../../scripts/graphcoder-e2e/suite.mjs";
import { nativePackage } from "./native-model.mjs";
import { startWorkspaceOwner } from "./workspace-owner.mjs";

const recovery = "Set ACYCLIC_TEST_BINARY, E2E_GRAPHCODER and E2E_OPENCODE and E2E_MOCK_MODEL to matching absolute executables; run node --test --test-concurrency=1 plugin/tests/hosts/opencode-workspace.test.mjs";

test("installed native OpenCode records its original session through the shared workspace", async t => {
  const [binary, graphcoder, native, mock] = ["ACYCLIC_TEST_BINARY", "E2E_GRAPHCODER", "E2E_OPENCODE", "E2E_MOCK_MODEL"].map(name => {
    const path = process.env[name];
    assert.ok(isAbsolute(path ?? ""), `step=admit ${name}; recovery=${recovery}`);
    return path;
  });
  const scope = new Processes();
  let root, provider, succeeded = false, step = "admit original recorded provider bytes";
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
    const config = join(profile, "explicit opencode config");
    await mkdir(config);
    env.OPENCODE_CONFIG_DIR = config;
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
      await writeFile(join(config, "opencode.json"), JSON.stringify({
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
      step = "recover the installed admission after an actual workspace refusal";
      const exported = await run(opencode, ["export", terminal.sessionID]);
      assert.equal(exported.code, 0, exported.stderr);
      const info = JSON.parse(exported.stdout).info;
      assert.equal(info.id, terminal.sessionID);
      const admission = await run(process.execPath, [
        fileURLToPath(new URL("./opencode-admission.mjs", import.meta.url)),
        join(config, "plugins", "acyclic.js"), workspace, JSON.stringify(info),
      ]);
      assert.equal(admission.code, 0, admission.stderr);
      step = "exercise native MCP spawn with the same isolated provider profile";
      t.diagnostic(JSON.stringify({ role: "native-provider-requests", requests }));
      const toolNames = requests.find(request => request.body.tools?.length)?.body.tools.map(tool => tool.function.name) ?? [];
      assert.ok(!toolNames.includes("task"), "installed plugin exposed native delegation outside the shared owner");
      const tool = name => {
        const matches = toolNames.filter(tool => tool === `acyclic-sessions_${name}`);
        assert.equal(matches.length, 1, "installed native MCP tool absent: " + name);
        return matches[0];
      };
      const prompt = "Spawn the writing OpenCode child, wait for its result, then return the parent marker.";
      const childPrompt = "Write the OpenCode child marker file, then return the child marker.";
      const childAnswer = "native-opencode-child-completed", answer = "native-opencode-parent-completed";
      const script = join(root, "model.json"), record = join(root, "model-requests.jsonl");
      await writeFile(script, JSON.stringify({ cases: { native: { steps: [
        ...Array.from({ length: 2 }, () => ({ when: { contains: "Generate a title for this conversation:" }, text: ["Native coordination"] })),
        { when: { user: prompt }, tools: [{ id: "native-spawn", name: tool("spawn"),
          arguments: { reference: "writing-child", prompt: childPrompt } }] },
        { when: { user: childPrompt }, tools: [{ id: "child-write", name: "write",
          arguments: { filePath: "native-child.txt", content: "native-opencode-child-write" } }] },
        { when: { user: childPrompt, tool_result: "Wrote file successfully" }, text: [childAnswer] },
        { when: { user: prompt, tool_result: "native:spawn" }, tools: [{ id: "native-wait", name: tool("wait"),
          arguments: { reference: "writing-child" } }] },
        { when: { user: prompt, tool_result: childAnswer }, text: [answer] },
      ] } } }));
      const model = scope.start(mock, ["--script", script, "--record", record], { env });
      const modelEvents = new Spool(model.child.stdout, model.closed, {
        key: event => event.event === "ready" ? ["ready"] : [], unique: true,
      });
      const { port } = await modelEvents.keyed("ready");
      const configured = JSON.parse(await readFile(join(config, "opencode.json"), "utf8"));
      configured.provider.fixture.options.baseURL = `http://127.0.0.1:${port}/native/v1`;
      configured.permission = { edit: "allow", [tool("spawn")]: "allow", [tool("wait")]: "allow" };
      await writeFile(join(config, "opencode.json"), JSON.stringify(configured));
      env.ACYCLIC_HOST_ARGS = JSON.stringify(["--model", "fixture/tiny"]);
      const coordinated = await run(opencode, ["run", "--format", "json", "--model", "fixture/tiny", prompt]);
      assert.equal(coordinated.code, 0, coordinated.stderr + coordinated.stdout);
      assert.ok(coordinated.stdout.includes(answer), "native parent omitted the completed child reply");
      const calls = (await readFile(record, "utf8")).trim().split(/\r?\n/).map(line => JSON.parse(line));
      t.diagnostic(JSON.stringify({ role: "native-model-coordination", requests: calls }));
      assert.deepEqual(calls.map(call => [call.status, call.step]).sort((a, b) => a[1] - b[1]),
        Array.from({ length: 7 }, (_, step) => [200, step]), "native child dropped the selected provider configuration");
      const waited = calls.at(-1).body.messages.findLast(message => message.role === "tool" && message.tool_call_id === "native-wait");
      assert.ok(waited, "native wait response absent");
      const childReply = JSON.parse(waited.content);
      assert.equal(childReply.status.settled, true);
      const reopened = await request({ kind: "reopen", task: childReply.task });
      assert.equal(reopened.code, 0, reopened.stderr);
      assert.equal(await readFile(join(JSON.parse(reopened.stdout).cwd, "native-child.txt"), "utf8"), "native-opencode-child-write");
      const session = await request({ kind: "hook", host: "opencode", event: "NativeSession", input: { task: childReply.task } });
      assert.equal(session.code, 0, session.stderr);
      const childId = JSON.parse(session.stdout).id;
      const childExport = await run(opencode, ["export", childId]);
      assert.equal(childExport.code, 0, "child session did not remain in the inherited trial data directory: " + childExport.stderr);
      t.diagnostic(JSON.stringify({ role: "native-child-session", info: JSON.parse(childExport.stdout).info }));
      const paths = await run(opencode, ["debug", "paths"]);
      assert.equal(paths.code, 0, paths.stderr);
      t.diagnostic(JSON.stringify({ role: "native-profile-paths", stdout: paths.stdout }));
      step = "capture after the physically retired native process";
      const captured = await request({ kind: "capture", task });
      assert.equal(captured.code, 0, captured.stderr);
      succeeded = true;
    } });
  } catch (cause) {
    throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
  } finally {
    await scope.close();
    if (provider) { provider.closeAllConnections(); await new Promise(resolve => provider.close(resolve)); }
    if (root && succeeded) await rm(root, { recursive: true });
    else if (root) t.diagnostic(JSON.stringify({ retained_native_fixture: root }));
  }
});
