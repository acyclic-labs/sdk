import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { chmod, copyFile, mkdir, readFile, readdir, rename, rm, rmdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { delimiter, dirname, isAbsolute, join } from "node:path";
import test from "node:test";
import { McpClient } from "../../shared/mcp-client.mjs";
import { Processes, Spool } from "../../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../../scripts/graphcoder-e2e/fixture-directory.mjs";
import { environment, nativeEnvironment } from "../../../scripts/graphcoder-e2e/scratch.mjs";
import { withOwnedCase } from "../../../scripts/graphcoder-e2e/suite.mjs";
import { nativePackage } from "./native-model.mjs";
import { startWorkspaceOwner } from "./workspace-owner.mjs";

const recovery = "Set ACYCLIC_TEST_BINARY, E2E_GRAPHCODER, E2E_AGY and E2E_MOCK_MODEL to matching absolute executables; run node --test --test-concurrency=1 plugin/tests/hosts/agy-capture-admission.test.mjs";

// The native host produces the hooks, requests and terminals. Authentication
// refusal and the shared model server exercise both admitted outcomes locally.
test("native AGY fences capture, executes tools and settles managed children", async t => {
  const binaries = ["ACYCLIC_TEST_BINARY", "E2E_GRAPHCODER", "E2E_AGY", "E2E_MOCK_MODEL"].map(name => {
    const path = process.env[name];
    assert.ok(isAbsolute(path ?? ""), `step=admit ${name}; recovery=${recovery}`);
    return path;
  });
  const [binary, graphcoder, nativeAgy, mock] = binaries;
  let root, provider, retained, succeeded = false;
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
      assert.equal(version.stdout.trim(), "1.5.3", "use the qualified native release");
      step = "install the shipping package through the native plugin manager";
      const staged = await nativePackage(binary, join(root, "package with spaces"))();
      const refusedVersion = await run(staged.executable, ["install", "agy", "--host-version", "1.5.2", "--plugin-root", staged.packageRoot]);
      assert.notEqual(refusedVersion.code, 0, "installer accepted an unqualified native release");
      assert.match(refusedVersion.stderr, /installed=1\.5\.3, required=1\.5\.2/);
      const installed = await run(staged.executable, ["install", "agy", "--host-version", "1.5.3", "--plugin-root", staged.packageRoot]);
      assert.equal(installed.code, 0, installed.stderr);
      const receipt = installed.stdout.split(/\r?\n/).filter(line => line.startsWith("{")).map(line => JSON.parse(line))
        .find(record => record.installed_native_host)?.installed_native_host;
      assert.deepEqual(receipt, { version: 1, host: "agy", native_version: "1.5.3" });
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
      let requests = 0, nativePath;
      provider = createServer((request, response) => {
        requests++;
        nativePath = new URL(request.url, "http://127.0.0.1").pathname;
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
        "--output-format", "stream-json", "--sandbox", "--mode", "accept-edits",
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
      assert.match(nativePath, /^\/[^/]+\/models\/[^/]+:streamGenerateContent$/);

      step = "start the shared native model producer";
      const prompt = "Write the native AGY tool marker, then return the completion marker.", answer = "native-agy-completed";
      const written = join(workspace, "native-tool.txt"), contents = "native-agy-tool-write";
      const children = ["first", "second"].map(reference => ({ reference,
        prompt: `Return the native AGY ${reference} child marker.`, answer: `native-agy-${reference}-child` }));
      const coordinationPrompt = "Spawn the managed native AGY child, wait for its reply, then return the coordination marker.";
      const childPrompt = "Return the model-coordinated native AGY child marker.", childAnswer = "native-agy-coordinated-child";
      const coordinationAnswer = "native-agy-coordination-completed";
      const delegationPrompt = "Try native delegation, then return the refusal marker.", delegationAnswer = "native-delegation-fenced";
      const childCommand = process.platform === "win32"
        ? "Set-Content -NoNewline -LiteralPath native-command.txt -Value native-agy-command-write"
        : "printf native-agy-command-write > native-command.txt";
      const deniedCommand = process.platform === "win32"
        ? "Set-Content -NoNewline -LiteralPath native-tool.txt -Value forbidden-write"
        : "printf forbidden-write > native-tool.txt";
      const coordinate = (id, name, Arguments) => ({ id, name: "call_mcp_tool",
        arguments: { ServerName: "acyclic_sessions", ToolName: name, Arguments } });
      const script = join(root, "model.json"), recorded = join(root, "model-requests.jsonl");
      const namespace = nativePath.split("/")[1];
      await writeFile(script, JSON.stringify({ cases: { [namespace]: { steps: [
        { when: { user: prompt }, tools: [{ id: "native-write", name: "write_file",
          arguments: { Path: written, Content: contents } }] },
        { when: { contains: "\"functionResponse\"" }, text: [answer] },
        ...children.map(child => ({ when: { user: child.prompt }, text: [child.answer] })),
        { when: { user: coordinationPrompt }, tools: [coordinate("native-spawn", "spawn",
          { reference: "model-child", prompt: childPrompt })] },
        { when: { user: childPrompt }, tools: [{ id: "child-command", name: "run_command",
          arguments: { CommandLine: childCommand, toolSummary: "Write child marker", WaitMsBeforeAsync: 500 } }] },
        { when: { user: childPrompt, contains: "\"functionResponse\"" }, tools: [{ id: "denied-command", name: "run_command",
          arguments: { CommandLine: deniedCommand, toolSummary: "Check native denied file", WaitMsBeforeAsync: 500 } }] },
        { when: { user: childPrompt, contains: "\"functionResponse\"" }, tools: [{ id: "child-write", name: "write_file",
          arguments: { Path: { $request_line: { pointer: "/systemInstruction/parts/0/text",
            prefix: "Command Working Directory: ", suffix: "/native-child.txt" } }, Content: "native-agy-child-write" } }] },
        { when: { user: childPrompt, contains: "\"functionResponse\"" }, text: [childAnswer] },
        { when: { user: coordinationPrompt, tool_result: "native:spawn" },
          tools: [coordinate("native-wait", "wait", { reference: "model-child" })] },
        { when: { user: coordinationPrompt, tool_result: childAnswer }, text: [coordinationAnswer] },
        // Arguments follow the original native invoke_subagent declaration
        // captured by the first real model exchange in this conformance path.
        { when: { user: delegationPrompt }, tools: [{ id: "native-delegation", name: "invoke_subagent",
          arguments: { TypeName: "self", Role: "Refusal Control", Prompt: "Return the unintended delegation marker." } }] },
        { when: { user: delegationPrompt, contains: "step=admit native delegation" }, text: [delegationAnswer] },
      ] } } }));
      const model = scope.start(mock, ["--script", script, "--record", recorded], { env });
      const modelEvents = new Spool(model.child.stdout, model.closed, {
        key: event => event.event === "ready" ? ["ready"] : [], unique: true,
      });
      const { port } = await modelEvents.keyed("ready");
      env.GOOGLE_GEMINI_BASE_URL = `http://127.0.0.1:${port}`;
      step = "complete an original native tool turn after successful capture";
      const toolArgs = [`--print=${prompt}`, ...args.slice(1)];
      toolArgs[toolArgs.indexOf("--mode") + 1] = "accept-edits";
      const completed = await invoke(toolArgs);
      const calls = (await readFile(recorded, "utf8")).trim().split(/\r?\n/).map(line => JSON.parse(line));
      t.diagnostic(JSON.stringify({ role: "model", requests: calls }));
      assert.equal(completed.code, 0, completed.stderr + completed.stdout);
      const terminal = completed.stdout.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
        .find(event => event.event === "result")?.result;
      assert.equal(terminal?.status, "SUCCESS", "native AGY omitted its successful terminal");
      assert.equal(terminal.response.trimEnd(), answer);
      assert.equal(terminal.error, undefined);
      assert.deepEqual(calls.map(call => [call.status, call.step]), [[200, 0], [200, 1]],
        "native AGY must complete the original tool exchange before its final model step");
      const returned = calls[1].body.contents.flatMap(content => content.parts)
        .find(part => part.functionResponse)?.functionResponse;
      assert.equal(returned?.name, "write_file");
      assert.equal(await readFile(written, "utf8"), contents, "native AGY wrote outside its admitted workspace");

      step = "settle two original native children through the shared process owner";
      env.ACYCLIC_HOST_ARGS = JSON.stringify(["--sandbox", "--mode", "accept-edits", "--print-timeout", "0",
        "--settings", JSON.stringify({ modelProvider: "gemini", permissions: {
          allow: ["write_file(.)", `command(${childCommand})`, `command(${deniedCommand})`],
          deny: ["write_file(native-tool.txt)"],
        } })]);
      const bridge = scope.start(staged.executable, ["__mcp", "agy"], { cwd: workspace, env });
      const client = new McpClient(bridge.child, { name: "native-agy-conformance", version: "1", recovery });
      const call = async (name, args) => {
        const result = await client.request(name, args);
        t.diagnostic(JSON.stringify({ role: "coordination", name, args, result }));
        assert.ok(!result.isError, JSON.stringify(result));
        return JSON.parse(result.content[0].text);
      };
      try {
        const spawned = [];
        for (const child of children) spawned.push(await call("spawn", {
          reference: child.reference, prompt: child.prompt,
        }));
        assert.notEqual(spawned[0].cwd, spawned[1].cwd, "managed children shared a native working directory");
        for (const child of spawned) assert.notEqual(child.cwd, workspace);
        const replies = await Promise.all(children.map(child => call("wait", { reference: child.reference })));
        for (const [index, reply] of replies.entries()) {
          assert.equal(reply.reply.kind, "answered", JSON.stringify(reply));
          assert.equal(reply.reply.outcome.Succeeded?.text.trimEnd(), children[index].answer, JSON.stringify(reply));
          assert.equal(reply.status.settled, true, "native reply preceded physical process settlement");
        }
      } finally {
        bridge.input.end();
        const result = await bridge.closed;
        t.diagnostic(JSON.stringify({ role: "coordination-owner", ...result }));
        assert.equal(result.code, 0, result.stderr);
      }
      const allCalls = (await readFile(recorded, "utf8")).trim().split(/\r?\n/).map(line => JSON.parse(line));
      t.diagnostic(JSON.stringify({ role: "model-with-children", requests: allCalls }));
      assert.deepEqual(allCalls.map(call => [call.status, call.step]).sort((a, b) => a[1] - b[1]),
        [[200, 0], [200, 1], [200, 2], [200, 3]], "each original native child must complete one model request");
      step = "complete native model-issued coordination through the installed MCP adapter";
      const coordinatedArgs = [...toolArgs]; coordinatedArgs[0] = `--print=${coordinationPrompt}`;
      // Headless native MCP calls require explicit user policy, independently
      // of edit approval. Grant only these two coordination operations.
      coordinatedArgs[coordinatedArgs.indexOf("--settings") + 1] = JSON.stringify({ modelProvider: "gemini",
        permissions: { allow: ["mcp(acyclic_sessions/spawn)", "mcp(acyclic_sessions/wait)"] } });
      const coordinated = await invoke(coordinatedArgs);
      const coordinatedCalls = (await readFile(recorded, "utf8")).trim().split(/\r?\n/).map(line => JSON.parse(line));
      t.diagnostic(JSON.stringify({ role: "model-coordination", requests: coordinatedCalls }));
      assert.equal(coordinated.code, 0, coordinated.stderr + coordinated.stdout);
      const coordinatedTerminal = coordinated.stdout.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
        .find(event => event.event === "result")?.result;
      assert.equal(coordinatedTerminal?.status, "SUCCESS", JSON.stringify(coordinatedTerminal));
      assert.equal(coordinatedTerminal.response.trimEnd(), coordinationAnswer);
      assert.deepEqual(coordinatedCalls.map(call => [call.status, call.step]).sort((a, b) => a[1] - b[1]),
        Array.from({ length: 11 }, (_, step) => [200, step]), "native spawn and wait must each return before the final model step");
      const waitResponse = coordinatedCalls.at(-1).body.contents.flatMap(content => content.parts)
        .findLast(part => part.functionResponse?.id === "native-wait").functionResponse.response.output;
      const childReply = JSON.parse(waitResponse.trimEnd().split("\n").at(-1));
      assert.equal(childReply.status.settled, true);
      const childRequest = coordinatedCalls.find(call => call.step === 5);
      assert.ok(!childRequest.body.systemInstruction.parts.some(part =>
        part.text?.includes(`Command Working Directory: ${workspace}\n`)), "native child selected the parent workspace as its command cwd");
      const childWriteResult = coordinatedCalls.find(call => call.step === 8).body.contents
        .flatMap(content => content.parts).findLast(part => part.functionResponse?.id === "child-write")?.functionResponse;
      assert.equal(childWriteResult?.name, "write_file");
      assert.ok(!childWriteResult?.response.error, JSON.stringify(childWriteResult));
      const reopened = await request({ kind: "reopen", task: childReply.task });
      assert.equal(await readFile(join(reopened.cwd, "native-child.txt"), "utf8"), "native-agy-child-write",
        "writing native child did not capture its file in the admitted view");
      const commandResult = coordinatedCalls.find(call => call.step === 6).body.contents
        .flatMap(content => content.parts).findLast(part => part.functionResponse?.id === "child-command")?.functionResponse;
      assert.match(JSON.stringify(commandResult?.response), /Command exited with code 0/, JSON.stringify(commandResult));
      assert.equal(await readFile(join(reopened.cwd, "native-command.txt"), "utf8"), "native-agy-command-write");
      const deniedResult = coordinatedCalls.find(call => call.step === 7).body.contents
        .flatMap(content => content.parts).findLast(part => part.functionResponse?.id === "denied-command")?.functionResponse;
      assert.match(JSON.stringify(deniedResult?.response), /denied|not permitted|Command exited with code [1-9]/i,
        "native deny rule did not return its original command refusal");
      assert.equal(await readFile(join(reopened.cwd, "native-tool.txt"), "utf8"), contents,
        "relative native deny rule was lost in the child view");
      step = "refuse native delegation through the installed pre-tool hook";
      const delegationArgs = [...toolArgs]; delegationArgs[0] = `--print=${delegationPrompt}`;
      const delegation = await invoke(delegationArgs);
      assert.equal(delegation.code, 0, delegation.stderr + delegation.stdout);
      const delegationResult = delegation.stdout.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
        .find(event => event.event === "result")?.result;
      assert.equal(delegationResult?.response.trimEnd(), delegationAnswer);
      const finalCalls = (await readFile(recorded, "utf8")).trim().split(/\r?\n/).map(line => JSON.parse(line));
      assert.deepEqual(finalCalls.map(call => [call.status, call.step]).sort((a, b) => a[1] - b[1]),
        Array.from({ length: 13 }, (_, step) => [200, step]), "native delegation must be refused before a native child model request");
      t.diagnostic(JSON.stringify({ role: "native-delegation-refusal", requests: finalCalls.slice(-2) }));
      step = "inspect native profile session storage after physical settlement";
      const profileFiles = await readdir(join(profile, ".gemini"), { recursive: true });
      t.diagnostic(JSON.stringify({ role: "native-profile-paths", files: profileFiles }));
      const nativeProfile = join(profile, ".gemini", "antigravity-cli");
      assert.ok(profileFiles.includes("antigravity-cli"), "native plugin/session profile escaped the trial HOME");
      const persisted = await readdir(nativeProfile, { recursive: true });
      assert.ok(persisted.some(path => path.includes(coordinatedTerminal.conversation_id)),
        "original native conversation absent from trial profile");
      succeeded = true;
    } });
  } catch (cause) {
    throw new Error(`step=${step}: ${cause.message}; recovery=${recovery}`, { cause });
  } finally {
    try {
      await scope.close();
      if (provider) { provider.closeAllConnections(); await new Promise(resolve => provider.close(resolve)); }
      // Failure retains the original native logs after physical retirement.
      if (root && !retained && succeeded) await rm(root, { recursive: true });
      else if (root) t.diagnostic(JSON.stringify({ retained_native_fixture: root }));
    } catch (cause) {
      throw new Error(`step=retire native capture fixture: ${cause.message}; recovery=${recovery}`, { cause });
    }
  }
});
