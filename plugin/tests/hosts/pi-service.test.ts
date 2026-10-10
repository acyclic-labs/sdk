import assert from "node:assert/strict";
import { execFile, spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, watch, writeFileSync, type FSWatcher } from "node:fs";
import { isAbsolute, join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { invokePi, piEnvironment } from "./pi-fixture.ts";

const execute = promisify(execFile);
const provider = `
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  const faux = fauxProvider({provider:"acyclic-service-qualification",models:[{id:"probe"},{id:"switched"}]});
  faux.setResponses([
    fauxAssistantMessage(fauxToolCall("acyclic_agents", {}, {id:"observe-original-task"}), {stopReason:"toolUse"}),
    context => {
      const message = context.messages.filter(message => message.role === "toolResult").at(-1);
      if (!message || message.isError) throw new Error("actual Acyclic task control failed");
      return fauxAssistantMessage(message.content.map(part => part.type === "text" ? part.text : "").join(""));
    },
  ]);
  pi.registerProvider(faux.provider);
}
`;

// This exercises installed-extension root admission and native service restart,
// not workspace allocation, child process execution or effect qualification.
test("installed Pi controls retain one authenticated root across service death and native model switch", { timeout: 120_000 }, async () => {
  const requested = process.env.ACYCLIC_BINARY;
  assert.ok(requested && isAbsolute(requested), "ACYCLIC_BINARY must name the final built CLI");
  const binary = realpathSync(requested);
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-service-", import.meta.url)));
  const project = join(directory, "project");
  mkdirSync(project);
  mkdirSync(join(directory, "agent"));
  writeFileSync(join(project, "user.txt"), "concurrent user content");
  const environment = piEnvironment(directory);
  const command = (args: string[], input = "") => {
    const invocation = execute(binary, args, {cwd:project, env:environment, windowsHide:true,
      timeout:30_000, maxBuffer:4 * 1024 * 1024, encoding:"utf8"});
    invocation.child.stdin?.end(input);
    return invocation;
  };
  let service: ChildProcess | undefined;
  const start = async () => {
    const data = process.platform === "win32"
      ? join(directory, "local", "Acyclic", "state-v5")
      : join(directory, "state", "acyclic", "state-v5");
    mkdirSync(data, {recursive:true});
    const marker = join(data, "service.identity");
    const before = existsSync(marker) ? readFileSync(marker, "utf8") : "";
    let watcher: FSWatcher | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        // The production marker is published after the transport is ready.
        // Await that publication, not a guessed scheduling delay.
        watcher = watch(data, (_event, filename) => {
          if (String(filename) === "service.identity" && existsSync(marker)
            && readFileSync(marker, "utf8") !== before) resolve();
        });
        service = spawn(binary, ["__service"], {cwd:project, env:environment, windowsHide:true, stdio:"ignore"});
        service.once("error", reject);
        service.once("exit", code => reject(new Error(`owned service exited before readiness: ${code}`)));
      });
    } finally {
      watcher?.close();
    }
    const status = JSON.parse((await command(["__service-status"])).stdout);
    assert.equal(typeof status.reachableIdentity, "string");
    assert.ok(status.reachableIdentity.length > 0);
    assert.equal(status.reachableIdentity, status.markerIdentity);
    assert.equal(status.lockAcquirable, false);
  };
  const stop = async () => {
    if (!service || service.exitCode !== null) return;
    const exited = once(service, "exit");
    service.kill("SIGKILL");
    await exited;
    service = undefined;
  };
  try {
    await command(["install", "pi"]);
    const extension = JSON.parse(readFileSync(join(directory, "agent", "settings.json"), "utf8")).extensions[0];
    assert.ok(isAbsolute(extension));
    assert.ok(readFileSync(extension, "utf8").includes(JSON.stringify(binary)), "extension must invoke this source-bound binary");
    const fixture = join(directory, "provider.ts");
    writeFileSync(fixture, provider);
    const manager = SessionManager.create(project, directory);
    manager.appendMessage({role:"user", content:"original native service instruction", timestamp:1});
    const session = manager.getSessionFile()!;
    assert.ok(session);
    const args = ["--print", "--mode", "text", "--session", session,
      "--provider", "acyclic-service-qualification", "--no-extensions", "--no-mcp", "--no-builtin-tools",
      "-e", extension, "-e", fixture];
    await start();
    let original: unknown;
    for (const model of ["probe", "switched"]) {
      const result = await invokePi([...args, "--model", model, "Observe only the original task."], project, directory);
      const observed = JSON.parse(result.stdout.trim());
      assert.equal(observed.session, manager.getSessionId());
      assert.equal(observed.tasks.length, 1);
      assert.equal(observed.tasks[0].state, "pending");
      if (original === undefined) original = observed;
      else assert.deepEqual(observed, original, "restart/model selection must not admit another root");
      assert.equal(readFileSync(join(project, "user.txt"), "utf8"), "concurrent user content");
      await stop();
      if (model === "probe") await start();
    }
    await start();
    const observation = original as {tasks:Array<{task:string}>};
    const cancelled = JSON.parse((await command(["__pi", "TaskCancel"], JSON.stringify({
      cwd:project, session_id:manager.getSessionId(), task:observation.tasks[0].task,
    }))).stdout);
    assert.equal(cancelled.tasks[0].state, "outcome");
    assert.equal(cancelled.tasks[0].task, observation.tasks[0].task);
    await command(["__service-drain"]);
    if (service && service.exitCode === null) await once(service, "exit");
    service = undefined;
  } finally {
    await stop();
    rmSync(directory, {recursive:true});
  }
});
