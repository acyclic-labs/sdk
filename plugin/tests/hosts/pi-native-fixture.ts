import assert from "node:assert/strict";
import { execFile, spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, readlinkSync, realpathSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { promisify } from "node:util";
import { VERSION, SessionManager } from "@earendil-works/pi-coding-agent";
import { invokePi, nodeExecutable, pinnedPiCli, piEnvironment } from "./pi-fixture.ts";

const execute = promisify(execFile);

/** Install Pi and own its service process. */
export async function installedNativeFixture(
  directory: string, providerSource: string, nativeConfiguration: Readonly<Record<string, string>>,
  commandTimeout = 60_000,
) {
  assert.equal(VERSION, "1.1.0");
  const requested = process.env.ACYCLIC_BINARY;
  assert.ok(requested && isAbsolute(requested), "ACYCLIC_BINARY must select the final source-bound CLI");
  const binary = realpathSync(requested);
  const project = join(directory, "project");
  mkdirSync(project);
  mkdirSync(join(directory, "agent"));
  writeFileSync(join(project, "seed.txt"), "original source\n");
  writeFileSync(join(project, "concurrent.txt"), "preserve concurrent root edit\n");
  const environment = { ...piEnvironment(directory), ...nativeConfiguration };
  const command = (args: string[], input = "") => {
    const invocation = execute(binary, args, { cwd: project, env: environment, windowsHide: true,
      timeout: commandTimeout, maxBuffer: 4 * 1024 * 1024, encoding: "utf8" });
    invocation.child.stdin?.end(input);
    return invocation;
  };
  await command(["install", "pi"]);
  const extension = JSON.parse(readFileSync(join(directory, "agent", "settings.json"), "utf8")).extensions[0];
  assert.ok(isAbsolute(extension));
  assert.ok(readFileSync(extension, "utf8").includes(JSON.stringify(binary)));
  const provider = join(directory, "provider.ts");
  writeFileSync(provider, providerSource);
  environment.ACYCLIC_PI_PROVIDER_EXTENSION = provider;
  const manager = SessionManager.create(project, directory);
  manager.appendMessage({ role: "user", content: "original native recursive instruction ROOT_ONLY_NATIVE_INSTRUCTION", timestamp: 1 });
  const session = manager.getSessionFile();
  assert.ok(session);
  let service: ChildProcess | undefined;
  const start = async () => {
    const {promise, resolve, reject} = Promise.withResolvers<void>();
    const timeout = setTimeout(() => reject(new Error("SDK owner did not signal readiness")), 3000);
    try {
      service = spawn(binary, ["__service", "--signal-ready"], {
        cwd: project, env: environment, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
      });
      service.stdout!.once("data", () => resolve());
      service.stderr!.on("data", chunk => process.stderr.write(chunk));
      service.once("error", reject);
      service.once("exit", code => reject(new Error(`original native service exited before readiness: ${code}`)));
      await promise;
    } finally { clearTimeout(timeout); }
    // An ordinary hook proves authenticated append/follow after readiness.
    const answer = JSON.parse((await command(["__hook", "pi", "session_start"], JSON.stringify({
      cwd:project,sessionId:manager.getSessionId(),
    }))).stdout);
    const attached = answer;
    assert.equal(typeof attached.task,"string");
    environment.ACYCLIC_NATIVE_TASK = attached.task;
  };
  const servicePid = () => service?.pid;
  const resources = () => {
    if (process.platform !== "linux") return { mounts:[], processes:[] };
    const mounts=readFileSync("/proc/self/mountinfo","utf8").split("\n")
      .filter(line=>line.split(" ")[4]?.startsWith(`${directory}/`));
    const processes: Array<{pid:string,parent:string,cwd:string}> = [];
    for (const pid of readdirSync("/proc").filter(name=>/^\d+$/.test(name))) {
      try {
        const cwd=readlinkSync(`/proc/${pid}/cwd`);
        if (cwd === directory || cwd.startsWith(`${directory}/`)) {
          const parent=readFileSync(`/proc/${pid}/status`,"utf8").match(/^PPid:\s+(\d+)/m)?.[1] ?? "unknown";
          processes.push({pid,parent,cwd});
        }
      } catch (error) {
        if (!(error instanceof Error && "code" in error && ["ENOENT","EACCES","EPERM"].includes(String(error.code)))) throw error;
      }
    }
    return {mounts,processes};
  };
  const kill = async () => {
    const current = service;
    if (!current || current.exitCode !== null || current.signalCode !== null) { service = undefined; return; }
    const stopped = waitForNativeServiceExit(current);
    if (!current.kill("SIGKILL")) {
      const originalFailure = new Error("failed to stop the original owned native service");
      try { await stopped; }
      catch (error) { throw new AggregateError([originalFailure, error], "native stop and actual exit observation failed", {cause:originalFailure}); }
      throw originalFailure;
    }
    await stopped;
    service = undefined;
  };
  const close = async () => {
    let originalFailure: unknown;
    const current = service;
    try {
      if (current && current.exitCode === null && current.signalCode === null) {
        await Promise.all([command(["__owner-stop"]), waitForNativeServiceExit(current)]);
        assert.equal(current.exitCode, 0, "original native service drain did not exit successfully");
      }
    } catch (error) { originalFailure = error; }
    try { await kill(); }
    catch (error) {
      if (originalFailure !== undefined) throw new AggregateError([originalFailure, error], "native drain and owned service termination failed", {cause:originalFailure});
      throw error;
    }
    if (originalFailure !== undefined) throw originalFailure;
  };
  const run = (prompt: string, model = "probe") => invokePi([
    "--print", "--mode", "text", "--session", session,
    "--provider", "acyclic-native-qualification", "--model", model,
    "--no-extensions", "--no-mcp", "--no-builtin-tools", "-e", extension, "-e", provider, "--", prompt,
  ], project, directory, environment);
  const runJson = async (prompt: string, onJsonLine: (line: string) => void, model = "probe") => {
    const invocation = execute(nodeExecutable, [pinnedPiCli,
      "--print", "--mode", "json", "--session", session,
      "--provider", "acyclic-native-qualification", "--model", model,
      "--no-extensions", "--no-mcp", "--no-builtin-tools", "-e", extension, "-e", provider, "--", prompt,
    ], {cwd:project,env:environment,windowsHide:true,timeout:180_000,maxBuffer:4*1024*1024,encoding:"utf8"});
    let pending = "";
    let callbackFailure: unknown;
    const consume = (chunk: string) => {
      if (callbackFailure !== undefined) return;
      pending += chunk;
      try {
        for (let end = pending.indexOf("\n"); end !== -1; end = pending.indexOf("\n")) {
          const line = pending.slice(0, end);
          pending = pending.slice(end + 1);
          if (line.trim()) onJsonLine(line);
        }
      } catch (error) {
        callbackFailure = error;
        invocation.child.kill();
      }
    };
    invocation.child.stdout?.setEncoding("utf8");
    invocation.child.stdout?.on("data", consume);
    invocation.child.stdin?.end();
    try {
      const result = await invocation;
      if (callbackFailure !== undefined) throw callbackFailure;
      if (pending.trim()) onJsonLine(pending);
      return result;
    } catch (error) {
      if (callbackFailure !== undefined && error !== callbackFailure) {
        throw new AggregateError([callbackFailure,error], "original native Root stream and owned CLI termination failed", {cause:callbackFailure});
      }
      throw error;
    } finally {
      invocation.child.stdout?.off("data", consume);
    }
  };
  return { project, environment, extension, provider, session, manager, command, start, kill, close, servicePid, resources, run, runJson };
}

function waitForNativeServiceExit(service: ChildProcess): Promise<void> {
  if (service.exitCode !== null || service.signalCode !== null) return Promise.resolve();
  const {promise, resolve, reject} = Promise.withResolvers<void>();
  const finish = (error?: Error) => {
    clearTimeout(timeout);
    service.off("exit", exited);
    service.off("error", failed);
    if (error) reject(error); else resolve();
  };
  const exited = () => finish();
  const failed = (error: Error) => finish(error);
  const timeout = setTimeout(() => finish(new Error("original native service failed to exit within the cleanup allowance")), 30_000);
  service.once("exit", exited);
  service.once("error", failed);
  return promise;
}
