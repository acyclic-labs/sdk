import assert from "node:assert/strict";
import { execFile, spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, realpathSync, watch, writeFileSync, type FSWatcher } from "node:fs";
import { isAbsolute, join } from "node:path";
import { promisify } from "node:util";
import { VERSION, SessionManager } from "@earendil-works/pi-coding-agent";
import { invokePi, nodeExecutable, pinnedPiCli, piEnvironment } from "./pi-fixture.ts";

const execute = promisify(execFile);

export interface InstalledNativeFixture {
  project: string;
  environment: NodeJS.ProcessEnv;
  extension: string;
  provider: string;
  session: string;
  manager: SessionManager;
  command(args: string[], input?: string): Promise<{stdout: string; stderr: string}>;
  start(): Promise<void>;
  kill(): Promise<void>;
  close(): Promise<void>;
  servicePid(): number | undefined;
  run(prompt: string, model?: string): Promise<{stdout: string; stderr: string}>;
  runJson(prompt: string, onJsonLine: (line: string) => void, model?: string): Promise<{stdout: string; stderr: string}>;
}

/** Install Pi and own its service process. */
export async function installedNativeFixture(
  directory: string, providerSource: string, nativeConfiguration: Readonly<Record<string, string>>,
  commandTimeout = 60_000,
): Promise<InstalledNativeFixture> {
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
  const data = process.platform === "win32" ? join(directory, "local", "Acyclic", "state-v5")
    : join(directory, "state", "acyclic", "state-v5");
  mkdirSync(data, { recursive: true });
  let service: ChildProcess | undefined;
  const start = async () => {
    const marker = join(data, "service.identity");
    const previous = existsSync(marker) ? readFileSync(marker, "utf8") : "";
    const {promise, resolve, reject} = Promise.withResolvers<void>();
    const timeout = setTimeout(() => reject(new Error("original native service did not publish readiness within its allowance")), 30_000);
    let watcher: FSWatcher | undefined;
    try {
      watcher = watch(data, (_event, filename) => {
        if (String(filename) === "service.identity" && existsSync(marker) && readFileSync(marker, "utf8") !== previous) resolve();
      });
      service = spawn(binary, ["__service"], { cwd: project, env: environment, windowsHide: true, stdio: "ignore" });
      service.once("error", reject);
      service.once("exit", code => reject(new Error(`original native service exited before readiness: ${code}`)));
      await promise;
    } finally { clearTimeout(timeout); watcher?.close(); }
    const status = JSON.parse((await command(["__service-status"])).stdout);
    assert.equal(typeof status.reachableIdentity, "string");
    assert.ok(status.reachableIdentity.length > 0);
    assert.equal(status.reachableIdentity, status.markerIdentity);
    assert.equal(status.lockAcquirable, false);
  };
  const servicePid = () => service?.pid;
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
        await Promise.all([command(["__service-drain"]), waitForNativeServiceExit(current)]);
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
  return { project, environment, extension, provider, session, manager, command, start, kill, close, servicePid, run, runJson };
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
