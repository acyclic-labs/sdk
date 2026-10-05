import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { spawn as spawnChild } from "node:child_process";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { retryOwnedProcessTermination, spawnOwnedProcess, terminateOwnedProcess, type OwnedProcessTermination } from "../src/owned-process.js";
import { PassThrough } from "node:stream";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { createNodeGraphCoderConnection as createConnection, JsonLineGraphCoderBridge, type GraphCoderProcessDiagnostic, type GraphCoderProcessBridgeOptions } from "../src/node.js";
import { GraphCoderTerminal } from "../src/terminal.js";
import type { GraphCoderWireRequest } from "../src/bridge.js";

// Native process startup can take several seconds on the Windows qualification
// lane while other SDK workers are compiling; keep the assertions bounded but
// avoid mistaking host scheduling pressure for a protocol failure.
setDefaultTimeout(30_000);

const childScript = `
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk.toString();
  for (;;) {
    const newline = buffer.indexOf("\\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line.trim()) continue;
    const request = JSON.parse(line);
    const delay = request.params && request.params.query && request.params.query.after ? Number(request.params.query.after) : 0;
    setTimeout(() => process.stdout.write(JSON.stringify({ request_id: request.request_id, ok: true, result: request.params }) + "\\n"), delay);
  }
});
`;

function request(id: string, delay = 0): GraphCoderWireRequest<"list_sessions"> {
  return { request_id: id, method: "list_sessions", params: delay === 0 ? { query: {} } : { query: { after: String(delay) } } };
}

function env(): NodeJS.ProcessEnv {
  return { PATH: process.env.PATH ?? "" };
}

function testRuntimeExecutable(): string {
  // Run bridge fixtures under Node on Windows. Bun's child wrapper can leave
  // the taskkill result without an exit code, which is not a stable fixture
  // for ordinary owner cleanup.
  return process.platform === "win32" ? "node" : process.execPath;
}

const activeBridges = new Set<JsonLineGraphCoderBridge>();

function longRunningCommand(): { readonly executable: string; readonly args: readonly string[] } {
  if (process.platform === "win32") {
    return {
      executable: process.env.ComSpec ?? "cmd.exe",
      args: ["/d", "/s", "/c", "ping -t 127.0.0.1 > NUL"],
    };
  }
  return { executable: testRuntimeExecutable(), args: ["-e", "setInterval(() => {}, 100000)"] };
}

function ownBridge(options: GraphCoderProcessBridgeOptions): JsonLineGraphCoderBridge {
  const bridge = new JsonLineGraphCoderBridge(options);
  activeBridges.add(bridge);
  return bridge;
}

function ownConnection(options: GraphCoderProcessBridgeOptions): ReturnType<typeof createConnection> {
  const connection = createConnection(options);
  activeBridges.add(connection.bridge);
  return connection;
}

afterEach(async () => {
  const bridges = [...activeBridges];
  await Promise.allSettled(bridges.map(async bridge => {
    bridge.close("test fixture cleanup");
    await bridge.waitForExit(2_000);
    activeBridges.delete(bridge);
  }));
  activeBridges.clear();
});

const runtimeScript = `
const snapshot = state => ({
  summary: { id: "session-1", title: "inspect", state, updated_at: "0", root_agent_id: "session-1" },
  agents: [{ id: "session-1", parent_id: null, task: "inspect", state: state === "cancelled" ? "cancelled" : "queued", depth: 0, children: [] }],
  workspace_generation: "1",
});
const approval = approved => ({ id: "approval-1", session_id: "session-1", agent_id: "session-1", operation_id: "op-runtime", action_digest: "digest", description: "approve", state: approved ? "approved" : "declined", created_at: "0" });
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk.toString();
  for (;;) {
    const newline = buffer.indexOf("\\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line.trim()) continue;
    const request = JSON.parse(line);
    let result;
    switch (request.method) {
      case "list_sessions": result = { items: [snapshot("idle").summary] }; break;
      case "start_session": result = snapshot("completed"); break;
      case "input_session": result = snapshot("completed"); break;
      case "open_session": result = snapshot("completed"); break;
      case "resume_session": result = snapshot("running"); break;
      case "cancel_session": result = snapshot("cancelled"); break;
      case "list_approvals": result = { session_id: "session-1", items: [approval(false)] }; break;
      case "operator_approve": result = {}; break;
      case "resolve_approval": result = approval(request.params.approved); break;
      default: result = { session_id: "session-1", items: [] };
    }
    process.stdout.write(JSON.stringify({ request_id: request.request_id, ok: true, result }) + "\\n");
  }
});
`;

async function waitForStableSize(path: string, stableMs = 150, maximumMs = 1_000): Promise<number> {
  const deadline = Date.now() + maximumMs;
  let previous = (await stat(path)).size;
  let stableSince = Date.now();
  while (Date.now() < deadline) {
    await new Promise<void>(resolve => setTimeout(resolve, 20));
    const current = (await stat(path)).size;
    if (current !== previous) {
      previous = current;
      stableSince = Date.now();
    } else if (Date.now() - stableSince >= stableMs) {
      return current;
    }
  }
  throw new Error(`file remained active: ${path}`);
}

async function waitForChildClose(child: ReturnType<typeof spawnOwnedProcess>, timeoutMs: number): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => finish(new Error("timed out waiting for fixture child close")), timeoutMs);
    const finish = (error?: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.removeListener("close", onClose);
      child.removeListener("error", onError);
      if (error === undefined) resolve();
      else reject(error);
    };
    const onClose = (): void => finish();
    const onError = (error: Error): void => finish(error);
    child.once("close", onClose);
    child.once("error", onError);
  });
}

describe("JSON-lines process bridge", () => {
  test("terminates descendants that retain the owned bridge pipes", async () => {
    const directory = await mkdtemp(join(tmpdir(), "graphcoder-owned-process-"));
    const marker = join(directory, "descendant-alive");
    const pidFile = join(directory, "descendant.pid");
    await writeFile(marker, "", "utf8");
    const descendant = "const fs = require('node:fs'); const marker = process.argv[1]; const pidFile = process.argv[2]; fs.writeFileSync(pidFile, String(process.pid)); setInterval(() => fs.appendFileSync(marker, 'x'), 20);";
    const systemRoot = process.env.SystemRoot ?? "";
    const detached = process.platform === "win32";
    const owner = `const fs = require('node:fs'); const { spawn } = require('node:child_process'); spawn(process.execPath, ['-e', ${JSON.stringify(descendant)}, process.argv[1], process.argv[2]], { detached: ${detached}, windowsHide: true, env: { PATH: process.env.PATH || '', SystemRoot: ${JSON.stringify(systemRoot)} }, stdio: ['ignore', 'inherit', 'inherit'] }); const deadline = Date.now() + 5000; const wait = setInterval(() => { if (fs.existsSync(process.argv[2]) || Date.now() >= deadline) { clearInterval(wait); process.exit(0); } }, 10);`;
    const outcomes: OwnedProcessTermination[] = [];
    let terminationResolve: ((outcome: OwnedProcessTermination) => void) | undefined;
    const terminationObserved = new Promise<OwnedProcessTermination>(resolve => { terminationResolve = resolve; });
    let descendantPid: number | undefined;
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", owner, marker, pidFile], env: env(), onDiagnostic: event => { if (event.kind === "termination") { outcomes.push(event.outcome); terminationResolve?.(event.outcome); } } });
    try {
      for (let attempt = 0; attempt < 50 && (await stat(marker)).size === 0; attempt += 1) {
        await new Promise<void>(resolve => setTimeout(resolve, 20));
      }
      const before = (await stat(marker)).size;
      expect(before).toBeGreaterThan(0);
      bridge.close("descendant cleanup");
      const termination = await terminationObserved;
      expect(outcomes).toHaveLength(1);
      expect(termination.pid).toBeGreaterThan(0);
      if (process.platform === "win32") {
        expect(termination.kind).toBe("unknown");
        await expect(bridge.waitForExit(250)).rejects.toMatchObject({ code: "transport" });
        descendantPid = Number(await readFile(pidFile, "utf8"));
        expect(Number.isSafeInteger(descendantPid)).toBe(true);
        try { process.kill(descendantPid); } catch { /* the fixture may have exited between observation and cleanup */ }
        await expect(bridge.waitForExit(2_000)).resolves.toMatchObject({ kind: "closed" });
      } else {
        expect(termination.kind).toBe("terminated");
        await expect(bridge.waitForExit(2_000)).resolves.toMatchObject({ kind: "closed" });
      }
      await expect(waitForStableSize(marker)).resolves.toBeGreaterThan(0);
    } finally {
      bridge.close("descendant cleanup fallback");
      if (process.platform === "win32" && descendantPid !== undefined) {
        try { process.kill(descendantPid); } catch { /* the fixture may have exited between observation and cleanup */ }
      }
      await bridge.waitForExit(2_000).catch(() => undefined);
      await rm(directory, { recursive: true, force: true });
    }
  });

  test("shares one bounded cleanup operation across repeated termination requests", async () => {
    // Bun's Windows child wrapper does not expose the native process handle
    // needed to classify taskkill's result. The installed Node lane covers
    // the Windows terminated outcome; this unit test covers the Unix owner.
    if (process.platform === "win32") return;
    const command = longRunningCommand();
    const child = spawnOwnedProcess(command.executable, command.args, { env: env(), stdio: "ignore" });
    try {
      const first = terminateOwnedProcess(child, 50);
      expect(terminateOwnedProcess(child, 50)).toBe(first);
      const outcome = await first;
      expect(outcome.kind).toBe("terminated");
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        try { child.kill(); } catch { /* cleanup remains bounded */ }
        await waitForChildClose(child, 1_000);
      }
    }
  });

  test("reports a failed Windows tree command without claiming cleanup", async () => {
    if (process.platform !== "win32") return;
    const command = longRunningCommand();
    const child = spawnOwnedProcess(command.executable, command.args, { env: env(), stdio: "ignore" });
    const previousSystemRoot = process.env.SystemRoot;
    process.env.SystemRoot = join(tmpdir(), "graphcoder-missing-system-root");
    try {
      await expect(terminateOwnedProcess(child, 50)).resolves.toMatchObject({ kind: "unknown", reason: expect.stringContaining("taskkill could not start") });
    } finally {
      if (previousSystemRoot === undefined) delete process.env.SystemRoot;
      else process.env.SystemRoot = previousSystemRoot;
      try { if (child.exitCode === null && child.signalCode === null) child.kill(); } catch { /* fixture cleanup is best effort after the typed outcome */ }
      await waitForChildClose(child, 1_000);
    }
  });

  test("does not cache invalid cleanup input and keeps terminal outcomes reusable", async () => {
    if (process.platform === "win32") return;
    const command = longRunningCommand();
    const child = spawnOwnedProcess(command.executable, command.args, { env: env(), stdio: "ignore" });
    try {
      await expect(terminateOwnedProcess(child, -1)).rejects.toThrow("nonnegative safe integer");
      const first = await terminateOwnedProcess(child, 50);
      expect(first.kind).toBe("terminated");
      const second = await retryOwnedProcessTermination(child, 50);
      expect(second.kind).toBe("terminated");
    } finally {
      try { if (child.exitCode === null && child.signalCode === null) child.kill(); } catch { /* cleanup remains bounded */ }
      await waitForChildClose(child, 1_000);
    }
  });

  test("native CLI awaits cleanup on a natural runtime exit", async () => {
    const cli = fileURLToPath(new URL("../src/native-cli.ts", import.meta.url));
    const child = spawnChild(process.execPath, [cli, "-e", "process.exit(0)", "--model-fixture=test"], {
      cwd: process.cwd(),
      env: { ...env(), GRAPHCODER_RUNTIME: process.execPath },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    let stderr = "";
    child.stderr?.setEncoding("utf8");
    child.stderr?.on("data", chunk => { stderr += String(chunk); });
    await waitForChildClose(child, 5_000);
    expect(child.exitCode).toBe(1);
    expect(stderr).toContain("runtime process cleanup unknown");
  });

  test("delegates runtime ownership to one injected native boundary", async () => {
    if (process.platform === "win32") return;
    let spawned = 0;
    let terminated = 0;
    const command = longRunningCommand();
    const processOwner = {
      spawn(executable: string, args: readonly string[], options: Parameters<typeof spawnOwnedProcess>[2]) {
        spawned += 1;
        return spawnOwnedProcess(executable, args, options);
      },
      terminate(child: ReturnType<typeof spawnOwnedProcess>, graceMs?: number) {
        terminated += 1;
        return terminateOwnedProcess(child, graceMs);
      },
    };
    const bridge = ownBridge({ executable: command.executable, args: command.args, env: env(), processOwner });
    bridge.close("native owner delegation");
    await expect(bridge.waitForExit(2_000)).resolves.toMatchObject({ kind: "closed" });
    expect(spawned).toBe(1);
    expect(terminated).toBe(1);
  });

  test("composes the process bridge with the public transport adapter", async () => {
    const script = `let buffer = ""; process.stdin.on("data", chunk => { buffer += chunk.toString(); for (;;) { const newline = buffer.indexOf("\\n"); if (newline < 0) break; const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1); if (!line.trim()) continue; const request = JSON.parse(line); process.stdout.write(JSON.stringify({ request_id: request.request_id, ok: true, result: { items: [] } }) + "\\n"); } });`;
    const connection = ownConnection({ executable: testRuntimeExecutable(), args: ["-e", script], env: env() });
    const page = await connection.transport.listSessions();
    expect(page.items).toEqual([]);
    connection.bridge.close();
    expect(await connection.bridge.waitForExit(2_000)).toMatchObject({ kind: "closed" });
  });

  test("runs headless terminal lifecycle through the owned runtime process", async () => {
    const lines: string[] = [];
    const output = { write(value: string, callback?: (error?: Error | null) => void): boolean { lines.push(value); callback?.(); return true; } } as unknown as NodeJS.WritableStream;
    const connection = ownConnection({ executable: testRuntimeExecutable(), args: ["-e", runtimeScript], env: env() });
    const terminal = new GraphCoderTerminal(connection.transport, { output });
    const status = await terminal.headless([
      "start op-runtime inspect",
      "open session-1",
      "resume session-1",
      "approvals",
      "approve approval-1 yes",
      "cancel",
    ]);
    expect(status).toBe(0);
    const responses = lines.map(line => JSON.parse(line) as { readonly ok: boolean; readonly value?: { readonly selectedSession?: { readonly state?: string }; readonly state?: string } });
    expect(responses).toHaveLength(6);
    expect(responses.every(response => response.ok)).toBe(true);
    expect(responses.at(-1)?.value?.state).toBe("cancelled");
    connection.bridge.close();
    expect(await connection.bridge.waitForExit(2_000)).toMatchObject({ kind: "closed" });
  });

  test("runs the interactive terminal entrypoint against the owned runtime process", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const lines: string[] = [];
    output.on("data", chunk => lines.push(String(chunk)));
    const connection = ownConnection({ executable: testRuntimeExecutable(), args: ["-e", runtimeScript], env: env() });
    const terminal = new GraphCoderTerminal(connection.transport, { input, output });
    const running = terminal.interactive();
    input.write("start op-interactive inspect\n");
    const firstDeadline = Date.now() + 10_000;
    while (lines.length < 1 && Date.now() < firstDeadline) await new Promise<void>(resolve => setTimeout(resolve, 10));
    expect(lines.length).toBeGreaterThanOrEqual(1);
    input.write("cancel\n");
    const secondDeadline = Date.now() + 10_000;
    while (lines.length < 2 && Date.now() < secondDeadline) await new Promise<void>(resolve => setTimeout(resolve, 10));
    expect(lines.length).toBeGreaterThanOrEqual(2);
    input.write("quit\n");
    input.end();
    await running;
    expect(lines.join("")).toContain('"exited":true');
    connection.bridge.close();
    expect(await connection.bridge.waitForExit(2_000)).toMatchObject({ kind: "closed" });
  });

  test("correlates concurrent responses and preserves explicit parameters", async () => {
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", childScript], env: env() });
    const [slow, fast] = await Promise.all([bridge.request(request("slow", 40)), bridge.request(request("fast"))]);
    expect(slow).toMatchObject({ request_id: "slow", ok: true, result: { query: { after: "40" } } });
    expect(fast).toMatchObject({ request_id: "fast", ok: true, result: { query: {} } });
    bridge.close();
  });

  test("cancels one pending request while keeping the process available", async () => {
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", childScript], env: env(), onDiagnostic: event => diagnostics.push(event), cancelMessage: requestId => ({ request_id: `${requestId}:cancel`, method: "cancel_session", params: {} }) });
    const pending = bridge.request(request("cancel", 100));
    expect(bridge.cancel("cancel", "user cancelled")).toBe(true);
    await expect(pending).rejects.toMatchObject({ code: "transport", message: "user cancelled" });
    const response = await bridge.request(request("after-cancel"));
    expect(response).toMatchObject({ request_id: "after-cancel", ok: true });
    const deadline = Date.now() + 2_000;
    while (!diagnostics.some(event => event.kind === "cancelled_response" && event.requestId === "cancel") && Date.now() < deadline) {
      await new Promise<void>(resolve => setTimeout(resolve, 20));
    }
    expect(diagnostics.some(event => event.kind === "cancelled_response" && event.requestId === "cancel")).toBe(true);
    expect(bridge.cancel("missing")).toBe(false);
    bridge.close();
  });

  test("retires a cancelled request id until its late response is consumed", async () => {
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", childScript], env: env() });
    const pending = bridge.request(request("reused", 50));
    expect(bridge.cancel("reused")).toBe(true);
    await expect(pending).rejects.toMatchObject({ code: "transport" });
    await expect(bridge.request(request("reused"))).rejects.toMatchObject({ code: "invalid_input" });
    await new Promise<void>(resolve => setTimeout(resolve, 80));
    bridge.close();
  });

  test("rejects a cancel control that collides with the retired request id", async () => {
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", childScript], env: env(), onDiagnostic: event => diagnostics.push(event), cancelMessage: requestId => request(requestId) });
    const pending = bridge.request(request("collision", 20));
    expect(bridge.cancel("collision")).toBe(true);
    await expect(pending).rejects.toMatchObject({ code: "transport" });
    expect(diagnostics).toContainEqual({ kind: "cancel_control_failed", requestId: "collision", message: "cancel control request id collides with an active or retired request" });
    bridge.close();
  });

  test("retains a cancellation control id after its response", async () => {
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", childScript], env: env(), cancelMessage: () => request("control") });
    const pending = bridge.request(request("original", 20));
    expect(bridge.cancel("original")).toBe(true);
    await expect(pending).rejects.toMatchObject({ code: "transport" });
    await new Promise<void>(resolve => setTimeout(resolve, 200));
    await expect(bridge.request(request("control"))).rejects.toMatchObject({ code: "invalid_input" });
    bridge.close();
  });

  test("rejects pending calls on clean EOF and reports malformed output", async () => {
    const eof = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", "process.exit(0)"], env: env() });
    await expect(eof.request(request("eof"))).rejects.toMatchObject({ code: "transport" });
    expect(await eof.waitForExit(2_000)).toMatchObject({ kind: "closed", code: 0 });
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const malformed = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", "console.log('malformed')"], env: env(), onDiagnostic: event => diagnostics.push(event), maximumLineBytes: 128 });
    await expect(malformed.request(request("malformed"))).rejects.toMatchObject({ code: "transport" });
    expect(diagnostics.some(event => event.kind === "malformed_line")).toBe(true);
    expect(await malformed.waitForExit(2_000)).toMatchObject({ kind: "closed" });
  });

  test("keeps exit cleanup authoritative when diagnostics throw", async () => {
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", "process.exit(0)"], env: env(), onDiagnostic: () => { throw new Error("diagnostic observer failed"); } });
    await expect(bridge.request(request("diagnostic-observer"))).rejects.toMatchObject({ code: "transport" });
    expect(await bridge.waitForExit(2_000)).toMatchObject({ kind: "closed", code: 0 });
  });

  test("sends operator approval only through the host control method", async () => {
    const script = `
const expected = process.env.GRAPHCODER_OPERATOR_TOKEN;
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk.toString();
  for (;;) {
    const newline = buffer.indexOf("\\n");
    if (newline < 0) break;
    const request = JSON.parse(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    const valid = request.method === "operator_approve" && request.params.operator_token === expected;
    process.stdout.write(JSON.stringify({ request_id: request.request_id, ok: valid, ...(valid ? { result: {} } : { error: { code: "denied", message: "operator credential missing" } }) }) + "\\n");
  }
});
`;
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", script], env: env() });
    await expect(bridge.operatorApprove({ approvalId: "approval-1", approved: true, sessionId: "session-1" })).resolves.toBeUndefined();
    bridge.close();
    expect(await bridge.waitForExit(2_000)).toMatchObject({ kind: "closed" });
  });

  test("rejects output with invalid UTF-8 before JSON decoding", async () => {
    const malformed = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", "process.stdout.write(Buffer.from([0xc3, 0x28, 0x0a]))"], env: env() });
    await expect(malformed.request(request("invalid-utf8"))).rejects.toMatchObject({ code: "transport" });
  });

  test("bounds a response line before parsing it", async () => {
    const oversized = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", "process.stdout.write('x'.repeat(512) + '\\n')"], env: env(), maximumLineBytes: 256 });
    await expect(oversized.request(request("oversized"))).rejects.toMatchObject({ code: "transport" });
  });

  test("bounds pending requests and applies the 256 UTF-8 byte request-id limit", async () => {
    const bridge = ownBridge({ executable: testRuntimeExecutable(), args: ["-e", childScript], env: env(), maximumPendingRequests: 1 });
    const first = bridge.request(request("first", 30));
    await expect(bridge.request(request("second"))).rejects.toMatchObject({ code: "transport" });
    await expect(first).resolves.toMatchObject({ request_id: "first", ok: true });
    await expect(bridge.request(request("é".repeat(129)))).rejects.toMatchObject({ code: "invalid_input" });
    await expect(bridge.request(request("é".repeat(128)))).resolves.toMatchObject({ ok: true });
    bridge.close();
  });

  test("rejects every pending call when an otherwise valid response has no matching request", async () => {
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const bridge = ownBridge({
      executable: testRuntimeExecutable(),
      args: ["-e", "process.stdout.write(JSON.stringify({ request_id: 'wrong', ok: true, result: {} }) + '\\n')"],
      env: env(),
      onDiagnostic: event => diagnostics.push(event),
    });
    await expect(bridge.request(request("expected"))).rejects.toMatchObject({ code: "transport" });
    expect(diagnostics).toContainEqual({ kind: "unmatched_response", requestId: "wrong" });
  });
});
