#!/usr/bin/env node

// Exercise the installed native GraphCoder runtime over its public JSON-lines
// boundary. The runtime and durable root are explicit inputs; this helper
// never builds the binary, invokes a shell, or inherits provider credentials.

import { existsSync, lstatSync, mkdtempSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn, spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const RESPONSE_TIMEOUT_MS = 15_000;
const CLOSE_TIMEOUT_MS = 5_000;

function fail(message) {
  throw new Error(`graphcoder-native-stage-e2e: ${message}`);
}

function required(name) {
  const value = process.env[name];
  if (typeof value !== "string" || value.trim() === "") fail(`${name} is required`);
  return resolve(value);
}

function regularFile(path) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata?.isFile() || metadata.isSymbolicLink()) fail(`runtime must be a regular file: ${path}`);
}

function childEnvironment() {
  return Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    ["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT"].includes(key),
  ));
}

function request(requestId, method, params) {
  return `${JSON.stringify({ request_id: requestId, method, params })}\n`;
}

function parseLine(line, label) {
  let value;
  try { value = JSON.parse(line); }
  catch (error) { fail(`${label} returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (!value || value.request_id === undefined || typeof value.ok !== "boolean") fail(`${label} returned an invalid wire response`);
  return value;
}

function assertNoAttachments(value, label) {
  if (value && typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "attachments")) fail(`${label} unexpectedly exposed automatic attachments`);
}

async function withDeadline(promise, label) {
  let timer;
  const result = await Promise.race([
    promise,
    new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), RESPONSE_TIMEOUT_MS); }),
  ]);
  clearTimeout(timer);
  if (result?.timeout === true) fail(`${label} timed out after ${RESPONSE_TIMEOUT_MS}ms`);
  return result;
}

function runDroppedStart(runtime, root) {
  const result = spawnSync(runtime, ["--root", root, "--model-fixture", "stage"], {
    cwd: resolve("."),
    env: childEnvironment(),
    input: request("stage-1", "start_session", { prompt: "write fixture", operation_id: "op-stage-1", model_fixture: "stage" }),
    encoding: "utf8",
    shell: false,
    windowsHide: true,
    maxBuffer: 16 * 1024 * 1024,
    timeout: RESPONSE_TIMEOUT_MS,
  });
  if (result.error) fail(`initial native process failed: ${result.error.message}`);
  if (result.status !== 0) fail(`initial native process exited ${result.status}: ${result.stderr}`);
  const line = result.stdout.trim().split(/\r?\n/u).filter(Boolean).at(-1);
  if (line === undefined) fail("initial native process returned no response");
  const response = parseLine(line, "initial native process");
  if (response.ok !== true) fail(`initial stage request failed: ${JSON.stringify(response)}`);
}

async function runReopen(runtime, root) {
  const child = spawn(runtime, ["--root", root, "--model-fixture", "stage"], {
    cwd: resolve("."),
    env: childEnvironment(),
    stdio: ["pipe", "pipe", "pipe"],
    shell: false,
    windowsHide: true,
  });
  if (child.stdin === null || child.stdout === null || child.stderr === null) fail("native process did not expose piped stdio");
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  const stderr = [];
  child.stderr.on("data", value => stderr.push(String(value)));
  const output = lines[Symbol.asyncIterator]();
  let closeResolve;
  const closed = new Promise(resolvePromise => { closeResolve = resolvePromise; });
  child.once("close", (code, signal) => closeResolve({ code, signal }));
  let errorReject;
  const processError = new Promise((_resolve, reject) => { errorReject = reject; });
  child.once("error", error => errorReject(error));
  const next = async label => {
    let timer;
    try {
      const result = await Promise.race([
        output.next(),
        closed.then(value => ({ closed: value })),
        processError.then(error => ({ error })),
        new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), RESPONSE_TIMEOUT_MS); }),
      ]);
      if (result?.timeout) fail(`${label} response timed out after ${RESPONSE_TIMEOUT_MS}ms`);
      if (result?.error) fail(`${label} native process failed: ${result.error.message}`);
      if (result?.closed) fail(`${label} native process closed before its response (code ${result.closed.code}, signal ${result.closed.signal ?? "none"})`);
      if (result.done) fail(`${label} native process ended before its response`);
      return parseLine(result.value, label);
    } finally {
      clearTimeout(timer);
    }
  };
  const waitForClose = async () => {
    let timer;
    const result = await Promise.race([
      closed,
      new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); }),
    ]);
    clearTimeout(timer);
    if (result.timeout) {
      child.kill();
      let killTimer;
      const killed = await Promise.race([
        closed,
        new Promise(resolvePromise => { killTimer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); }),
      ]);
      clearTimeout(killTimer);
      if (killed.timeout) fail(`native process did not close within ${CLOSE_TIMEOUT_MS}ms after termination`);
      return killed;
    }
    return result;
  };
  try {
    child.stdin.write(request("stage-retry", "start_session", { prompt: "write fixture", operation_id: "op-stage-1", model_fixture: "stage" }));
    const retried = await next("retry");
    if (retried.ok !== true) fail(`retry failed: ${JSON.stringify(retried)}`);
    const sessionId = retried.result?.summary?.id;
    const generation = retried.result?.workspace_generation;
    if (typeof sessionId !== "string" || typeof generation !== "string") fail("retry omitted session identity or generation");
    if (!/^[1-9][0-9]*$/u.test(generation)) fail(`retry returned a nonzero decimal generation: ${generation}`);
    assertNoAttachments(retried.result, "retry snapshot");

    child.stdin.write(request("stage-file", "read_file", { session_id: sessionId, path: "graphcoder-fixture.txt", generation }));
    const file = await next("read_file");
    if (file.ok !== true) fail(`native read_file route failed instead of returning a file: ${JSON.stringify(file)}`);
    if (file.result?.path !== "graphcoder-fixture.txt" || file.result?.media_type !== "text/plain" || file.result?.generation !== generation) fail(`staged file response was invalid: ${JSON.stringify(file)}`);
    const bytes = file.result?.bytes;
    if (!Array.isArray(bytes) || Buffer.from(bytes).toString("utf8") !== "fixture:stage") fail(`staged file bytes were invalid: ${JSON.stringify(file)}`);
    assertNoAttachments(file.result, "file response");

    child.stdin.write(request("stage-reopen", "open_session", { session_id: sessionId }));
    const reopened = await next("reopen");
    if (reopened.ok !== true || reopened.result?.summary?.state !== "completed" || reopened.result?.workspace_generation !== generation) fail(`reopened session was invalid: ${JSON.stringify(reopened)}`);
    assertNoAttachments(reopened.result, "reopened snapshot");

    child.stdin.write(request("stage-activity", "read_activity", { session_id: sessionId }));
    const activity = await next("activity");
    if (activity.ok !== true || !Array.isArray(activity.result?.items) || activity.result.items.length === 0) fail(`activity response was invalid: ${JSON.stringify(activity)}`);

    return { sessionId, generation, retried, file, reopened, activity };
  } finally {
    child.stdin.end();
    const exit = await waitForClose();
    lines.close();
    if (exit.code !== 0) fail(`reopened native process exited ${exit.code}: ${stderr.join("")}`);
  }
}

async function runInstalledConsumerRead({ packageRoot, runtime, root, sessionId, generation }) {
  const modulePath = join(packageRoot, "dist", "node.js");
  if (!existsSync(modulePath)) fail(`installed GraphCoder package is missing its Node connection: ${modulePath}`);
  const { createNodeGraphCoderConnection } = await import(`${pathToFileURL(modulePath).href}?qualification=${Date.now()}`);
  let exitResolve;
  const exited = new Promise(resolvePromise => { exitResolve = resolvePromise; });
  const connection = createNodeGraphCoderConnection({
    executable: runtime,
    args: ["--root", root, "--model-fixture", "stage"],
    cwd: resolve("."),
    env: childEnvironment(),
    onDiagnostic: event => { if (event.kind === "exit") exitResolve(event); },
  });
  try {
    const snapshot = await withDeadline(connection.transport.openSession(sessionId), "installed consumer open_session");
    if (snapshot.workspaceGeneration <= 0n || snapshot.workspaceGeneration !== BigInt(generation)) fail(`installed consumer decoded an unexpected workspace generation: ${snapshot.workspaceGeneration}`);
    assertNoAttachments(snapshot, "installed consumer snapshot");
    const file = await withDeadline(connection.transport.readFile(sessionId, "graphcoder-fixture.txt", snapshot.workspaceGeneration), "installed consumer read_file");
    if (typeof file.generation !== "bigint" || file.generation !== snapshot.workspaceGeneration) fail(`installed consumer did not preserve BigInt file generation: ${String(file.generation)}`);
    if (file.mediaType !== "text/plain" || Buffer.from(file.bytes).toString("utf8") !== "fixture:stage") fail(`installed consumer decoded unexpected file body: ${JSON.stringify({ path: file.path, mediaType: file.mediaType, bytes: [...file.bytes] })}`);
    assertNoAttachments(file, "installed consumer file");
    return { generation: file.generation.toString(), mediaType: file.mediaType, bytes: [...file.bytes] };
  } finally {
    connection.bridge.close("native stage qualification finished");
    let timer;
    const result = await Promise.race([
      exited,
      new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); }),
    ]);
    clearTimeout(timer);
    if (result.timeout) fail(`installed consumer bridge did not close within ${CLOSE_TIMEOUT_MS}ms`);
  }
}

export async function runNativeStageScenario({ runtime = required("GRAPHCODER_NATIVE_RUNTIME"), root = process.env.GRAPHCODER_NATIVE_ROOT } = {}) {
  regularFile(runtime);
  const packageRoot = required("GRAPHCODER_PACKAGE_ROOT");
  const ownedRoot = root === undefined ? mkdtempSync(join(tmpdir(), "graphcoder-native-stage-")) : resolve(root);
  try {
    runDroppedStart(runtime, ownedRoot);
    const result = await runReopen(runtime, ownedRoot);
    const installed = await runInstalledConsumerRead({ packageRoot, runtime, root: ownedRoot, sessionId: result.sessionId, generation: result.generation });
    return { ...result, installed };
  } finally {
    if (root === undefined) rmSync(ownedRoot, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runNativeStageScenario().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["dropped-response", "retry", "read_file", "reopen", "activity", "installed-consumer-read"], result }, null, 2)}\n`);
  }).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
