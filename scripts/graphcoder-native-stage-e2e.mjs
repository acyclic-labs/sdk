#!/usr/bin/env node

// Exercise the installed native GraphCoder runtime over its public JSON-lines
// boundary. The runtime and durable root are explicit inputs; this helper
// never builds the binary, invokes a shell, or inherits provider credentials.

import { existsSync, lstatSync, mkdtempSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn, spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

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

function runDroppedStart(runtime, root) {
  const result = spawnSync(runtime, ["--root", root, "--model-fixture", "stage"], {
    cwd: resolve("."),
    env: childEnvironment(),
    input: request("stage-1", "start_session", { prompt: "write fixture", operation_id: "op-stage-1", model_fixture: "stage" }),
    encoding: "utf8",
    shell: false,
    windowsHide: true,
    maxBuffer: 16 * 1024 * 1024,
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
  const next = async label => {
    const result = await new Promise((resolvePromise, reject) => {
      const onLine = line => { lines.off("line", onLine); resolvePromise(parseLine(line, label)); };
      lines.on("line", onLine);
      child.once("error", reject);
    });
    return result;
  };
  try {
    child.stdin.write(request("stage-retry", "start_session", { prompt: "write fixture", operation_id: "op-stage-1", model_fixture: "stage" }));
    const retried = await next("retry");
    if (retried.ok !== true) fail(`retry failed: ${JSON.stringify(retried)}`);
    const sessionId = retried.result?.summary?.id;
    const generation = retried.result?.workspace_generation;
    if (typeof sessionId !== "string" || typeof generation !== "string") fail("retry omitted session identity or generation");

    child.stdin.write(request("stage-file", "read_file", { session_id: sessionId, path: "graphcoder-fixture.txt", generation }));
    const file = await next("read_file");
    if (file.ok !== true || file.result?.path !== "graphcoder-fixture.txt" || file.result?.media_type !== "text/plain") fail(`staged file response was invalid: ${JSON.stringify(file)}`);
    const bytes = file.result?.bytes;
    if (!Array.isArray(bytes) || Buffer.from(bytes).toString("utf8") !== "fixture:stage") fail(`staged file bytes were invalid: ${JSON.stringify(file)}`);

    child.stdin.write(request("stage-reopen", "open_session", { session_id: sessionId }));
    const reopened = await next("reopen");
    if (reopened.ok !== true || reopened.result?.summary?.state !== "completed") fail(`reopened session was invalid: ${JSON.stringify(reopened)}`);

    child.stdin.write(request("stage-activity", "read_activity", { session_id: sessionId }));
    const activity = await next("activity");
    if (activity.ok !== true || !Array.isArray(activity.result?.items) || activity.result.items.length === 0) fail(`activity response was invalid: ${JSON.stringify(activity)}`);

    return { retried, file, reopened, activity };
  } finally {
    child.stdin.end();
    await new Promise(resolvePromise => child.once("close", resolvePromise));
    lines.close();
    if (child.exitCode !== 0) fail(`reopened native process exited ${child.exitCode}: ${stderr.join("")}`);
  }
}

export async function runNativeStageScenario({ runtime = required("GRAPHCODER_NATIVE_RUNTIME"), root = process.env.GRAPHCODER_NATIVE_ROOT } = {}) {
  regularFile(runtime);
  const ownedRoot = root === undefined ? mkdtempSync(join(tmpdir(), "graphcoder-native-stage-")) : resolve(root);
  try {
    runDroppedStart(runtime, ownedRoot);
    return await runReopen(runtime, ownedRoot);
  } finally {
    if (root === undefined) rmSync(ownedRoot, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runNativeStageScenario().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["dropped-response", "retry", "read_file", "reopen", "activity"], result }, null, 2)}\n`);
  }).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
