#!/usr/bin/env node

// Exercise the installed native GraphCoder blocking fixture through the public
// JSON-lines bridge. The fixture itself owns the waiting model state; this
// driver only sends public requests and checks their durable responses.

import { lstatSync, mkdtempSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const RESPONSE_TIMEOUT_MS = 10_000;
const CLOSE_TIMEOUT_MS = 5_000;

function fail(message) {
  throw new Error(`graphcoder-native-blocking-e2e: ${message}`);
}

function required(name) {
  const value = process.env[name];
  if (typeof value !== "string" || value.trim() === "") fail(`${name} is required`);
  return resolve(value);
}

function childEnvironment() {
  return Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    ["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT"].includes(key),
  ));
}

function request(requestId, method, params) {
  return `${JSON.stringify({ request_id: requestId, method, params })}\n`;
}

function parseLine(line, label, expectedRequestId) {
  let value;
  try { value = JSON.parse(line); }
  catch (error) { fail(`${label} returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (!value || typeof value.request_id !== "string" || typeof value.ok !== "boolean") fail(`${label} returned an invalid wire response`);
  if (value.request_id !== expectedRequestId) fail(`${label} correlated response ${JSON.stringify(value.request_id)} to ${JSON.stringify(expectedRequestId)}`);
  return value;
}

function regularFile(path) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata?.isFile() || metadata.isSymbolicLink()) fail(`runtime must be a regular file: ${path}`);
}

export async function runNativeBlockingScenario({ runtime = required("GRAPHCODER_NATIVE_RUNTIME"), root } = {}) {
  regularFile(runtime);
  const ownedRoot = root === undefined ? mkdtempSync(join(tmpdir(), "graphcoder-native-blocking-")) : resolve(root);
  const child = spawn(runtime, ["--root", ownedRoot, "--model-fixture", "blocking", "--operator-token", "operator-secret"], {
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
  const output = lines[Symbol.asyncIterator]();
  const stderr = [];
  child.stderr.on("data", value => stderr.push(String(value)));
  let closeResolve;
  const closed = new Promise(resolvePromise => { closeResolve = resolvePromise; });
  child.once("close", (code, signal) => closeResolve({ code, signal }));
  let errorReject;
  const processError = new Promise((_resolve, reject) => { errorReject = reject; });
  child.once("error", error => errorReject(error));
  const next = async (label, requestId) => {
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
      return parseLine(result.value, label, requestId);
    } finally {
      clearTimeout(timer);
    }
  };
  const nextAny = async label => {
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
      let value;
      try { value = JSON.parse(result.value); }
      catch (error) { fail(`${label} returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
      if (!value || typeof value.request_id !== "string" || typeof value.ok !== "boolean") fail(`${label} returned an invalid wire response`);
      return value;
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
    if (!result.timeout) return result;
    child.kill();
    let killTimer;
    const killed = await Promise.race([
      closed,
      new Promise(resolvePromise => { killTimer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); }),
    ]);
    clearTimeout(killTimer);
    if (killed.timeout) fail(`native process did not close within ${CLOSE_TIMEOUT_MS}ms after termination`);
    return killed;
  };

  try {
    child.stdin.write(request("blocking-list", "list_sessions", {}));
    const listed = await next("list_sessions", "blocking-list");
    if (listed.ok !== true || !Array.isArray(listed.result?.items) || listed.result.items.length !== 1) fail(`session listing was invalid: ${JSON.stringify(listed)}`);
    const sessionId = listed.result.items[0]?.id;
    if (typeof sessionId !== "string") fail("session listing omitted the root session id");

    child.stdin.write(request("blocking-approvals", "list_approvals", { session_id: sessionId }));
    const approvals = await next("list_approvals", "blocking-approvals");
    const approval = approvals.result?.items?.[0];
    if (approvals.ok !== true || approval?.state !== "pending" || typeof approval.id !== "string") fail(`blocking approval was not pending: ${JSON.stringify(approvals)}`);

    child.stdin.write(request("blocking-denied", "operator_approve", { operator_token: "wrong", session_id: sessionId, approval_id: approval.id, approved: true }));
    const denied = await next("wrong operator", "blocking-denied");
    if (denied.ok !== false || denied.error?.code !== "denied") fail(`wrong operator was not denied: ${JSON.stringify(denied)}`);

    child.stdin.write(request("blocking-still-pending", "list_approvals", { session_id: sessionId }));
    const stillPending = await next("approval remains pending", "blocking-still-pending");
    if (stillPending.ok !== true || stillPending.result?.items?.[0]?.state !== "pending") fail(`wrong operator changed approval state: ${JSON.stringify(stillPending)}`);

    child.stdin.write(request("blocking-approved", "operator_approve", { operator_token: "operator-secret", session_id: sessionId, approval_id: approval.id, approved: true }));
    const approved = await next("operator approval", "blocking-approved");
    if (approved.ok !== true || approved.result?.approved !== true) fail(`operator approval did not commit: ${JSON.stringify(approved)}`);

    child.stdin.write(request("blocking-start", "start_session", { prompt: "wait for cancellation", operation_id: "blocking-operation", model_fixture: "blocking" }));
    child.stdin.write(request("blocking-cancel", "cancel_session", { session_id: sessionId }));
    const responses = new Map();
    while (!responses.has("blocking-start") || !responses.has("blocking-cancel")) {
      const response = await nextAny("blocking cancellation");
      if (response.request_id !== "blocking-start" && response.request_id !== "blocking-cancel") fail(`unexpected response during cancellation: ${JSON.stringify(response)}`);
      responses.set(response.request_id, response);
    }
    const cancelled = responses.get("blocking-cancel");
    if (cancelled.ok !== true || cancelled.result?.summary?.state !== "cancelled") fail(`cancel_session did not persist cancellation: ${JSON.stringify(cancelled)}`);
    const start = responses.get("blocking-start");
    if (start.ok !== false || start.error?.code !== "stale") fail(`cancelled start was not rejected as stale: ${JSON.stringify(start)}`);
    return { sessionId, approvalId: approval.id, denied: denied.error.code, cancelled: cancelled.result.summary.state, startError: start.error.code };
  } finally {
    let exit;
    let closeError;
    try { child.stdin.end(); }
    catch (error) { closeError = error; }
    try { exit = await waitForClose(); }
    catch (error) { closeError ??= error; }
    finally {
      lines.close();
      if (root === undefined) rmSync(ownedRoot, { recursive: true, force: true });
    }
    if (closeError) throw closeError;
    if (exit?.code !== 0) fail(`native process exited ${exit.code}: ${stderr.join("")}`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runNativeBlockingScenario().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["list-sessions", "pending-approval", "denied-operator", "approved-operator", "cancelled", "no-false-completion"], result }, null, 2)}\n`);
  }).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
