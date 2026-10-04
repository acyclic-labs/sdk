#!/usr/bin/env node

// Qualifies cancellation of an admitted native process through the public
// bridge. The process is real and runs in the background; cancellation and
// receipt persistence remain Harness responsibilities.

import { existsSync, lstatSync, mkdtempSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const RESPONSE_TIMEOUT_MS = 10_000;
const CLOSE_TIMEOUT_MS = 5_000;
const CANCEL_TIMEOUT_MS = 10_000;

function fail(message) { throw new Error(`graphcoder-native-cancellation-e2e: ${message}`); }
function required(name) {
  const value = process.env[name];
  if (typeof value !== "string" || value.trim() === "") fail(`${name} is required`);
  return resolve(value);
}
function environment() {
  return Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    ["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT"].includes(key),
  ));
}
function regularFile(path) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata?.isFile() || metadata.isSymbolicLink()) fail(`runtime must be a regular file: ${path}`);
}
function wire(requestId, method, params) {
  return `${JSON.stringify({ request_id: requestId, method, params })}\n`;
}

async function openRuntime(runtime, root, checkout) {
  const child = spawn(runtime, [
    "--root", root,
    "--model-fixture", "native-approval-blocking",
    "--operator-token", "operator-secret",
    "--checkout", checkout,
    "--project-id", "native-process-checkout",
  ], {
    cwd: resolve("."), env: environment(), stdio: ["pipe", "pipe", "pipe"],
    shell: false, windowsHide: true,
  });
  if (!child.stdin || !child.stdout || !child.stderr) fail("native process did not expose piped stdio");
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  const output = createInterface({ input: child.stdout, crlfDelay: Infinity })[Symbol.asyncIterator]();
  const stderr = [];
  child.stderr.on("data", value => stderr.push(String(value)));
  let closeResolve;
  const closed = new Promise(resolvePromise => { closeResolve = resolvePromise; });
  child.once("close", (code, signal) => closeResolve({ code, signal }));
  let errorReject;
  const processError = new Promise((_resolve, reject) => { errorReject = reject; });
  child.once("error", error => errorReject(error));
  const buffered = new Map();
  const nextAny = async label => {
    let timer;
    try {
      const result = await Promise.race([
        output.next(), closed.then(value => ({ closed: value })), processError.then(error => ({ error })),
        new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), RESPONSE_TIMEOUT_MS); }),
      ]);
      if (result?.timeout) fail(`${label} timed out after ${RESPONSE_TIMEOUT_MS}ms`);
      if (result?.error) fail(`${label} native process failed: ${result.error.message}`);
      if (result?.closed) fail(`${label} native process closed before its response`);
      if (result.done) fail(`${label} native process ended before its response`);
      try { return JSON.parse(result.value); }
      catch (error) { fail(`${label} returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
    } finally { clearTimeout(timer); }
  };
  const nextFor = async (id, label) => {
    const existing = buffered.get(id);
    if (existing) { buffered.delete(id); return existing; }
    for (;;) {
      const response = await nextAny(label);
      if (response.request_id === id) return response;
      buffered.set(response.request_id, response);
    }
  };
  const send = (id, method, params) => {
    child.stdin.write(wire(id, method, params));
  };
  const request = async (id, method, params) => { send(id, method, params); return nextFor(id, method); };
  const close = async () => {
    try { child.stdin.end(); } catch {}
    let timer;
    const result = await Promise.race([closed, new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); })]);
    clearTimeout(timer);
    if (!result.timeout) return result;
    child.kill();
    return Promise.race([closed, new Promise(resolvePromise => setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS))]);
  };
  return { child, request, send, waitFor: nextFor, close, output, stderr };
}

async function runCancellation(runtime, root, checkout) {
  const host = await openRuntime(runtime, root, checkout);
  try {
    const listed = await host.request("sessions", "list_sessions", {});
    if (!listed.ok || listed.result?.items?.length !== 1) fail(`invalid session listing: ${JSON.stringify(listed)}`);
    const sessionId = listed.result.items[0].id;
    const approvals = await host.request("approvals", "list_approvals", { session_id: sessionId });
    const ticket = approvals.result?.items?.[0];
    if (!approvals.ok || ticket?.state !== "pending" || typeof ticket.id !== "string") fail(`blocking approval ticket was not pending: ${JSON.stringify(approvals)}`);
    const approved = await host.request("approve", "operator_approve", {
      operator_token: "operator-secret", session_id: sessionId, approval_id: ticket.id, approved: true,
    });
    if (!approved.ok || approved.result?.approved !== true) fail(`blocking approval did not commit: ${JSON.stringify(approved)}`);
    const wrongOperator = await host.request("wrong-operator", "native_process_cancel", {
      operator_token: "wrong", session_id: sessionId, approval_id: ticket.id,
    });
    if (wrongOperator.ok || wrongOperator.error?.code !== "denied") fail(`wrong operator cancelled native process: ${JSON.stringify(wrongOperator)}`);
    const forged = await host.request("forged-cancel", "native_process_cancel", {
      operator_token: "operator-secret", session_id: sessionId,
      approval_id: "00000000-0000-0000-0000-000000000001",
    });
    if (forged.ok || forged.error?.code !== "not_found") fail(`forged native cancellation identity was accepted: ${JSON.stringify(forged)}`);
    host.send("execute", "native_process", { session_id: sessionId, approval_id: ticket.id });
    let cancellation;
    const deadline = Date.now() + CANCEL_TIMEOUT_MS;
    let attempt = 0;
    while (Date.now() < deadline) {
      const result = await host.request(`cancel-${attempt}`, "native_process_cancel", {
        operator_token: "operator-secret", session_id: sessionId, approval_id: ticket.id,
      });
      if (!result.ok) fail(`native cancellation failed: ${JSON.stringify(result)}`);
      if (result.result?.cancel_requested === true) { cancellation = result; break; }
      await new Promise(resolvePromise => setTimeout(resolvePromise, 50));
      attempt += 1;
    }
    if (!cancellation) fail("native cancellation never acquired the live attempt");
    const completed = await host.waitFor("execute", "native execution");
    if (!completed.ok || completed.result?.status !== "cancelled") fail(`native execution was not durably cancelled: ${JSON.stringify(completed)}`);
    if (existsSync(join(checkout, "graphcoder-native-blocking-effect.txt"))) fail("cancelled process published its completion marker");
    const exit = await host.close();
    if (exit.timeout || exit.code !== 0) fail(`first runtime did not close cleanly: ${JSON.stringify(exit)} ${host.stderr.join("")}`);
    return { sessionId, approvalId: ticket.id, status: completed.result.status };
  } finally {
    if (host.child.exitCode === null) await host.close();
    host.output.return?.();
  }
}

async function runColdReplay(runtime, root, checkout, approvalId) {
  const host = await openRuntime(runtime, root, checkout);
  try {
    const listed = await host.request("sessions", "list_sessions", {});
    if (!listed.ok || listed.result?.items?.length !== 1) fail(`cold restart lost session: ${JSON.stringify(listed)}`);
    const sessionId = listed.result.items[0].id;
    const approvals = await host.request("approvals", "list_approvals", { session_id: sessionId });
    const ticket = approvals.result?.items?.find(item => item.id === approvalId);
    if (!ticket || ticket.state !== "approved") fail(`cold restart lost approval state: ${JSON.stringify(approvals)}`);
    const replay = await host.request("replay", "native_process", { session_id: sessionId, approval_id: approvalId });
    if (!replay.ok || replay.result?.status !== "cancelled") fail(`cold replay did not retain cancellation fence: ${JSON.stringify(replay)}`);
    const exit = await host.close();
    if (exit.timeout || exit.code !== 0) fail(`cold runtime did not close cleanly: ${JSON.stringify(exit)} ${host.stderr.join("")}`);
    return { sessionId, approvalId, replay: replay.result.status };
  } finally {
    if (host.child.exitCode === null) await host.close();
    host.output.return?.();
  }
}

export async function runNativeCancellationScenario({ runtime = required("GRAPHCODER_NATIVE_RUNTIME") } = {}) {
  regularFile(runtime);
  const root = mkdtempSync(join(tmpdir(), "graphcoder-native-cancel-"));
  const checkout = mkdtempSync(join(tmpdir(), "graphcoder-native-cancel-checkout-"));
  try {
    const cancelled = await runCancellation(runtime, root, checkout);
    const replay = await runColdReplay(runtime, root, checkout, cancelled.approvalId);
    return { cancelled, replay };
  } finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(checkout, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runNativeCancellationScenario().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["in-flight-cancel", "durable-cancelled-receipt", "cold-restart-replay", "process-cleanup"], result }, null, 2)}\n`);
  }).catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
