#!/usr/bin/env node

// Qualify the installed native approval fixture through the public bridge.
// The Harness policy creates the ticket and acyclic.stage_file performs the
// real admitted write; this driver only routes public requests and observes
// durable state.

import { lstatSync, mkdtempSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const RESPONSE_TIMEOUT_MS = 10_000;
const CLOSE_TIMEOUT_MS = 5_000;

function fail(message) { throw new Error(`graphcoder-native-approval-e2e: ${message}`); }
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
function wire(requestId, method, params) {
  return `${JSON.stringify({ request_id: requestId, method, params })}\n`;
}
function regularFile(path) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata?.isFile() || metadata.isSymbolicLink()) fail(`runtime must be a regular file: ${path}`);
}

async function runScenario(runtime, root, checkoutOverride) {
  const checkout = checkoutOverride ?? process.env.GRAPHCODER_NATIVE_CHECKOUT;
  const projectId = process.env.GRAPHCODER_NATIVE_PROJECT_ID
    ?? (process.env.GRAPHCODER_NATIVE_CHECKOUT === "auto"
      ? "native-approval-checkout"
      : undefined);
  if ((checkout && !projectId) || (!checkout && projectId)) fail("GRAPHCODER_NATIVE_CHECKOUT and GRAPHCODER_NATIVE_PROJECT_ID must be supplied together");
  const args = ["--root", root, "--model-fixture", "approval", "--operator-token", "operator-secret"];
  if (checkout) args.push("--checkout", resolve(checkout), "--project-id", projectId);
  const child = spawn(runtime, args, {
    cwd: resolve("."), env: environment(), stdio: ["pipe", "pipe", "pipe"], shell: false, windowsHide: true,
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
  const request = async (id, method, params) => {
    child.stdin.write(wire(id, method, params));
    return nextFor(id, method);
  };
  const waitForClose = async () => {
    let timer;
    const result = await Promise.race([closed, new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); })]);
    clearTimeout(timer);
    if (!result.timeout) return result;
    child.kill();
    let killTimer;
    const killed = await Promise.race([closed, new Promise(resolvePromise => { killTimer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); })]);
    clearTimeout(killTimer);
    if (killed.timeout) fail(`native process did not close after termination`);
    return killed;
  };
  const buffered = new Map();
  const nextFor = async (id, label) => {
    const existing = buffered.get(id);
    if (existing) { buffered.delete(id); return existing; }
    for (;;) {
      const response = await nextAny(label);
      if (response.request_id === id) return response;
      buffered.set(response.request_id, response);
    }
  };

  try {
    const listed = await request("approval-list", "list_sessions", {});
    if (!listed.ok || listed.result?.items?.length !== 1) fail(`invalid session listing: ${JSON.stringify(listed)}`);
    const sessionId = listed.result.items[0].id;
    const pending = await request("approval-start", "start_session", {
      prompt: "write through approval", operation_id: "native-approval-operation", model_fixture: "approval",
    });
    if (pending.ok || !String(pending.error?.message).includes("indeterminate")) fail(`approval did not pause before effect: ${JSON.stringify(pending)}`);
    const approvals = await request("approval-list-ticket", "list_approvals", { session_id: sessionId });
    const ticket = approvals.result?.items?.[0];
    if (!approvals.ok || ticket?.state !== "pending" || typeof ticket.id !== "string") fail(`approval ticket was not pending: ${JSON.stringify(approvals)}`);
    const forged = await request("approval-forged", "operator_approve", {
      operator_token: "operator-secret", session_id: sessionId,
      approval_id: "00000000-0000-0000-0000-000000000001", approved: true,
    });
    if (forged.ok || forged.error?.code !== "not_found") fail(`forged approval identity was accepted: ${JSON.stringify(forged)}`);
    const wrongOperator = await request("approval-wrong-operator", "operator_approve", {
      operator_token: "wrong", session_id: sessionId, approval_id: ticket.id, approved: true,
    });
    if (wrongOperator.ok || wrongOperator.error?.code !== "denied") fail(`wrong operator was accepted: ${JSON.stringify(wrongOperator)}`);
    const approved = await request("approval-approved", "operator_approve", {
      operator_token: "operator-secret", session_id: sessionId, approval_id: ticket.id, approved: true,
    });
    if (!approved.ok || approved.result?.approved !== true) fail(`exact approval did not commit: ${JSON.stringify(approved)}`);

    child.stdin.write(wire("approval-resume", "resume_session", {
      session_id: sessionId, prompt: "write through approval", operation_id: "native-approval-operation", model_fixture: "approval",
    }));
    let observedEffect = false;
    let poll = 0;
    const deadline = Date.now() + RESPONSE_TIMEOUT_MS;
    while (!observedEffect && Date.now() < deadline) {
      const snapshot = await request(`approval-snapshot-${poll}`, "open_session", { session_id: sessionId });
      const file = snapshot.ok && snapshot.result?.workspace_generation
        ? await request(`approval-file-poll-${poll}`, "read_file", {
            session_id: sessionId, path: "graphcoder-fixture.txt", generation: snapshot.result.workspace_generation,
          })
        : null;
      observedEffect = file?.ok === true && String.fromCharCode(...file.result.bytes) === "fixture:stage";
      poll += 1;
    }
    if (!observedEffect) fail("did not observe durable stage_file completion before cancellation");
    child.stdin.write(wire("approval-cancel", "cancel_session", { session_id: sessionId }));
    const responses = new Map();
    while (!responses.has("approval-resume") || !responses.has("approval-cancel")) {
      const response = await nextAny("approval cancellation");
      if (!["approval-resume", "approval-cancel"].includes(response.request_id)) fail(`unexpected response: ${JSON.stringify(response)}`);
      responses.set(response.request_id, response);
    }
    if (responses.get("approval-cancel")?.result?.summary?.state !== "cancelled") fail(`cancellation was not durable: ${JSON.stringify(responses.get("approval-cancel"))}`);
    if (responses.get("approval-resume")?.ok !== false) fail(`cancelled resume falsely completed: ${JSON.stringify(responses.get("approval-resume"))}`);
    const reopened = await request("approval-reopen", "open_session", { session_id: sessionId });
    if (!reopened.ok || reopened.result?.summary?.state !== "cancelled") fail(`reopen lost cancellation: ${JSON.stringify(reopened)}`);
    const file = await request("approval-file", "read_file", {
      session_id: sessionId, path: "graphcoder-fixture.txt", generation: reopened.result.workspace_generation,
    });
    if (!file.ok || String.fromCharCode(...file.result.bytes) !== "fixture:stage") fail(`approved stage effect was not durable: ${JSON.stringify(file)}`);
    return { sessionId, approvalId: ticket.id, forged: forged.error.code, wrongOperator: wrongOperator.error.code, state: reopened.result.summary.state };
  } finally {
    let closeError;
    try { child.stdin.end(); } catch (error) { closeError = error; }
    const exit = await waitForClose();
    output.return?.();
    if (closeError) throw closeError;
    if (exit.code !== 0) fail(`native process exited ${exit.code}: ${stderr.join("")}`);
  }
}

export async function runNativeApprovalScenario({ runtime = required("GRAPHCODER_NATIVE_RUNTIME"), root } = {}) {
  regularFile(runtime);
  const ownedRoot = root === undefined ? mkdtempSync(join(tmpdir(), "graphcoder-native-approval-")) : resolve(root);
  const ownedCheckout = process.env.GRAPHCODER_NATIVE_CHECKOUT === "auto"
    ? mkdtempSync(join(tmpdir(), "graphcoder-native-approval-checkout-"))
    : undefined;
  try { return await runScenario(runtime, ownedRoot, ownedCheckout); }
  finally {
    if (root === undefined) rmSync(ownedRoot, { recursive: true, force: true });
    if (ownedCheckout !== undefined) rmSync(ownedCheckout, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runNativeApprovalScenario().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["pending-approval", "forged-identity", "denied-operator", "approved-stage-effect", "cancelled", "reopened"], result }, null, 2)}\n`);
  }).catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
