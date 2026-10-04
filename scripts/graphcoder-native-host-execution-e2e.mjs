#!/usr/bin/env node

// Qualifies the installed Harness native host execution adapter through the
// public JSON-lines bridge. The fixture uses a real admitted process and an
// attached temporary checkout; no shell or visible terminal is started.

import { existsSync, lstatSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const RESPONSE_TIMEOUT_MS = 10_000;
const CLOSE_TIMEOUT_MS = 5_000;

function fail(message) { throw new Error(`graphcoder-native-host-execution-e2e: ${message}`); }
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

async function runScenario(runtime, root, checkout) {
  const args = [
    "--root", root,
    "--model-fixture", "native-approval",
    "--operator-token", "operator-secret",
    "--checkout", checkout,
    "--project-id", "native-process-checkout",
  ];
  const child = spawn(runtime, args, {
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
  const request = async (id, method, params) => {
    child.stdin.write(`${JSON.stringify({ request_id: id, method, params })}\n`);
    return nextFor(id, method);
  };
  const close = async () => {
    try { child.stdin.end(); } catch {}
    let timer;
    const result = await Promise.race([closed, new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS); })]);
    clearTimeout(timer);
    if (!result.timeout) return result;
    child.kill();
    return Promise.race([closed, new Promise(resolvePromise => setTimeout(() => resolvePromise({ timeout: true }), CLOSE_TIMEOUT_MS))]);
  };

  try {
    const listed = await request("sessions", "list_sessions", {});
    if (!listed.ok || listed.result?.items?.length !== 1) fail(`invalid session listing: ${JSON.stringify(listed)}`);
    const sessionId = listed.result.items[0].id;
    const approvals = await request("approvals", "list_approvals", { session_id: sessionId });
    const ticket = approvals.result?.items?.[0];
    if (!approvals.ok || ticket?.state !== "pending" || typeof ticket.id !== "string") fail(`native approval ticket was not pending: ${JSON.stringify(approvals)}`);
    const wrongInspection = await request("inspect-wrong", "operator_inspect_approval", {
      operator_token: "wrong", session_id: sessionId, approval_id: ticket.id,
    });
    if (wrongInspection.ok || wrongInspection.error?.code !== "denied") fail(`wrong operator inspected action: ${JSON.stringify(wrongInspection)}`);
    const inspection = await request("inspect", "operator_inspect_approval", {
      operator_token: "operator-secret", session_id: sessionId, approval_id: ticket.id,
    });
    if (!inspection.ok) fail(`operator inspection failed: ${JSON.stringify(inspection)}`);
    const action = inspection.result;
    if (!String(action.executable).toLowerCase().endsWith("\\cmd.exe")) fail(`unexpected executable: ${JSON.stringify(action)}`);
    if (JSON.stringify(action.arguments) !== JSON.stringify(["/C", "echo graphcoder-native-approval>graphcoder-native-effect.txt"])) fail(`argv was not exact: ${JSON.stringify(action)}`);
    if (resolve(action.working_directory) !== resolve(checkout)) fail(`cwd was not exact: ${JSON.stringify(action)}`);
    if (JSON.stringify(action.environment) !== JSON.stringify({ kind: "explicit", variables: { GRAPHCODER_FIXTURE: "native-approval" } })) fail(`environment was not explicit: ${JSON.stringify(action)}`);
    if (JSON.stringify(action).includes("operator-secret")) fail("operator credential appeared in action projection");
    const forged = await request("forged", "operator_approve", {
      operator_token: "operator-secret", session_id: sessionId,
      approval_id: "00000000-0000-0000-0000-000000000001", approved: true,
    });
    if (forged.ok || forged.error?.code !== "not_found") fail(`forged approval identity was accepted: ${JSON.stringify(forged)}`);
    const wrong = await request("wrong-operator", "operator_approve", {
      operator_token: "wrong", session_id: sessionId, approval_id: ticket.id, approved: true,
    });
    if (wrong.ok || wrong.error?.code !== "denied") fail(`wrong operator was accepted: ${JSON.stringify(wrong)}`);
    const beforeApproval = await request("before-approval", "native_process", { session_id: sessionId, approval_id: ticket.id });
    if (beforeApproval.ok || beforeApproval.error?.code !== "denied") fail(`unapproved host action was dispatched: ${JSON.stringify(beforeApproval)}`);
    if (existsSync(join(checkout, "graphcoder-native-effect.txt"))) fail("unapproved host action produced a side effect");
    const approved = await request("approve", "operator_approve", {
      operator_token: "operator-secret", session_id: sessionId, approval_id: ticket.id, approved: true,
    });
    if (!approved.ok || approved.result?.approved !== true) fail(`exact approval did not commit: ${JSON.stringify(approved)}`);
    const executed = await request("execute", "native_process", { session_id: sessionId, approval_id: ticket.id });
    if (!executed.ok || executed.result?.status !== "succeeded") fail(`approved effect did not succeed: ${JSON.stringify(executed)}`);
    if (!existsSync(join(checkout, "graphcoder-native-effect.txt"))) fail("native effect did not create its exact output");
    if (!readFileSync(join(checkout, "graphcoder-native-effect.txt"), "utf8").includes("graphcoder-native-approval")) fail("native output was not observed");
    const replay = await request("replay", "native_process", { session_id: sessionId, approval_id: ticket.id });
    if (!replay.ok || replay.result?.status !== "succeeded") fail(`replayed effect was not durable: ${JSON.stringify(replay)}`);
    const exit = await close();
    if (exit.timeout || exit.code !== 0) fail(`native process did not close cleanly: ${JSON.stringify(exit)} ${stderr.join("")}`);
    return { sessionId, approvalId: ticket.id, status: executed.result.status, replay: replay.result.status };
  } finally {
    if (child.exitCode === null) await close();
    output.return?.();
  }
}

export async function runNativeHostExecutionScenario({ runtime = required("GRAPHCODER_NATIVE_RUNTIME") } = {}) {
  regularFile(runtime);
  const root = mkdtempSync(join(tmpdir(), "graphcoder-native-host-"));
  const checkout = mkdtempSync(join(tmpdir(), "graphcoder-native-host-checkout-"));
  try { return await runScenario(runtime, root, checkout); }
  finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(checkout, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runNativeHostExecutionScenario().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["pending-approval", "operator-inspection", "forged-identity", "denied-operator", "approved-host-effect", "durable-replay"], result }, null, 2)}\n`);
  }).catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
