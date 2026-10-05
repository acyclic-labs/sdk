#!/usr/bin/env node

// Public, headless GraphCoder terminal contract fixture.  This driver talks
// only to the installed JSON-line bridge, so interactive terminals and native
// applications can reuse the same request sequence.  It deliberately keeps
// source checkout writeback behind an opaque, Harness-sealed inspection;
// a path or an operator token is never enough to publish changes.

import { createRequire } from "node:module";
import { existsSync, lstatSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve, dirname, relative } from "node:path";
import { pathToFileURL } from "node:url";

function fail(message) {
  throw new Error(`graphcoder-terminal-interface-fixture: ${message}`);
}

function required(name) {
  const value = process.env[name];
  if (typeof value !== "string" || value.trim() === "") fail(`${name} is required`);
  return value;
}

function jsonEnvironment(name, fallback = "{}") {
  try {
    return JSON.parse(process.env[name] ?? fallback);
  } catch (error) {
    fail(`${name} is invalid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function object(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${label} is not an object`);
  return value;
}

function responseValue(response, requestId, method) {
  if (!response || response.request_id !== requestId) fail(`${method} returned the wrong request identity`);
  if (response.ok !== true) fail(`${method} failed: ${JSON.stringify(response.error ?? response)}`);
  return response.result;
}

function expectPage(value, method) {
  const page = object(value, `${method} result`);
  if (!Array.isArray(page.items)) fail(`${method} did not return a bounded item page`);
  return page;
}

function expectSession(value, method) {
  const snapshot = object(value, `${method} result`);
  const selected = object(snapshot.summary, `${method} summary`);
  if (typeof selected.id !== "string" || selected.id.length === 0) fail(`${method} omitted the session identity`);
  if (!Array.isArray(snapshot.agents)) fail(`${method} omitted the agent tree`);
  return snapshot;
}

const packageRoot = resolve(required("GRAPHCODER_PACKAGE_ROOT"));
const bridgeExecutable = required("GRAPHCODER_BRIDGE_EXECUTABLE");
const bridgeCwd = resolve(required("GRAPHCODER_BRIDGE_CWD"));
const bridgeArgs = jsonEnvironment("GRAPHCODER_BRIDGE_ARGS_JSON", "[]");
const bridgeEnvironment = jsonEnvironment("GRAPHCODER_BRIDGE_ENV_JSON", "{}");
const fixture = process.env.GRAPHCODER_MOCK_FIXTURE ?? "complete";
const sourceCheckout = process.env.GRAPHCODER_TERMINAL_FIXTURE_CHECKOUT;
const projectId = process.env.GRAPHCODER_TERMINAL_FIXTURE_PROJECT_ID;
const evidencePath = resolve(process.env.GRAPHCODER_TERMINAL_FIXTURE_EVIDENCE ?? "target/graphcoder-terminal-interface-evidence.json");
if (!existsSync(packageRoot)) fail("GRAPHCODER_PACKAGE_ROOT does not exist");
if (!Array.isArray(bridgeArgs) || bridgeArgs.some(value => typeof value !== "string")) fail("bridge args must be a string array");
if (!bridgeEnvironment || typeof bridgeEnvironment !== "object" || Array.isArray(bridgeEnvironment)) fail("bridge environment must be an object");
const allowedEnvironment = new Set(["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT", "GRAPHCODER_MOCK_FIXTURE", "GRAPHCODER_OPERATOR_TOKEN", "GRAPHCODER_LAZY_OBSERVATION_PATH", "GRAPHCODER_REQUIRE_LAZY_COUNTERS"]);
if (Object.keys(bridgeEnvironment).some(key => !allowedEnvironment.has(key))) fail("bridge environment contains an undeclared key");
if ((sourceCheckout === undefined) !== (projectId === undefined)) fail("checkout and project identity must be selected together");
if (sourceCheckout !== undefined && projectId !== undefined) {
  const checkout = resolve(sourceCheckout);
  if (!lstatSync(checkout, { throwIfNoEntry: false })?.isDirectory() || lstatSync(checkout).isSymbolicLink()) fail("selected checkout must be a real directory");
  const checkoutIndex = bridgeArgs.indexOf("--checkout");
  const projectIndex = bridgeArgs.indexOf("--project-id");
  if (checkoutIndex < 0 || bridgeArgs[checkoutIndex + 1] !== checkout) fail("bridge args must carry the exact selected checkout");
  if (projectIndex < 0 || bridgeArgs[projectIndex + 1] !== projectId) fail("bridge args must carry the exact selected project identity");
}

const packageJson = resolve(packageRoot, "package.json");
let bridgePath;
try {
  bridgePath = createRequire(packageJson).resolve("@acyclic-labs/graphcoder/bridge");
} catch (error) {
  fail(`installed bridge export cannot be resolved: ${error instanceof Error ? error.message : String(error)}`);
}
if (relative(packageRoot, resolve(bridgePath)).startsWith("..")) fail("bridge export resolved outside the package root");
const { JsonLineGraphCoderBridge } = await import(pathToFileURL(bridgePath).href);
const bridge = new JsonLineGraphCoderBridge({ executable: bridgeExecutable, args: bridgeArgs, cwd: bridgeCwd, env: bridgeEnvironment });
const observations = [];
let requestNumber = 0;
let rootId;
let evidence;

async function request(method, params) {
  const requestId = `terminal-fixture-${++requestNumber}`;
  const response = await bridge.request({ request_id: requestId, method, params });
  observations.push({ request_id: requestId, method, ok: response?.ok === true, error_code: response?.error?.code ?? null });
  return { requestId, response };
}

try {
  const initial = await request("list_sessions", { limit: 8 });
  const initialPage = expectPage(responseValue(initial.response, initial.requestId, "list_sessions"), "list_sessions");

  const started = await request("start_session", {
    prompt: "terminal interface fixture: complete one deterministic local turn",
    operation_id: "terminal-interface-root",
    model_fixture: fixture,
  });
  const startSnapshot = expectSession(responseValue(started.response, started.requestId, "start_session"), "start_session");
  rootId = startSnapshot.summary.id;

  for (const method of ["open_session", "resume_session"]) {
    const opened = await request(method, { session_id: rootId });
    expectSession(responseValue(opened.response, opened.requestId, method), method);
  }

  const input = await request("input_session", {
    session_id: rootId,
    prompt: "terminal interface fixture: accept a fresh user turn",
    operation_id: "terminal-interface-follow-up",
  });
  expectSession(responseValue(input.response, input.requestId, "input_session"), "input_session");

  const activity = await request("read_activity", { session_id: rootId, limit: 64 });
  expectPage(responseValue(activity.response, activity.requestId, "read_activity"), "read_activity");
  const messages = await request("read_messages", { session_id: rootId, limit: 64 });
  expectPage(responseValue(messages.response, messages.requestId, "read_messages"), "read_messages");
  const approvals = await request("list_approvals", { session_id: rootId, limit: 64 });
  expectPage(responseValue(approvals.response, approvals.requestId, "list_approvals"), "list_approvals");

  const changes = await request("list_changes", { session_id: rootId, limit: 64 });
  const changePage = expectPage(responseValue(changes.response, changes.requestId, "list_changes"), "list_changes");
  if (typeof changePage.generation !== "string" || changePage.generation.length === 0) fail("list_changes omitted its generation fence");

  const sent = await request("send_message", {
    session_id: rootId,
    sender_id: rootId,
    recipient_id: rootId,
    body: "terminal interface fixture message",
  });
  object(responseValue(sent.response, sent.requestId, "send_message"), "send_message result");

  // The typed writeback routes intentionally require a Harness-sealed
  // inspection handle. The fixture may exercise inspect/apply/recover when a
  // host supplies one; otherwise it proves that a bare checkout path cannot
  // authorize it (a bare checkout path cannot authorize writeback).
  const writebackInput = process.env.GRAPHCODER_TERMINAL_FIXTURE_WRITEBACK_JSON;
  if (writebackInput) {
    const writeback = object(jsonEnvironment("GRAPHCODER_TERMINAL_FIXTURE_WRITEBACK_JSON"), "writeback fixture");
    const inspected = await request("inspect_writeback", { ...writeback.inspect, session_id: rootId });
    const inspection = object(responseValue(inspected.response, inspected.requestId, "inspect_writeback"), "inspection");
    const applied = await request("apply_writeback", { ...writeback.apply, session_id: rootId, inspection });
    object(responseValue(applied.response, applied.requestId, "apply_writeback"), "apply_writeback result");
    if (writeback.recover) {
      const recovered = await request("recover_writeback", { ...writeback.recover, session_id: rootId, inspection });
      object(responseValue(recovered.response, recovered.requestId, "recover_writeback"), "recover_writeback result");
    }
  } else {
    const rejected = await request("apply_writeback", { session_id: rootId, inspection: { inspection_id: "unbound" } });
    if (rejected.response?.ok === true || !["invalid_input", "denied", "unsupported"].includes(rejected.response?.error?.code)) {
      fail("writeback accepted an unbound inspection handle");
    }
  }

  // Cancellation is terminal and durable.  A completed fixture may reject a
  // late cancellation; either outcome is valid only when it is typed and the
  // bridge remains usable for a final lazy page read.
  const cancelled = await request("cancel_session", { session_id: rootId });
  if (cancelled.response?.ok === true) expectSession(responseValue(cancelled.response, cancelled.requestId, "cancel_session"), "cancel_session");
  else if (!["conflict", "invalid_input", "denied"].includes(cancelled.response?.error?.code)) fail("cancel_session returned an untyped failure");

  const finalList = await request("list_sessions", { limit: 8 });
  expectPage(responseValue(finalList.response, finalList.requestId, "final list_sessions"), "final list_sessions");
  evidence = {
    protocol: "acyclic.graphcoder.terminal-interface-evidence.v1",
    fixture,
    root_id: rootId,
    source_checkout: sourceCheckout === undefined ? null : resolve(sourceCheckout),
    project_id: projectId ?? null,
    initial_session_count: initialPage.items.length,
    observations,
    writeback: writebackInput ? "harness-sealed-inspection" : "unbound-handle-rejected",
  };
} finally {
  bridge.close("terminal interface fixture finished");
  const exit = await bridge.waitForExit(5_000);
  if (exit.kind !== "closed") fail(`bridge cleanup was not verified: ${JSON.stringify(exit)}`);
}

mkdirSync(dirname(evidencePath), { recursive: true });
writeFileSync(evidencePath, `${JSON.stringify({ ...evidence, cleanup_verified: true }, null, 2)}\n`, { flag: "wx" });
process.stdout.write(`${JSON.stringify({ ...evidence, cleanup_verified: true })}\n`);
