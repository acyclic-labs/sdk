#!/usr/bin/env node

// Source-bound installed-package driver for the real recursive swarm lane.
// This intentionally fails when the installed bridge does not expose the
// recursive/effect/writeback surface; a transport smoke test is not evidence
// for those contracts.

import { createRequire } from "node:module";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

function fail(message) { throw new Error(`graphcoder-installed-swarm-e2e: ${message}`); }
function required(name) {
  const value = process.env[name];
  if (typeof value !== "string" || value.trim() === "") fail(`${name} is required`);
  return value;
}
function object(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${label} is not an object`);
  return value;
}
function responseValue(response, requestId, label) {
  if (!response || response.request_id !== requestId) fail(`${label} response identity is invalid`);
  if (response.ok !== true) fail(`${label} failed: ${JSON.stringify(response.error ?? response)}`);
  return response.result;
}
function responseError(response, requestId, label, code) {
  if (!response || response.request_id !== requestId || response.ok !== false || response.error?.code !== code) {
    fail(`${label} did not fail with ${code}: ${JSON.stringify(response)}`);
  }
  return response.error;
}
function parseJsonEnvironment(name, fallback) {
  let value;
  try { value = JSON.parse(process.env[name] ?? fallback); }
  catch (error) { fail(`${name} is invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  return value;
}

const packageRoot = resolve(required("GRAPHCODER_PACKAGE_ROOT"));
const bridgeExecutable = required("GRAPHCODER_BRIDGE_EXECUTABLE");
const bridgeArgs = parseJsonEnvironment("GRAPHCODER_BRIDGE_ARGS_JSON", "[]");
const bridgeEnvironment = parseJsonEnvironment("GRAPHCODER_BRIDGE_ENV_JSON", "{}");
const bridgeCwd = resolve(process.env.GRAPHCODER_BRIDGE_CWD ?? process.cwd());
const evidencePath = resolve(process.env.GRAPHCODER_SWARM_EVIDENCE_PATH ?? resolve("target/graphcoder-installed-swarm-evidence.json"));
if (!Array.isArray(bridgeArgs) || bridgeArgs.some(value => typeof value !== "string")) fail("GRAPHCODER_BRIDGE_ARGS_JSON must be a string array");
if (!bridgeEnvironment || typeof bridgeEnvironment !== "object" || Array.isArray(bridgeEnvironment)) fail("GRAPHCODER_BRIDGE_ENV_JSON must be an object");
if (Object.keys(bridgeEnvironment).some(key => /(?:TOKEN|PASSWORD|SECRET|CREDENTIAL|PRIVATE_KEY|ACCESS_KEY|API_KEY)/iu.test(key))) fail("bridge environment contains a credential key");

const packageJsonPath = resolve(packageRoot, "package.json");
const resolveExport = createRequire(packageJsonPath);
let bridgePath;
try { bridgePath = resolveExport.resolve("@acyclic-labs/graphcoder/bridge"); }
catch (error) { fail(`installed bridge export cannot be resolved: ${error instanceof Error ? error.message : String(error)}`); }
const { JsonLineGraphCoderBridge } = await import(pathToFileURL(bridgePath).href);
const bridge = new JsonLineGraphCoderBridge({ executable: bridgeExecutable, args: bridgeArgs, cwd: bridgeCwd, env: bridgeEnvironment });
const observations = [];
let requestNumber = 0;
async function request(method, params) {
  const requestId = `installed-swarm-${++requestNumber}`;
  const response = await bridge.request({ request_id: requestId, method, params });
  observations.push({ request_id: requestId, method, ok: response?.ok === true, error_code: response?.error?.code ?? null });
  return { requestId, response };
}

try {
  const listed = await request("list_sessions", {});
  responseValue(listed.response, listed.requestId, "list_sessions");

  const started = await request("start_session", {
    prompt: "recursive swarm qualification: fork two children, fork one grandchild, communicate, execute an approved native command, and publish a writeback",
    operation_id: "installed-swarm-recursive-root",
    model_fixture: "recursive",
  });
  const snapshot = object(responseValue(started.response, started.requestId, "start_session"), "start_session result");
  const rootId = snapshot.summary?.id ?? snapshot.summary?.task_id;
  if (typeof rootId !== "string") fail("start_session omitted root identity");
  const agents = Array.isArray(snapshot.agents) ? snapshot.agents : [];
  const root = agents.find(agent => agent.id === rootId || agent.parent_id === null);
  if (!root) fail("recursive start omitted root agent tree entry");
  const children = agents.filter(agent => agent.parent_id === rootId);
  if (children.length < 2) fail(`recursive start exposed ${children.length} direct children; two are required`);
  if (!agents.some(agent => children.some(child => agent.parent_id === child.id))) fail("recursive start omitted a grandchild");

  const activity = await request("read_activity", { session_id: rootId, limit: 256 });
  responseValue(activity.response, activity.requestId, "read_activity");
  const messages = await request("read_messages", { session_id: rootId, limit: 256 });
  responseValue(messages.response, messages.requestId, "read_messages");
  const approvals = await request("list_approvals", { session_id: rootId, limit: 256 });
  const approvalPage = object(responseValue(approvals.response, approvals.requestId, "list_approvals"), "approval page");
  const pending = (approvalPage.items ?? []).find(item => item?.state === "pending");
  if (!pending) fail("recursive swarm did not publish a pending native/writeback approval");

  const changes = await request("list_changes", { session_id: rootId });
  if (changes.response?.ok !== true) responseError(changes.response, changes.requestId, "list_changes", "unsupported");
  const changePage = object(responseValue(changes.response, changes.requestId, "list_changes"), "change page");
  const generation = changePage.generation;
  const change = await request("read_change", { session_id: rootId, path: "README.md", generation });
  responseValue(change.response, change.requestId, "read_change");
  const approved = await request("approve_writeback", {
    session_id: rootId,
    operation_id: pending.operation_id ?? pending.operationId,
    expected_generation: generation,
    approved: true,
  });
  responseValue(approved.response, approved.requestId, "approve_writeback");
  if (!approved.response.result?.applied) fail("writeback approval did not return applied=true");

  const evidence = { protocol: "acyclic.graphcoder.installed-swarm-evidence.v1", root_id: rootId, agents, observations, recursive: true, native_approval: true, writeback: true };
  mkdirSync(dirname(evidencePath), { recursive: true });
  writeFileSync(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`, { flag: "wx" });
  process.stdout.write(`${JSON.stringify(evidence)}\n`);
} finally {
  bridge.close("installed recursive swarm qualification finished");
  const exit = await bridge.waitForExit(5_000);
  if (exit.kind !== "closed") fail(`bridge cleanup was not verified: ${JSON.stringify(exit)}`);
}
