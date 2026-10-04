#!/usr/bin/env node

// Source-bound installed-package driver for the real recursive swarm lane.
// This intentionally fails when the installed bridge does not expose the
// recursive/effect/writeback surface; a transport smoke test is not evidence
// for those contracts.

import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
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
function parseJsonEnvironment(name, fallback) {
  let value;
  try { value = JSON.parse(process.env[name] ?? fallback); }
  catch (error) { fail(`${name} is invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  return value;
}
function requiredJsonObject(name) {
  const value = parseJsonEnvironment(name);
  if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${name} must be a JSON object`);
  return value;
}
function requiredJsonObjectValue(parent, key, label) {
  const value = parent[key];
  if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${label} must be a JSON object`);
  return value;
}
function bytesDigest(value, label) {
  if (!Array.isArray(value) || value.some(item => !Number.isInteger(item) || item < 0 || item > 255)) fail(`${label} bytes are invalid`);
  return createHash("sha256").update(Buffer.from(value)).digest("hex");
}
function relativeCheckoutPath(path) {
  if (typeof path !== "string" || path.trim() === "" || /^[A-Za-z]:/u.test(path) || path.startsWith("/") || path.split(/[\\/]/u).includes("..")) fail(`checkout file path is not relative: ${path}`);
  return path;
}
function checkoutDigest(checkoutRoot, path, label) {
  const relativePath = relativeCheckoutPath(path);
  const absolutePath = resolve(checkoutRoot, relativePath);
  if (relative(checkoutRoot, absolutePath).startsWith("..")) fail(`${label} escapes the explicit checkout root`);
  const metadata = lstatSync(absolutePath, { throwIfNoEntry: false });
  if (!metadata?.isFile() || metadata.isSymbolicLink()) fail(`${label} is not a regular file: ${relativePath}`);
  const canonical = realpathSync(absolutePath);
  if (relative(checkoutRoot, canonical).startsWith("..")) fail(`${label} escapes the canonical checkout root`);
  return createHash("sha256").update(readFileSync(canonical)).digest("hex");
}

const packageRoot = resolve(required("GRAPHCODER_PACKAGE_ROOT"));
const bridgeExecutable = required("GRAPHCODER_BRIDGE_EXECUTABLE");
const bridgeArgs = parseJsonEnvironment("GRAPHCODER_BRIDGE_ARGS_JSON", "[]");
const bridgeEnvironment = parseJsonEnvironment("GRAPHCODER_BRIDGE_ENV_JSON", "{}");
const bridgeCwd = resolve(required("GRAPHCODER_BRIDGE_CWD"));
const checkoutRootInput = resolve(required("GRAPHCODER_SWARM_CHECKOUT_ROOT"));
const evidencePath = resolve(process.env.GRAPHCODER_SWARM_EVIDENCE_PATH ?? resolve("target/graphcoder-installed-swarm-evidence.json"));
const expectedFiles = requiredJsonObject("GRAPHCODER_SWARM_EXPECTED_FILES_JSON");
const expectedApproval = requiredJsonObject("GRAPHCODER_SWARM_EXPECTED_APPROVAL_JSON");
const expectedCommand = requiredJsonObject("GRAPHCODER_SWARM_EXPECTED_COMMAND_JSON");
const concurrentEdit = requiredJsonObject("GRAPHCODER_SWARM_CONCURRENT_EDIT_JSON");
if (Object.keys(expectedFiles).length === 0) fail("GRAPHCODER_SWARM_EXPECTED_FILES_JSON must contain at least one checkout file");
const commandApproval = requiredJsonObjectValue(expectedApproval, "command", "GRAPHCODER_SWARM_EXPECTED_APPROVAL_JSON.command");
const writebackApproval = requiredJsonObjectValue(expectedApproval, "writeback", "GRAPHCODER_SWARM_EXPECTED_APPROVAL_JSON.writeback");
const allowedBridgeEnvironment = new Set(["PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT", "GRAPHCODER_MOCK_FIXTURE", "GRAPHCODER_OPERATOR_TOKEN", "GRAPHCODER_LAZY_OBSERVATION_PATH", "GRAPHCODER_REQUIRE_LAZY_COUNTERS"]);
if (!Array.isArray(bridgeArgs) || bridgeArgs.some(value => typeof value !== "string")) fail("GRAPHCODER_BRIDGE_ARGS_JSON must be a string array");
if (!bridgeEnvironment || typeof bridgeEnvironment !== "object" || Array.isArray(bridgeEnvironment)) fail("GRAPHCODER_BRIDGE_ENV_JSON must be an object");
if (Object.keys(bridgeEnvironment).some(key => !allowedBridgeEnvironment.has(key))) fail("bridge environment contains an undeclared key");
if (typeof bridgeEnvironment.GRAPHCODER_OPERATOR_TOKEN !== "string" || bridgeEnvironment.GRAPHCODER_OPERATOR_TOKEN.trim() === "") fail("bridge environment must contain the explicit operator approval token");
if (!lstatSync(packageRoot, { throwIfNoEntry: false })?.isDirectory()) fail("GRAPHCODER_PACKAGE_ROOT must be a directory");
if (!lstatSync(checkoutRootInput, { throwIfNoEntry: false })?.isDirectory() || lstatSync(checkoutRootInput).isSymbolicLink()) fail("GRAPHCODER_SWARM_CHECKOUT_ROOT must be a real directory");
const checkoutRoot = realpathSync(checkoutRootInput);
if (relative(checkoutRootInput, checkoutRoot).startsWith("..")) fail("GRAPHCODER_SWARM_CHECKOUT_ROOT canonical path is invalid");
const packageArtifact = required("GRAPHCODER_PACKAGE_ARTIFACT");
if (!lstatSync(packageArtifact, { throwIfNoEntry: false })?.isFile()) fail("GRAPHCODER_PACKAGE_ARTIFACT must be a regular file");

const packageJsonPath = resolve(packageRoot, "package.json");
const resolveExport = createRequire(packageJsonPath);
let bridgePath;
try { bridgePath = resolveExport.resolve("@acyclic-labs/graphcoder/bridge"); }
catch (error) { fail(`installed bridge export cannot be resolved: ${error instanceof Error ? error.message : String(error)}`); }
if (relative(packageRoot, bridgePath).startsWith("..") || resolve(packageRoot, bridgePath) === packageRoot) fail("installed bridge export resolved outside the package artifact root");
const { JsonLineGraphCoderBridge } = await import(pathToFileURL(bridgePath).href);
const bridge = new JsonLineGraphCoderBridge({ executable: bridgeExecutable, args: bridgeArgs, cwd: bridgeCwd, env: bridgeEnvironment });
const observations = [];
let requestNumber = 0;
let evidence;
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
  const root = agents.find(agent => agent.id === rootId && agent.parent_id === null);
  if (!root) fail("recursive start omitted the root agent tree entry");
  const childA = agents.find(agent => agent.task === "child-a" && agent.parent_id === rootId);
  const childB = agents.find(agent => agent.task === "child-b" && agent.parent_id === rootId);
  const grandchild = agents.find(agent => agent.task === "grandchild" && agent.parent_id === childA?.id);
  if (!childA || !childB || !grandchild) fail("recursive tree is not exactly root -> child-a/child-b -> grandchild");

  const activity = await request("read_activity", { session_id: rootId, limit: 256 });
  responseValue(activity.response, activity.requestId, "read_activity");
  const messages = await request("read_messages", { session_id: rootId, limit: 256 });
  responseValue(messages.response, messages.requestId, "read_messages");
  const beforeFiles = {};
  for (const [path, expectation] of Object.entries(expectedFiles)) {
    if (!expectation || typeof expectation.before_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(expectation.before_sha256) || typeof expectation.workspace_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(expectation.workspace_sha256) || typeof expectation.after_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(expectation.after_sha256)) fail(`expected checkout file ${path} lacks valid physical/workspace SHA-256 digests`);
    beforeFiles[path] = checkoutDigest(checkoutRoot, path, "pre-approval checkout file");
    if (beforeFiles[path] !== expectation.before_sha256) fail(`checkout file ${path} changed before approval`);
  }
  async function pendingApproval(expected, label) {
    const approvals = await request("list_approvals", { session_id: rootId, limit: 256 });
    const page = object(responseValue(approvals.response, approvals.requestId, `list_approvals ${label}`), `${label} approval page`);
    const item = (page.items ?? []).find(candidate => candidate?.state === "pending" && candidate?.kind === expected.kind);
    if (!item) fail(`recursive swarm did not publish a typed pending ${label} approval`);
    if (typeof item.id !== "string" || item.id.trim() === "" || item.operation_id !== expected.operation_id || item.action_digest !== expected.action_digest) fail(`${label} approval operation/action does not match the expected action`);
    return item;
  }
  const command = await pendingApproval(commandApproval, "command");
  if (command.action !== commandApproval.action || command.executable !== expectedCommand.executable || JSON.stringify(command.arguments) !== JSON.stringify(expectedCommand.arguments) || command.cwd !== expectedCommand.cwd || JSON.stringify(command.environment) !== JSON.stringify(expectedCommand.environment)) fail("command approval does not expose the exact executable, arguments, cwd, and environment scope");

  const operator = await request("operator_approve", { session_id: rootId, approval_id: command.id, approved: true, operator_token: bridgeEnvironment.GRAPHCODER_OPERATOR_TOKEN });
  responseValue(operator.response, operator.requestId, "operator_approve");
  const resolved = await request("resolve_approval", { session_id: rootId, approval_id: command.id, approved: true });
  const resolvedApproval = object(responseValue(resolved.response, resolved.requestId, "resolve_approval"), "resolved approval");
  if (resolvedApproval.state !== "approved" || resolvedApproval.operation_id !== command.operation_id) fail("public command approval resolution is not bound to the host approval");

  const changes = await request("list_changes", { session_id: rootId });
  if (changes.response?.ok !== true) fail(`list_changes is not available for installed swarm qualification: ${JSON.stringify(changes.response?.error ?? changes.response)}`);
  const changePage = object(responseValue(changes.response, changes.requestId, "list_changes"), "change page");
  const generation = changePage.generation;
  for (const [path, expectation] of Object.entries(expectedFiles)) {
    const file = await request("read_file", { session_id: rootId, path, generation });
    const body = responseValue(file.response, file.requestId, `read_file ${path}`);
    beforeFiles[path] = bytesDigest(body.bytes, `read_file ${path}`);
    if (beforeFiles[path] !== expectation.workspace_sha256) fail(`private workspace file ${path} does not match its expected agent-edited bytes`);
  }
  const concurrentPath = relativeCheckoutPath(concurrentEdit.path);
  if (Object.prototype.hasOwnProperty.call(expectedFiles, concurrentPath)) fail("concurrent edit path must be separate from agent-published files");
  if (typeof concurrentEdit.before_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(concurrentEdit.before_sha256) || !Array.isArray(concurrentEdit.bytes) || typeof concurrentEdit.after_sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(concurrentEdit.after_sha256)) fail("concurrent edit must declare path, before/after SHA-256 digests, and bytes");
  if (checkoutDigest(checkoutRoot, concurrentPath, "concurrent-edit checkout file") !== concurrentEdit.before_sha256) fail("concurrent-edit file changed before the declared user edit");
  if (bytesDigest(concurrentEdit.bytes, "concurrent edit") !== concurrentEdit.after_sha256) fail("concurrent edit bytes do not match their declared digest");
  writeFileSync(resolve(checkoutRoot, concurrentPath), Buffer.from(concurrentEdit.bytes));
  if (checkoutDigest(checkoutRoot, concurrentPath, "declared user edit") !== concurrentEdit.after_sha256) fail("declared concurrent user edit was not observed on disk");
  for (const [path, expectation] of Object.entries(expectedFiles)) if (checkoutDigest(checkoutRoot, path, "pre-writeback checkout file") !== expectation.before_sha256) fail(`checkout file ${path} changed before writeback approval`);
  const writeback = await pendingApproval(writebackApproval, "writeback");
  const writebackOperator = await request("operator_approve", { session_id: rootId, approval_id: writeback.id, approved: true, operator_token: bridgeEnvironment.GRAPHCODER_OPERATOR_TOKEN });
  responseValue(writebackOperator.response, writebackOperator.requestId, "operator_approve writeback");
  const writebackResolved = await request("resolve_approval", { session_id: rootId, approval_id: writeback.id, approved: true });
  const resolvedWriteback = object(responseValue(writebackResolved.response, writebackResolved.requestId, "resolve_approval writeback"), "resolved writeback approval");
  if (resolvedWriteback.state !== "approved" || resolvedWriteback.operation_id !== writeback.operation_id) fail("public writeback approval resolution is not bound to the host approval");
  const change = await request("read_change", { session_id: rootId, path: Object.keys(expectedFiles)[0], generation });
  if (change.response?.ok !== true) fail(`read_change is not available for installed swarm qualification: ${JSON.stringify(change.response?.error ?? change.response)}`);
  responseValue(change.response, change.requestId, "read_change");
  const approved = await request("approve_writeback", {
    session_id: rootId,
    operation_id: writeback.operation_id,
    expected_generation: generation,
    approved: true,
  });
  responseValue(approved.response, approved.requestId, "approve_writeback");
  if (!approved.response.result?.applied || approved.response.result.operation_id !== writeback.operation_id || approved.response.result.generation !== generation || approved.response.result.concurrent_user_edit_preserved !== true) fail("writeback did not return exact operation/generation and concurrent-user reconciliation evidence");
  for (const [path, expectation] of Object.entries(expectedFiles)) {
    if (checkoutDigest(checkoutRoot, path, "post-writeback checkout file") !== expectation.after_sha256) fail(`checkout file ${path} does not match exact post-writeback bytes`);
  }
  if (checkoutDigest(checkoutRoot, concurrentPath, "post-writeback concurrent-edit file") !== concurrentEdit.after_sha256) fail("concurrent user edit was not preserved on disk");

  evidence = { protocol: "acyclic.graphcoder.installed-swarm-evidence.v1", root_id: rootId, agents, observations, recursive: true, native_approval: true, writeback: true };
} finally {
  bridge.close("installed recursive swarm qualification finished");
  const exit = await bridge.waitForExit(5_000);
  if (exit.kind !== "closed") fail(`bridge cleanup was not verified: ${JSON.stringify(exit)}`);
}
mkdirSync(dirname(evidencePath), { recursive: true });
writeFileSync(evidencePath, `${JSON.stringify({ ...evidence, cleanup_verified: true }, null, 2)}\n`, { flag: "wx" });
process.stdout.write(`${JSON.stringify({ ...evidence, cleanup_verified: true })}\n`);
