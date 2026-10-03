import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index < 0 ? fallback : process.argv[index + 1];
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function loadExport(consumerRoot, specifier) {
  const require = createRequire(resolve(consumerRoot, "package.json"));
  return import(pathToFileURL(require.resolve(specifier)).href);
}

async function expectCode(label, code, operation) {
  try {
    await operation();
  } catch (error) {
    assert(error?.code === code, `${label}: expected ${code}, got ${error?.code ?? String(error)}`);
    return;
  }
  throw new Error(`${label}: operation unexpectedly succeeded`);
}

async function withProcessBridge(nodeApi, bridgeApi, hostPath, run, options = {}) {
  const bridge = new nodeApi.JsonLineGraphCoderBridge({ executable: process.execPath, args: [resolve(hostPath), ...(options.args ?? [])], env: { PATH: process.env.PATH ?? "" }, maximumLineBytes: options.maximumLineBytes });
  try {
    return await run(bridge, new bridgeApi.HarnessGraphCoderTransport(bridge));
  } finally {
    bridge.close();
  }
}

function snapshot(api, id = "session-1") {
  return {
    summary: { id: api.sessionId(id), title: "qualification", state: "running", updatedAt: "2026-01-01T00:00:00.000Z", rootAgentId: api.agentId("agent-1") },
    agents: [{ id: api.agentId("agent-1"), parentId: null, task: "qualification", state: "running", depth: 0, children: [] }],
    workspaceGeneration: 7n,
  };
}

const consumerRoot = option("--root");
const hostPath = option("--host");
const epochHostPath = option("--epoch-host");
if (consumerRoot === undefined || hostPath === undefined || epochHostPath === undefined) {
  throw new Error("usage: node negative-conformance.mjs --root <installed-consumer> --host <negative-host.mjs> --epoch-host <epoch-host.mjs>");
}

const api = await loadExport(consumerRoot, "@acyclic-labs/graphcoder");
const bridgeApi = await loadExport(consumerRoot, "@acyclic-labs/graphcoder/bridge");
const nodeApi = await loadExport(consumerRoot, "@acyclic-labs/graphcoder/node");
const terminalApi = await loadExport(consumerRoot, "@acyclic-labs/graphcoder/terminal");
assert(typeof terminalApi.runCliWithTransport === "function", "NEG-ERROR-01 requires the installed structured terminal adapter");

await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-IDENTITY-01 snapshot", "transport", () => transport.openSession(api.sessionId("session-1")));
}, { args: ["--case", "wrong-session"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-IDENTITY-01 page", "transport", () => transport.readActivity(api.sessionId("session-1")));
}, { args: ["--case", "wrong-page-session"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-IDENTITY-01 path", "transport", () => transport.readChange(api.sessionId("session-1"), "README.md", 7n));
}, { args: ["--case", "wrong-path"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-APPROVAL-01 receipt", "transport", () => transport.approveWriteback({ sessionId: api.sessionId("session-1"), operationId: "op-7", expectedGeneration: 7n, approved: true }));
}, { args: ["--case", "wrong-receipt"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  const ui = new api.GraphCoderUi(transport);
  await ui.dispatch({ kind: "open_session", sessionId: api.sessionId("session-1") });
  await expectCode("NEG-APPROVAL-01 session", "transport", () => ui.dispatch({ kind: "resolve_approval", approvalId: api.approvalId("approval-1"), approved: true }));
}, { args: ["--case", "wrong-approval-session"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-APPROVAL-01 missing session", "invalid_input", () => transport.resolveApproval({ approvalId: api.approvalId("approval-1"), approved: true }));
}, { args: ["--case", "missing-approval-session"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-BYTES-01", "transport", () => transport.readFile(api.sessionId("session-1"), "README.md", 7n));
}, { args: ["--case", "bad-bytes"] });

// The process adapter owns the line bound independently of the transport's
// response-envelope bound.
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-BYTES-01 line", "transport", () => transport.listSessions());
}, { args: ["--case", "oversized"], maximumLineBytes: 256 });

await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-ERROR-01 envelope", "transport", () => transport.listSessions());
}, { args: ["--case", "malformed-envelope"] });
await withProcessBridge(nodeApi, bridgeApi, hostPath, async (_bridge, transport) => {
  await expectCode("NEG-ERROR-01 backend code", "denied", () => transport.listSessions());
}, { args: ["--case", "backend-error"] });

const pathRequests = [];
const pathBridge = {
  request(request) {
    pathRequests.push(request);
    return Promise.resolve({ request_id: request.request_id, ok: true, result: { path: request.params.path, media_type: "text/plain", bytes: [1], generation: "7" } });
  },
};
const pathTransport = new bridgeApi.HarnessGraphCoderTransport(pathBridge);
for (const path of ["", "  ", ".", "..", "/absolute", "C:\\absolute", "..\\secret", "a/../b", "a//b", "\u0000bad", "a".repeat(4097)]) {
  await expectCode(`NEG-PATH-01 ${JSON.stringify(path.slice(0, 24))}`, "invalid_input", () => pathTransport.readFile(api.sessionId("session-1"), path, 7n));
}
for (const path of [null, 1, {}, []]) {
  await expectCode(`NEG-INPUT-01 path ${String(path)}`, "invalid_input", () => pathTransport.readFile(api.sessionId("session-1"), path, 7n));
}
await expectCode("NEG-INPUT-01 page cursor", "invalid_input", () => pathTransport.listSessions({ after: 42 }));
await expectCode("NEG-INPUT-01 session prompt", "invalid_input", () => pathTransport.startSession({ prompt: null }));
await expectCode("NEG-INPUT-01 session id", "invalid_input", () => pathTransport.openSession(null));
assert(pathRequests.length === 0, "NEG-PATH-01 sent invalid paths to the bridge");
const boundaryPath = "a".repeat(4096);
const boundaryFile = await pathTransport.readFile(api.sessionId("session-1"), boundaryPath, 7n);
assert(boundaryFile.path === boundaryPath && pathRequests.length === 1, "NEG-PATH-01 rejected the 4096-byte relative path boundary");

const retainedError = new api.GraphCoderError("stale", "workspace generation changed");
const throwingTransport = {
  listSessions: async () => { throw retainedError; },
};
const errorOutput = [];
const errorStatus = await terminalApi.runCliWithTransport(["list"], throwingTransport, { output: { write(value) { errorOutput.push(String(value)); return true; } } });
assert(errorStatus === 1, `NEG-ERROR-01 terminal status was ${errorStatus}`);
const errorLine = JSON.parse(errorOutput.join("").trim());
assert(errorLine.ok === false && errorLine.error?.code === "stale", "NEG-ERROR-01 terminal dropped the typed error code");

let cancellationCalls = 0;
const selected = snapshot(api);
const cancelTransport = {
  openSession: async () => selected,
  readActivity: async () => new Promise(() => undefined),
  cancelSession: async () => { cancellationCalls += 1; return { ...selected, summary: { ...selected.summary, state: "cancelled" } }; },
};
const ui = new api.GraphCoderUi(cancelTransport);
await ui.dispatch({ kind: "open_session", sessionId: api.sessionId("session-1") });
const hung = ui.dispatch({ kind: "load_activity" });
hung.catch(() => undefined);
await new Promise(resolve => setTimeout(resolve, 10));
const cancelResult = await Promise.race([
  ui.dispatch({ kind: "cancel_session" }),
  new Promise((_, reject) => setTimeout(() => reject(new Error("NEG-CANCEL-01 cancellation remained behind a hung history request")), 250)),
]);
assert(cancelResult.selectedSession?.summary.state === "cancelled" && cancellationCalls === 1, "NEG-CANCEL-01 did not dispatch cancellation independently");
void hung;

const epochBridge = new nodeApi.JsonLineGraphCoderBridge({ executable: process.execPath, args: [resolve(epochHostPath)], env: { PATH: process.env.PATH ?? "" } });
try {
  const first = epochBridge.request({ request_id: "same", method: "list_sessions", params: {} });
  assert(epochBridge.cancel("same", "fixture cancellation"), "NEG-EPOCH-01 could not cancel the first request");
  await expectCode("NEG-EPOCH-01 cancelled request", "transport", () => first);
  const second = epochBridge.request({ request_id: "same", method: "list_sessions", params: {} });
  await expectCode("NEG-EPOCH-01 id quarantine", "invalid_input", () => second);
} finally {
  epochBridge.close();
}

process.stdout.write(JSON.stringify({
  ok: true,
  scenarios: ["NEG-CANCEL-01", "NEG-EPOCH-01", "NEG-PATH-01", "NEG-INPUT-01", "NEG-ERROR-01", "NEG-APPROVAL-01", "NEG-BYTES-01", "NEG-IDENTITY-01"],
}) + "\n");
