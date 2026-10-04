import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { assertLazyCounters, inspectInstalledPackage, loadInstalledExport } from "./package-contract.mjs";

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index < 0 ? fallback : process.argv[index + 1];
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const consumerRoot = option("--root");
const hostPath = option("--host");
const logPath = option("--log");
const artifactPath = option("--artifact");
const lazyObservationPath = option("--lazy-observation");
if (consumerRoot === undefined || hostPath === undefined || logPath === undefined) {
  throw new Error("usage: node consumer.mjs --root <installed-consumer> --host <host.mjs> --log <request-log> [--artifact package.tgz] [--lazy-observation counters.json]");
}

const packageRoot = resolve(consumerRoot, "node_modules/@acyclic-labs/graphcoder");
const identity = inspectInstalledPackage(packageRoot, { artifactPath });
const api = await loadInstalledExport(consumerRoot, "@acyclic-labs/graphcoder").module;
const bridgeApi = await loadInstalledExport(consumerRoot, "@acyclic-labs/graphcoder/bridge").module;
const nodeApi = await loadInstalledExport(consumerRoot, "@acyclic-labs/graphcoder/node").module;
const terminalApi = await loadInstalledExport(consumerRoot, "@acyclic-labs/graphcoder/terminal").module;
writeFileSync(logPath, "");

const processBridge = new nodeApi.JsonLineGraphCoderBridge({
  executable: process.execPath,
  args: [resolve(hostPath)],
  env: { PATH: process.env.PATH ?? "", GRAPH_CODER_HOST_LOG: resolve(logPath) },
});
const transport = new bridgeApi.HarnessGraphCoderTransport(processBridge);

try {
  const sessions = await transport.listSessions({ limit: 32 });
  assert(sessions.items.length === 1 && sessions.items[0].id === api.sessionId("session-1"), "installed bridge did not decode session listing");

  const ui = new api.GraphCoderUi(transport);
  await ui.dispatch({ kind: "open_session", sessionId: api.sessionId("session-1") });
  await ui.dispatch({ kind: "load_activity" });
  await ui.dispatch({ kind: "load_messages" });
  await ui.dispatch({ kind: "load_approvals" });
  await ui.dispatch({ kind: "list_changes" });
  await ui.dispatch({ kind: "read_change", path: "README.md" });
  await ui.dispatch({ kind: "read_file", path: "README.md" });
  assert(ui.state().activity.length === 1, "activity page was not decoded");
  assert(ui.state().messages.length === 1, "message page was not decoded");
  assert(ui.state().approvals[0]?.sessionId === api.sessionId("session-1"), "approval session identity was not decoded");
  assert(ui.state().changeBody?.unifiedDiff.includes("fixture"), "snake_case diff body was not decoded");
  assert(ui.state().fileBody?.mediaType === "text/markdown", "snake_case file body was not decoded");

  const approval = ui.state().approvals[0];
  assert(approval !== undefined, "native fixture did not expose approval");
  await ui.dispatch({ kind: "resolve_approval", approvalId: approval.id, approved: true });
  await ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 7n, approved: true });
  assert(ui.state().writeback?.applied === true, "writeback receipt was not decoded");
  await ui.dispatch({ kind: "cancel_session" });
  await ui.dispatch({ kind: "resume_session", sessionId: api.sessionId("session-1") });

  const output = [];
  const terminalStatus = await terminalApi.runCliWithTransport(
    ["open session-1", "activity", "messages", "approvals", "changes", "diff README.md", "file README.md", "cancel"],
    transport,
    { output: { write(value) { output.push(String(value)); return true; } } },
  );
  assert(terminalStatus === 0, `installed terminal returned ${terminalStatus}`);
  const terminalLines = output.filter(line => line.trim() !== "").map(line => JSON.parse(line));
  assert(terminalLines.length === 8 && terminalLines.every(line => line.ok === true), "installed terminal emitted an unsuccessful result");

  const records = readFileSync(logPath, "utf8").trim().split(/\r?\n/u).filter(Boolean).map(line => JSON.parse(line));
  const startup = records.find(record => record.kind === "host_identity");
  assert(startup?.executable === process.execPath, "bridge executable identity was not recorded by the installed host");
  assert(startup?.argv?.[0] === resolve(hostPath), "bridge host script identity was not recorded by the installed host");
  const calls = records.filter(record => typeof record.method === "string");
  assert(calls[0]?.method === "list_sessions", "session listing did not remain the first lazy operation");
  assert(calls[0]?.params?.query?.limit === 32, "page limit was not preserved on the wire");
  assert(calls.slice(0, 2).map(call => call.method).join(",") === "list_sessions,open_session", "session listing performed hidden detail loads");
  const methods = new Set(calls.map(call => call.method));
  for (const method of ["list_sessions", "open_session", "read_activity", "read_messages", "list_approvals", "resolve_approval", "list_changes", "read_change", "read_file", "approve_writeback", "cancel_session", "resume_session"]) {
    assert(methods.has(method), `native fixture did not observe ${method}`);
  }
  const lazyObservation = assertLazyCounters(lazyObservationPath, { require: lazyObservationPath !== undefined });
  process.stdout.write(JSON.stringify({
    ok: true,
    scenario: "PKG-NATIVE-01",
    calls: calls.length,
    checks: [
      "installed-package-identity",
      "archive-to-install-manifest-mapping",
      "export-map-and-bin-targets",
      "bridge-executable-and-host-identity",
      "summary-only-listing",
      "session-history-pages",
      "snake-case-change-and-file-decoding",
      "authenticated-operator-approval",
      "writeback-receipt",
      "cancel-resume-lifecycle",
      "terminal-adapter",
    ],
    package: identity,
    lazy_observation: lazyObservation ?? null,
  }) + "\n");
} finally {
  processBridge.close();
}
