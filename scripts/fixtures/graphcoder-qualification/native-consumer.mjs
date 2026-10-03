import { createRequire } from "node:module";
import { createInterface } from "node:readline";
import { createHash } from "node:crypto";
import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

function option(name) {
  const index = process.argv.indexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function loadExport(consumerRoot, specifier) {
  const require = createRequire(resolve(consumerRoot, "package.json"));
  return import(pathToFileURL(require.resolve(specifier)).href);
}

async function expectNativeError(label, operation, code) {
  const response = await operation();
  assert(response?.ok === false, `${label}: request unexpectedly succeeded`);
  assert(response.error?.code === code, `${label}: expected ${code}, got ${response.error?.code ?? "missing code"}`);
  return response;
}

function assertResponseId(response, requestId, label) {
  assert(response?.request_id === requestId, `${label}: response request_id was not preserved`);
}

async function durableManifest(root) {
  const entries = [];
  async function visit(directory, prefix) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        await visit(path, relative);
      } else if (entry.isFile()) {
        const details = await stat(path);
        entries.push(`${relative}\t${details.size}\t${details.mtimeMs}`);
      }
    }
  }
  await visit(root, "");
  return entries.sort().join("\n");
}

function launchInstalled(nodeApi, executable, runtimeRoot, fixture, diagnostics) {
  let resolveExit;
  const exited = new Promise(resolveExitEvent => { resolveExit = resolveExitEvent; });
  const bridge = new nodeApi.JsonLineGraphCoderBridge({
    executable,
    args: ["--root", runtimeRoot, "--model-fixture", fixture],
    env: { PATH: process.env.PATH ?? "", SystemRoot: process.env.SystemRoot ?? "" },
    onDiagnostic(event) {
      diagnostics?.push(event);
      if (event.kind === "exit") resolveExit(event);
    },
  });
  return {
    bridge,
    async close() {
      bridge.close();
      await Promise.race([
        exited,
        new Promise((_, reject) => setTimeout(() => reject(new Error("installed native bridge did not exit within 2 seconds")), 2_000)),
      ]);
    },
  };
}

function launchRaw(executable, runtimeRoot, fixture = "echo") {
  const child = spawn(executable, ["--root", runtimeRoot, "--model-fixture", fixture], {
    env: { PATH: process.env.PATH ?? "", SystemRoot: process.env.SystemRoot ?? "" },
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true,
  });
  child.stderr.resume();
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();

  async function nextLine() {
    let timer;
    try {
      const result = await Promise.race([
        lines.next(),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("native runtime did not reply within 2 seconds")), 2_000); }),
      ]);
      if (result.done) throw new Error("native runtime closed before replying");
      return JSON.parse(result.value);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  async function send(request) {
    const line = `${JSON.stringify(request)}\n`;
    await new Promise((resolveWrite, rejectWrite) => child.stdin.write(line, error => error ? rejectWrite(error) : resolveWrite()));
    return nextLine();
  }

  async function sendRaw(line) {
    await new Promise((resolveWrite, rejectWrite) => child.stdin.write(`${line}\n`, error => error ? rejectWrite(error) : resolveWrite()));
    return nextLine();
  }

  async function close() {
    child.stdin.end();
    await new Promise(resolveExit => {
      const timer = setTimeout(() => { child.kill(); resolveExit(); }, 500);
      child.once("close", () => { clearTimeout(timer); resolveExit(); });
    });
  }

  return { send, sendRaw, close };
}

async function assertStageProjection(bridge, sessionId, generation, body, pending) {
  const response = await bridge.request({
    request_id: "native-stage-file",
    method: "read_file",
    params: { session_id: sessionId, path: "graphcoder-fixture.txt", generation },
  });
  if (!response.ok && response.error?.code === "unsupported") {
    pending.add("stage-file-projection");
    return;
  }
  assertResponseId(response, "native-stage-file", "PKG-NATIVE-STAGE-REOPEN-01 read_file");
  assert(response.ok === true && response.result && typeof response.result === "object", "PKG-NATIVE-STAGE-REOPEN-01 file body is missing");
  const file = response.result;
  assert(file.session_id === sessionId, "PKG-NATIVE-STAGE-REOPEN-01 file session identity changed");
  assert(file.path === "graphcoder-fixture.txt", "PKG-NATIVE-STAGE-REOPEN-01 staged logical path changed");
  assert(file.media_type === "text/plain", "PKG-NATIVE-STAGE-REOPEN-01 staged media type changed");
  assert(file.generation === generation, "PKG-NATIVE-STAGE-REOPEN-01 staged generation changed");
  assert(Array.isArray(file.bytes), "PKG-NATIVE-STAGE-REOPEN-01 staged body is not a byte array");
  assert(Buffer.from(file.bytes).equals(Buffer.from(body)), "PKG-NATIVE-STAGE-REOPEN-01 staged body changed");
  if (Object.hasOwn(file, "display_name")) {
    assert(file.display_name === "graphcoder-fixture.txt", "PKG-NATIVE-STAGE-REOPEN-01 staged display name changed");
  } else {
    pending.add("stage-display-name-projection");
  }
}

function assertStageOutcome(response, body, pending, label) {
  const attachments = response.result?.outcome?.attachments;
  if (!Array.isArray(attachments)) {
    pending.add("stage-outcome-projection");
    return;
  }
  assert(attachments.length === 1, `${label}: stage outcome attachment count changed`);
  const file = attachments[0]?.file;
  assert(file && typeof file === "object", `${label}: stage outcome file is missing`);
  assert(file.path === "graphcoder-fixture.txt", `${label}: staged logical path changed`);
  assert(file.display_name === "graphcoder-fixture.txt", `${label}: staged display name changed`);
  assert(file.descriptor?.media_type === "text/plain", `${label}: staged media type changed`);
  assert(file.descriptor?.byte_length === Buffer.byteLength(body), `${label}: staged byte length changed`);
  const expectedDigest = [...createHash("sha256").update(Buffer.from(body)).digest()];
  assert(JSON.stringify(file.descriptor?.sha256) === JSON.stringify(expectedDigest), `${label}: staged body digest changed`);
}

const consumerRoot = option("--root");
const executable = option("--executable");
if (consumerRoot === undefined || executable === undefined) {
  throw new Error("usage: node native-consumer.mjs --root <installed-consumer> --executable <graphcoder-runtime>");
}

const nodeApi = await loadExport(consumerRoot, "@acyclic-labs/graphcoder/node");
assert(typeof nodeApi.JsonLineGraphCoderBridge === "function", "PKG-NATIVE-CLI-01 installed node bridge export is missing");

const pending = new Set(["list-provider-observable"]);
const runtimeRoot = await mkdtemp(join(tmpdir(), "graphcoder-native-consumer-"));
try {
  const seedDiagnostics = [];
  const seed = launchInstalled(nodeApi, executable, runtimeRoot, "echo", seedDiagnostics);
  try {
    const seeded = await seed.bridge.request({ request_id: "native-seed", method: "list_sessions", params: {} });
    assertResponseId(seeded, "native-seed", "NEG-NATIVE-LIST-01 preseed");
    assert(seeded.ok === true && Array.isArray(seeded.result?.items), "NEG-NATIVE-LIST-01 preseed listing shape is invalid");
  } finally {
    await seed.close();
  }

  const diagnostics = [];
  const bridgeHandle = launchInstalled(nodeApi, executable, runtimeRoot, "echo", diagnostics);
  const bridge = bridgeHandle.bridge;
  try {
    const list = request => bridge.request(request);
    const beforeList = await durableManifest(runtimeRoot);
    const first = await list({ request_id: "native-list-1", method: "list_sessions", params: {} });
    assertResponseId(first, "native-list-1", "PKG-NATIVE-CLI-01 list");
    assert(first.ok === true && Array.isArray(first.result?.items), "PKG-NATIVE-CLI-01 listing shape is invalid");
    const afterFirstList = await durableManifest(runtimeRoot);
    const second = await list({ request_id: "native-list-2", method: "list_sessions", params: {} });
    assertResponseId(second, "native-list-2", "NEG-NATIVE-LIST-01 repeat");
    const afterSecondList = await durableManifest(runtimeRoot);
    assert(JSON.stringify(first.result) === JSON.stringify(second.result), "NEG-NATIVE-LIST-01 listing changed state or started hidden work");
    assert(beforeList === afterFirstList && afterFirstList === afterSecondList, "NEG-NATIVE-LIST-01 listing mutated the durable workspace descriptor");

    await expectNativeError("NEG-NATIVE-BOUNDS-01 zero limit", () => list({ request_id: "native-limit-0", method: "list_sessions", params: { query: { limit: 0 } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 high limit", () => list({ request_id: "native-limit-high", method: "list_sessions", params: { query: { limit: 1_025 } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 nontext cursor", () => list({ request_id: "native-cursor-type", method: "list_sessions", params: { query: { after: 7 } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 unknown cursor", () => list({ request_id: "native-cursor-missing", method: "list_sessions", params: { query: { after: "missing-session" } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 malformed params", () => list({ request_id: "native-params-array", method: "list_sessions", params: [] }), "invalid_input");

    await expectNativeError("NEG-NATIVE-METHOD-01 unknown method", () => list({ request_id: "native-unknown", method: "unknown_method", params: {} }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 unsupported method", () => list({ request_id: "native-unsupported", method: "read_activity", params: {} }), "unsupported");
    await expectNativeError("NEG-NATIVE-METHOD-01 malformed prompt", () => list({ request_id: "native-prompt-type", method: "start_session", params: { prompt: 7 } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 missing operation", () => list({ request_id: "native-operation-missing", method: "start_session", params: { prompt: "hello" } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 oversized operation", () => list({ request_id: "native-operation-high", method: "start_session", params: { prompt: "hello", operation_id: "x".repeat(257) } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 malformed session", () => list({ request_id: "native-session-type", method: "open_session", params: { session_id: 7 } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 empty request id", () => list({ request_id: "", method: "list_sessions", params: {} }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 oversized request id", () => list({ request_id: "x".repeat(257), method: "list_sessions", params: {} }), "invalid_input");
  } finally {
    await bridgeHandle.close();
  }

  const stageRoot = await mkdtemp(join(tmpdir(), "graphcoder-native-stage-"));
  const stageBody = "native stage fixture body";
  try {
    const stageDiagnostics = [];
    const stageHandle = launchInstalled(nodeApi, executable, stageRoot, "stage", stageDiagnostics);
    let stageSessionId;
    try {
      const started = await stageHandle.bridge.request({
        request_id: "native-stage-start",
        method: "start_session",
        params: { prompt: stageBody, operation_id: "native-stage-operation", model_fixture: "stage" },
      });
      assertResponseId(started, "native-stage-start", "PKG-NATIVE-STAGE-REOPEN-01 start");
      assert(started.ok === true && typeof started.result?.summary?.id === "string", "PKG-NATIVE-STAGE-REOPEN-01 start snapshot is invalid");
      assert(started.result.summary.state === "completed", "PKG-NATIVE-STAGE-REOPEN-01 stage session did not complete");
      assert(started.result.workspace_generation === "0", "PKG-NATIVE-STAGE-REOPEN-01 stage generation is unstable");
      stageSessionId = started.result.summary.id;
      assertStageOutcome(started, stageBody, pending, "PKG-NATIVE-STAGE-REOPEN-01 start");
      await assertStageProjection(stageHandle.bridge, stageSessionId, started.result.workspace_generation, stageBody, pending);
    } finally {
      await stageHandle.close();
    }

    const reopenedDiagnostics = [];
    const reopenedHandle = launchInstalled(nodeApi, executable, stageRoot, "stage", reopenedDiagnostics);
    try {
      const retried = await reopenedHandle.bridge.request({
        request_id: "native-stage-retry",
        method: "start_session",
        params: { prompt: stageBody, operation_id: "native-stage-operation", model_fixture: "stage" },
      });
      assertResponseId(retried, "native-stage-retry", "PKG-NATIVE-STAGE-REOPEN-01 retry");
      assert(retried.ok === true && retried.result?.summary?.id === stageSessionId, "PKG-NATIVE-STAGE-REOPEN-01 retry changed session identity");
      assertStageOutcome(retried, stageBody, pending, "PKG-NATIVE-STAGE-REOPEN-01 retry");
      const reopened = await reopenedHandle.bridge.request({
        request_id: "native-stage-reopen",
        method: "open_session",
        params: { session_id: stageSessionId },
      });
      assertResponseId(reopened, "native-stage-reopen", "PKG-NATIVE-STAGE-REOPEN-01 reopen");
      assert(reopened.ok === true && reopened.result?.summary?.id === stageSessionId, "PKG-NATIVE-STAGE-REOPEN-01 reopened a different session");
      assert(reopened.result.summary.state === "completed", "PKG-NATIVE-STAGE-REOPEN-01 reopened state was not completed");
      await assertStageProjection(reopenedHandle.bridge, stageSessionId, reopened.result.workspace_generation, stageBody, pending);
      const reopenedList = await reopenedHandle.bridge.request({ request_id: "native-stage-list", method: "list_sessions", params: {} });
      assertResponseId(reopenedList, "native-stage-list", "PKG-NATIVE-STAGE-REOPEN-01 reopened list");
      assert(reopenedList.ok === true && reopenedList.result.items.length === 1 && reopenedList.result.items[0].id === stageSessionId, "PKG-NATIVE-STAGE-REOPEN-01 reopened listing lost the session");
    } finally {
      await reopenedHandle.close();
    }
  } finally {
    await rm(stageRoot, { recursive: true, force: true });
  }

  const rawRoot = await mkdtemp(join(tmpdir(), "graphcoder-native-malformed-"));
  const raw = launchRaw(executable, rawRoot);
  try {
    const malformed = await raw.sendRaw(JSON.stringify({ request_id: "native-malformed", method: 7, params: {} }));
    assertResponseId(malformed, "native-malformed", "NEG-NATIVE-CORRELATION-01 malformed method");
    assert(malformed.ok === false && malformed.error?.code === "invalid_input", "NEG-NATIVE-CORRELATION-01 malformed method was not typed");
    const invalidJson = await raw.sendRaw("{not-json");
    assert(invalidJson.ok === false && invalidJson.error?.code === "invalid_input", "NEG-NATIVE-CORRELATION-01 invalid JSON did not fail closed");
    const missingId = await raw.sendRaw(JSON.stringify({ method: "list_sessions", params: {} }));
    assert(missingId.ok === false && missingId.error?.code === "invalid_input", "NEG-NATIVE-CORRELATION-01 missing request id did not fail closed");
    const oversized = await raw.sendRaw("x".repeat(16 * 1024 * 1024 + 1));
    assert(oversized.ok === false && oversized.error?.code === "invalid_input", "NEG-NATIVE-CORRELATION-01 oversized line did not fail closed");
  } finally {
    await raw.close();
    await rm(rawRoot, { recursive: true, force: true });
  }
} finally {
  await rm(runtimeRoot, { recursive: true, force: true });
}

process.stdout.write(JSON.stringify({
  ok: true,
  pending: [...pending],
  scenarios: ["PKG-NATIVE-CLI-01", "NEG-NATIVE-LIST-01", "NEG-NATIVE-BOUNDS-01", "NEG-NATIVE-METHOD-01", "PKG-NATIVE-STAGE-REOPEN-01", "NEG-NATIVE-CORRELATION-01"],
}) + "\n");
