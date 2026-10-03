import { createRequire } from "node:module";
import { createInterface } from "node:readline";
import { mkdtemp, rm } from "node:fs/promises";
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

function launchRaw(executable, runtimeRoot) {
  const child = spawn(executable, ["--root", runtimeRoot, "--model-fixture", "echo"], {
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

const consumerRoot = option("--root");
const executable = option("--executable");
if (consumerRoot === undefined || executable === undefined) {
  throw new Error("usage: node native-consumer.mjs --root <installed-consumer> --executable <graphcoder-runtime>");
}

const nodeApi = await loadExport(consumerRoot, "@acyclic-labs/graphcoder/node");
assert(typeof nodeApi.JsonLineGraphCoderBridge === "function", "PKG-NATIVE-CLI-01 installed node bridge export is missing");

const runtimeRoot = await mkdtemp(join(tmpdir(), "graphcoder-native-consumer-"));
try {
  const bridge = new nodeApi.JsonLineGraphCoderBridge({
    executable,
    args: ["--root", runtimeRoot, "--model-fixture", "echo"],
    env: { PATH: process.env.PATH ?? "", SystemRoot: process.env.SystemRoot ?? "" },
  });
  try {
    const list = request => bridge.request(request);
    const first = await list({ request_id: "native-list-1", method: "list_sessions", params: {} });
    assertResponseId(first, "native-list-1", "PKG-NATIVE-CLI-01 list");
    assert(first.ok === true && Array.isArray(first.result?.items), "PKG-NATIVE-CLI-01 listing shape is invalid");
    const second = await list({ request_id: "native-list-2", method: "list_sessions", params: {} });
    assertResponseId(second, "native-list-2", "NEG-NATIVE-LIST-01 repeat");
    assert(JSON.stringify(first.result) === JSON.stringify(second.result), "NEG-NATIVE-LIST-01 listing changed state or started hidden work");

    await expectNativeError("NEG-NATIVE-BOUNDS-01 zero limit", () => list({ request_id: "native-limit-0", method: "list_sessions", params: { query: { limit: 0 } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 high limit", () => list({ request_id: "native-limit-high", method: "list_sessions", params: { query: { limit: 1_025 } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 nontext cursor", () => list({ request_id: "native-cursor-type", method: "list_sessions", params: { query: { after: 7 } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 unknown cursor", () => list({ request_id: "native-cursor-missing", method: "list_sessions", params: { query: { after: "missing-session" } } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-BOUNDS-01 malformed params", () => list({ request_id: "native-params-array", method: "list_sessions", params: [] }), "invalid_input");

    await expectNativeError("NEG-NATIVE-METHOD-01 unknown method", () => list({ request_id: "native-unknown", method: "unknown_method", params: {} }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 unsupported method", () => list({ request_id: "native-unsupported", method: "read_activity", params: {} }), "unsupported");
    await expectNativeError("NEG-NATIVE-METHOD-01 malformed prompt", () => list({ request_id: "native-prompt-type", method: "start_session", params: { prompt: 7 } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 malformed session", () => list({ request_id: "native-session-type", method: "open_session", params: { session_id: 7 } }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 empty request id", () => list({ request_id: "", method: "list_sessions", params: {} }), "invalid_input");
    await expectNativeError("NEG-NATIVE-METHOD-01 oversized request id", () => list({ request_id: "x".repeat(257), method: "list_sessions", params: {} }), "invalid_input");
  } finally {
    bridge.close();
  }

  const rawRoot = await mkdtemp(join(tmpdir(), "graphcoder-native-malformed-"));
  const raw = launchRaw(executable, rawRoot);
  try {
    const malformed = await raw.sendRaw(JSON.stringify({ request_id: "native-malformed", method: 7, params: {} }));
    assertResponseId(malformed, "native-malformed", "NEG-NATIVE-CORRELATION-01 malformed method");
    assert(malformed.ok === false && malformed.error?.code === "invalid_input", "NEG-NATIVE-CORRELATION-01 malformed method was not typed");
  } finally {
    await raw.close();
    await rm(rawRoot, { recursive: true, force: true });
  }
} finally {
  await rm(runtimeRoot, { recursive: true, force: true });
}

process.stdout.write(JSON.stringify({
  ok: true,
  scenarios: ["PKG-NATIVE-CLI-01", "NEG-NATIVE-LIST-01", "NEG-NATIVE-BOUNDS-01", "NEG-NATIVE-METHOD-01", "NEG-NATIVE-CORRELATION-01"],
}) + "\n");
