// Capture published server output without manufacturing response fixtures.
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const recovery = "node capture-mcp.mjs /path/to/installed/server-package [stdio|http]";
const digest = bytes => createHash("sha256").update(bytes).digest("hex");
const initial = "06060606-0606-0606-0606-060606060606";
const operation = "07070707-0707-0707-0707-070707070707";
const protocol = "2025-11-25";
const line = value => Buffer.from(JSON.stringify(value) + "\n");
let step = "read published server identity";

function launch(args, ipc = false) {
  const child = spawn(process.execPath, args, {
    stdio: ["pipe", "pipe", "pipe", ...(ipc ? ["ipc"] : [])], windowsHide: true,
  });
  const closed = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  closed.catch(() => {});
  const stderr = [];
  child.stderr.on("data", bytes => stderr.push(bytes));
  return { child, closed, stderr };
}

function plan(everything, sampling = false) {
  const calls = [{ method: "tools/list", params: {} }];
  if (everything) calls.push(
    { method: "resources/list", params: {} },
    { method: "resources/templates/list", params: {} },
    { method: "resources/read", params: results => {
      const uri = results.get("resources/list")?.resources?.[0]?.uri;
      if (!uri) throw new Error("published server listed no readable resource");
      return { uri };
    } },
    { method: "prompts/list", params: {} },
    { method: "prompts/get", params: { name: "simple-prompt" } },
    { method: "tools/call", params: { name: "echo", arguments: { message: "codec capture" } } },
  );
  if (sampling) calls.push({ method: "tools/call", name: "sampling-refusal",
    params: { name: "trigger-sampling-request", arguments: { prompt: "codec capture", maxTokens: 1 } },
  });
  return calls;
}

function initialize(sampling) {
  return { jsonrpc: "2.0", id: initial, method: "initialize", params: {
    protocolVersion: protocol, capabilities: sampling ? { sampling: {} } : {},
    clientInfo: { name: "codec-capture", version: "1" },
  } };
}

async function stdio(server, everything) {
  const directory = await mkdtemp(join(tmpdir(), "mcp-capture-"));
  const { child, closed, stderr } = launch([
    join(server, "dist/index.js"), everything ? "stdio" : directory,
  ]);
  child.stdin.on("error", error => child.stdout.destroy(error));
  const requests = [], responses = [], lines = [], outputs = [], results = new Map();
  const calls = plan(everything, everything);
  let index = -1, callback = false, pending = Buffer.alloc(0);
  function send(value) { const bytes = line(value); requests.push(bytes); child.stdin.write(bytes); }
  function next() {
    index++;
    if (index === calls.length) { child.stdin.end(); return; }
    const call = calls[index];
    step = `capture published ${call.name ?? call.method} stdio output`;
    send({ jsonrpc: "2.0", id: operation, method: call.method,
      params: typeof call.params === "function" ? call.params(results) : call.params });
  }
  try {
    step = "initialize published MCP server";
    send(initialize(everything));
    for await (const bytes of child.stdout) {
      pending = Buffer.concat([pending, bytes]);
      for (let end; (end = pending.indexOf(10)) >= 0;) {
        const raw = pending.subarray(0, end + 1);
        pending = pending.subarray(end + 1);
        lines.push(raw);
        const value = JSON.parse(raw.toString("utf8"));
        if (value.jsonrpc !== "2.0") throw new Error("server emitted another JSON-RPC version");
        if (value.method && value.id === undefined) continue;
        if (value.method) {
          if (calls[index]?.name !== "sampling-refusal" || callback || value.method !== "sampling/createMessage") {
            throw new Error("unexpected server callback");
          }
          callback = true;
          outputs.push(["sampling-request", raw]);
          // Observe the real callback, then refuse it. No model response is fabricated.
          send({ jsonrpc: "2.0", id: value.id, error: { code: -32601, message: "Capture client does not execute sampling" } });
          continue;
        }
        if (index === -1) {
          if (value.id !== initial || value.result?.protocolVersion !== protocol) throw new Error("initialization refused");
          responses.push(raw);
          send({ jsonrpc: "2.0", method: "notifications/initialized" });
        } else {
          const call = calls[index];
          if (value.id !== operation || (!value.result && !call.name)) throw new Error("service response refused or mismatched");
          if (call.name && !callback) throw new Error("sampling request was not observed");
          if (!call.name && value.result.isError) throw new Error("published tool refused capture input");
          responses.push(raw);
          results.set(call.method, value.result);
          outputs.push([everything ? (call.name ?? call.method.replaceAll("/", "-")) : "result", raw]);
          if (index === 0) outputs.unshift(["stdio", Buffer.concat(lines)]);
        }
        next();
      }
    }
    step = "observe published stdio server EOF";
    const result = await closed;
    if (result.code !== 0 || pending.length || index !== calls.length || responses.length !== calls.length + 1) {
      throw new Error(`incomplete capture: exit=${result.code}, signal=${result.signal}, responses=${responses.length}; ${Buffer.concat(stderr)}`);
    }
    return { outputs, requests: Buffer.concat(requests), transcript: Buffer.concat(lines),
      response_sha256: responses.map(digest), tools: results.get("tools/list").tools.length,
      services: calls.map(call => call.method), sampling: everything ? "server request captured; client refused without model execution" : "disabled" };
  } finally {
    if (child.exitCode === null) child.kill();
    await closed;
    if (resolve(dirname(directory)) !== resolve(tmpdir())) throw new Error("temporary directory escaped its allocated parent");
    await rm(directory, { recursive: true });
  }
}

async function http(server) {
  // The installed production app supplies all protocol and transport behavior.
  // Listening and shutdown are event driven; the child owns both producers.
  const bootstrap = `
    import { pathToFileURL } from "node:url";
    const { createApp } = await import(pathToFileURL(process.argv[1]).href);
    const { app, shutdown } = createApp();
    const listener = app.listen(0, "127.0.0.1");
    listener.once("listening", () => process.send({ port: listener.address().port }));
    listener.once("error", error => { console.error(error); process.exit(1); });
    process.once("message", message => { if (message === "stop") void shutdown(); });
    process.once("disconnect", () => void shutdown());
  `;
  const { child, closed, stderr } = launch(["--input-type=module", "-e", bootstrap,
    join(server, "dist/transports/streamableHttp.js")], true);
  child.stdout.resume();
  const requests = [], outputs = [], exchanges = [], results = new Map();
  let session;
  try {
    step = "wait for published HTTP app listening event";
    const ready = new Promise(resolve => child.once("message", resolve));
    const { port } = await Promise.race([ready, closed.then(result => {
      throw new Error(`server exited before listening: ${result.code}; ${Buffer.concat(stderr)}`);
    })]);
    if (!Number.isInteger(port) || port <= 0) throw new Error("invalid listening event");
    const endpoint = `http://127.0.0.1:${port}/mcp`;
    async function post(message, name) {
      step = `capture published ${name} HTTP response`;
      const bytes = line(message);
      requests.push(bytes);
      const response = await fetch(endpoint, { method: "POST", body: bytes, headers: {
        "content-type": "application/json", accept: "application/json, text/event-stream",
        ...(session ? { "mcp-session-id": session, "mcp-protocol-version": protocol } : {}),
      } });
      session ??= response.headers.get("mcp-session-id");
      const body = Buffer.from(await response.arrayBuffer());
      exchanges.push({ method: message.method, request_sha256: digest(bytes), status: response.status,
        headers: Object.fromEntries(response.headers), response_sha256: digest(body), bytes: body.length });
      if (message.id === undefined) {
        if (response.status !== 202 || body.length) throw new Error("initialization notification refused");
        return;
      }
      if (response.status !== 200 || !response.headers.get("content-type")?.startsWith("text/event-stream")) {
        throw new Error(`expected actual SSE response, received ${response.status}`);
      }
      const events = body.toString("utf8").split(/\r?\n\r?\n/).map(event => event
        .split(/\r?\n/).filter(row => row.startsWith("data:")).map(row => row.slice(5).replace(/^ /, "")).join("\n"))
        .filter(data => data.trim()).map(data => JSON.parse(data));
      const value = events.findLast(event => event.id === message.id);
      if (!value?.result || value.error || value.result.isError) throw new Error("incomplete or refused HTTP result");
      outputs.push([`http-${name}`, body]);
      return value.result;
    }
    const initialized = await post(initialize(false), "initialize");
    if (initialized.protocolVersion !== protocol || !session) throw new Error("HTTP protocol negotiation refused");
    await post({ jsonrpc: "2.0", method: "notifications/initialized" }, "initialized");
    const calls = plan(true);
    for (const call of calls) {
      const result = await post({ jsonrpc: "2.0", id: operation, method: call.method,
        params: typeof call.params === "function" ? call.params(results) : call.params }, call.method.replaceAll("/", "-"));
      results.set(call.method, result);
    }
    return { outputs, requests: Buffer.concat(requests), exchanges, services: calls.map(call => call.method),
      tools: results.get("tools/list").tools.length, sampling: "disabled" };
  } finally {
    if (child.connected) child.send("stop");
    else if (child.exitCode === null) child.kill();
    const result = await closed;
    if (result.code !== 0) throw new Error(`retire HTTP producer failed: exit=${result.code}, signal=${result.signal}; ${Buffer.concat(stderr)}`);
  }
}

async function capture() {
  const source = process.argv[2], transport = process.argv[3] ?? "stdio";
  if (!source) throw new Error("provide the installed server package directory");
  const server = resolve(source);
  const identity = JSON.parse(await readFile(join(server, "package.json"), "utf8"));
  const everything = identity.name === "@modelcontextprotocol/server-everything";
  if ((!everything && identity.name !== "@modelcontextprotocol/server-filesystem") || identity.version !== "2.1.0") {
    throw new Error("capture requires published server-filesystem@2.1.0 or server-everything@2.1.0");
  }
  if (!["stdio", "http"].includes(transport) || (transport === "http" && !everything)) throw new Error("unsupported capture transport");
  const installed = JSON.parse(await readFile(join(server, "../../.package-lock.json"), "utf8"));
  const packages = Object.fromEntries(Object.entries(installed.packages).map(([name, value]) => [
    name.replace(/^node_modules\//, ""), { version: value.version, integrity: value.integrity },
  ]));
  const captured = transport === "stdio" ? await stdio(server, everything) : await http(server);
  step = "save byte-exact external output";
  const stem = `mcp-${everything ? "everything" : "filesystem"}-2.1.0`;
  const group = transport === "http" ? `${stem}-http` : stem;
  const manifestPath = join(root, "corpus/EXTERNAL.tsv");
  let previous;
  try { previous = await readFile(manifestPath, "utf8"); }
  catch (error) { if (error.code !== "ENOENT") throw error; previous = "# Captured external server output\n# path\tbytes\tsha256\tproducer\n"; }
  const rows = previous.trimEnd().split("\n").filter(row => {
    const name = row.split("\t")[0];
    return !(name.startsWith(`harness_codecs/${group}-`) && (transport === "http" || !name.startsWith(`harness_codecs/${stem}-http-`)));
  });
  await mkdir(join(root, "corpus/harness_codecs"), { recursive: true });
  const outputs = [];
  for (const [suffix, bytes] of captured.outputs) {
    const name = `${stem}-${suffix}`;
    await writeFile(join(root, "corpus/harness_codecs", name), bytes);
    rows.push(`harness_codecs/${name}\t${bytes.length}\t${digest(bytes)}\t${identity.name}@${identity.version} ${transport}; capture-mcp.mjs`);
    const codec = suffix === "stdio" ? "stdio" : suffix === "sampling-request" ? "callback" : suffix.startsWith("http-") ? "sse" : "json";
    const method = suffix === "stdio" || suffix === "result" ? "tools/list"
      : suffix === "sampling-request" ? "sampling/createMessage"
      : suffix === "sampling-refusal" ? "tools/call" : suffix.replace(/^http-/, "").replaceAll("-", "/");
    outputs.push({ path: `harness_codecs/${name}`, bytes: bytes.length, sha256: digest(bytes), codec, method });
  }
  await writeFile(manifestPath, rows.join("\n") + "\n");
  await mkdir(join(root, "captures"), { recursive: true });
  await writeFile(join(root, `captures/${group}.requests.jsonl`), captured.requests);
  if (captured.transcript) await writeFile(join(root, `captures/${group}.stdout.jsonl`), captured.transcript);
  await writeFile(join(root, `captures/${group}.json`), JSON.stringify({
    package: { name: identity.name, version: identity.version }, node: process.version, packages, protocol, transport,
    requests_sha256: digest(captured.requests), stdout_sha256: captured.transcript ? digest(captured.transcript) : undefined,
    response_sha256: captured.response_sha256, tools: captured.tools, services: captured.services,
    sampling: captured.sampling, exchanges: captured.exchanges, outputs,
  }, null, 2) + "\n");
  console.log(`Captured ${outputs.length} inputs from ${identity.name}@${identity.version} over ${transport}; review before admitting golden bytes.`);
}

capture().catch(error => {
  console.error(`MCP capture failed at ${step}: ${error.message}; recovery: ${recovery}`);
  process.exitCode = 1;
});
