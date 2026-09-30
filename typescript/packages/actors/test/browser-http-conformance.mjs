// Real Chrome HTTPS qualification. Trust is limited to this fixture's ephemeral
// public key in a private browser profile; customer transport security is unchanged.
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, X509Certificate } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:https";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { extname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { create, fromBinary, fromJsonString, toBinary, toJsonString } from "@bufbuild/protobuf";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../../workers/generated/proto/workers/v1/workers_pb.js";
import { HTTP_ROUTES as actorRoutes } from "../dist/routes.js";
import { HTTP_ROUTES as workerRoutes } from "../../workers/dist/routes.js";
import * as objectsWire from "../../objects/generated/proto/objects/v2/objects_pb.js";
import { MemoryObjectsV2 } from "../../objects/dist/v2.js";
import { ObjectsV2Memory, objects_v2_http_type, decode_objects_v2_json, encode_objects_v2_json } from "../../objects/generated/wasm/acyclic_objects_wasm.js";
import { MemoryStreamProvider } from "../../stream/dist/memory.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const require = createRequire(import.meta.url);
const protobufRoot = resolve(require.resolve("@bufbuild/protobuf"), "../../..");
const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
assert.equal(generated.status, 0, generated.stderr);
const identity = JSON.parse(generated.stdout);
const spki = new X509Certificate(identity.certificate).publicKey.export({ type: "spki", format: "der" });
const trust = createHash("sha256").update(spki).digest("base64");
await MemoryObjectsV2.create();
const objects = new ObjectsV2Memory(64n * 1024n * 1024n, 10000);
const stream = new MemoryStreamProvider();
const seen = new Set();
const byteValue = value => new Uint8Array(Buffer.from(value, "base64"));
function revive(name, value) {
  if (value === null) return value;
  if (["from", "ifTail", "atTail", "deadlineUnixMillis"].includes(name)) return BigInt(value);
  if (["idempotencyKey", "commitId", "hierarchyVersion"].includes(name)) return byteValue(value);
  if (name === "values") return value.map(byteValue);
  if (Array.isArray(value)) return value.map(item => revive("", item));
  if (typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, revive(key, item)]));
  return value;
}
const jsonReplacer = (_name, value) => typeof value === "bigint" ? value.toString() : value instanceof Uint8Array ? Buffer.from(value).toString("base64") : value;
async function streamDispatch(route, input) {
  switch (route) {
    case "tail": return stream.tail(input.path);
    case "append": return stream.append(input.path, input.values, input.options);
    case "fork": return stream.fork(input.source, input.destination, input.options);
    case "read": { const records = []; for await (const record of stream.read(input.path, { from: input.from, limit: input.limit })) records.push(record); return records; }
    case "children": { const children = []; for await (const child of stream.children(input.parent, input.limit)) children.push(child); return children; }
    case "children/page": return stream.childrenPage(input);
    case "commit": return stream.commit(input.request, input.options);
    case "commits/read": return stream.readCommit(input.commitId);
    case "idempotency/inspect": return (await stream.inspectIdempotency(input.idempotencyKey)) ?? null;
    case "tokens/create":
      assert.equal(input.allow[0].path, "browser");
      return { token: "fixture-issued-token", expiresAt: new Date(Date.now() + 60000).toISOString() };
    default: throw new Error(`unknown Stream route ${route}`);
  }
}
function actorWorkerResponse(method, input) {
  if (method.name === "AddSubscription") {
    assert.equal(input.subscription.streamPath, "events/input");
    assert.equal(input.subscription.start.start.value, 9007199254740993n);
  }
  if (method.name === "CheckpointActor") { assert.equal(input.actorId, "browser-actor"); assert.equal(input.idempotencyKey, "checkpoint-browser"); return { actor: { actorId: input.actorId, checkpointEpoch: 9n } }; }
  if (method.name === "InvokeActor") return { status: 201, body: new Uint8Array([5]) };
  if (method.name === "InvokeVersion") { assert.deepEqual(input.versionSha256, new Uint8Array(32).fill(1)); return { resolvedSha256: input.versionSha256 }; }
  if (method.name === "InvokeDeployment") { assert.equal(input.alias, "current"); return { resolvedSha256: new Uint8Array(32).fill(2), resolvedRevision: 8n }; }
  if (method.name === "SelectDeployment") assert.equal(input.expectedRevision, 7n);
  return {};
}
const server = createServer({ key: identity.key, cert: identity.certificate }, async (request, response) => {
  const pathname = new URL(request.url, "https://localhost").pathname;
  try {
    if (request.method === "GET") {
      const dependency = pathname.startsWith("/deps/protobuf/");
      const base = dependency ? protobufRoot : resolve(root);
      const relative = dependency ? pathname.slice("/deps/protobuf/".length) : pathname.slice(1);
      const path = resolve(base, decodeURIComponent(relative));
      if (!path.startsWith(`${base}${sep}`) || !existsSync(path)) { response.writeHead(404).end(); return; }
      response.writeHead(200, { "content-type": ({ ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm" })[extname(path)] ?? "application/octet-stream" });
      response.end(readFileSync(path)); return;
    }
    const isObjects = pathname.startsWith("/v2/objects/");
    const isStream = pathname.startsWith("/v1/stream/");
    if (request.headers.authorization !== "Bearer conformance") {
      response.writeHead(403, { "content-type": "application/json" });
      response.end(isObjects ? '{"code":"ERROR_CODE_ACCESS_DENIED"}' : isStream ? '{"code":"access_denied"}' : '{"code":"ERROR_CODE_CAPABILITY_DENIED"}'); return;
    }
    const chunks = []; let total = 0;
    for await (const chunk of request) { total += chunk.length; assert.ok(total <= 16 * 1024 * 1024); chunks.push(chunk); }
    const data = Buffer.concat(chunks);
    if (isObjects) {
      const route = pathname.slice("/v2/objects/".length);
      const input = objects_v2_http_type(route, false); const output = objects_v2_http_type(route, true);
      let query; let body = new Uint8Array(0);
      if (route === "objects/put" || route === "multipart/upload-part") {
        assert.equal(request.headers["content-type"], "application/x-ndjson"); assert.equal(data.at(-1), 10);
        const put = route === "objects/put";
        const schema = put ? objectsWire.PutObjectRequestSchema : objectsWire.UploadPartRequestSchema;
        const headerSchema = put ? objectsWire.PutObjectHeaderSchema : objectsWire.UploadPartHeaderSchema;
        const frames = data.toString("utf8").trimEnd().split("\n").map(line => fromBinary(schema, decode_objects_v2_json(input, Buffer.from(line), 128 * 1024)));
        const first = frames.shift(); assert.equal(first.frame.case, "header"); query = toBinary(headerSchema, first.frame.value);
        assert.deepEqual(frames.pop().frame, { case: "complete", value: true });
        body = Buffer.concat(frames.map(({ frame }) => { assert.equal(frame.case, "body"); assert.ok(frame.value.length <= 65536); return frame.value; }));
      } else query = decode_objects_v2_json(input, data, 16 * 1024 * 1024);
      seen.add(`objects/${route}`);
      const result = await objects.invoke(route, query, body, 64n * 1024n * 1024n);
      const encoded = result.map(frame => Buffer.from(encode_objects_v2_json(output, frame, 16 * 1024 * 1024)));
      response.writeHead(200, { "content-type": route === "objects/get" ? "application/x-ndjson" : "application/json" });
      const payload = route === "objects/get" ? Buffer.concat(encoded.flatMap(frame => [frame, Buffer.from("\n")])) : encoded[0];
      for (let offset = 0; offset < payload.length; offset += 97) response.write(payload.subarray(offset, offset + 97));
      response.end(); return;
    }
    if (isStream) {
      const route = pathname.slice("/v1/stream/".length); seen.add(`stream/${route}`);
      response.setHeader("content-type", "application/json");
      const result = await streamDispatch(route, revive("", JSON.parse(data.toString("utf8"))));
      response.end(JSON.stringify(result, jsonReplacer)); return;
    }
    for (const [service, routes] of [[ActorsService, actorRoutes], [WorkersService, workerRoutes]]) for (const method of service.methods) {
      const route = "/" + routes[method.localName].replace("{sha256hex}", "01".repeat(32)).replace("{alias}", "current");
      if (route !== pathname) continue;
      const input = fromJsonString(method.input, data.toString("utf8"));
      if (input.actorId === "oversize" || input.jobId === "oversize") { response.writeHead(200, { "content-type": "application/json" }).end(" ".repeat(64)); return; }
      seen.add(`${service.typeName}/${method.name}`);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(toJsonString(method.output, create(method.output, actorWorkerResponse(method, input)))); return;
    }
    response.writeHead(404).end();
  } catch (error) {
    response.writeHead(400, { "content-type": "application/json" });
    if (pathname.startsWith("/v2/objects/")) response.end(encode_objects_v2_json("ErrorDetail", toBinary(objectsWire.ErrorDetailSchema, create(objectsWire.ErrorDetailSchema, { code: typeof error.code === "number" ? error.code : objectsWire.ErrorCode.UNAVAILABLE })), 1024));
    else response.end(JSON.stringify({ code: error.code ?? "unavailable" }));
  }
});
await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
const profile = mkdtempSync(join(tmpdir(), "acyclic-sdk-http-browser-"));
const chromePath = [process.env.CHROME, "C:/Program Files/Google/Chrome/Application/chrome.exe", "/usr/bin/google-chrome", "/usr/bin/chromium", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"].find(path => path && existsSync(path));
let chrome; let socket;
async function until(description, probe, duration = 30000) {
  const end = Date.now() + duration;
  while (Date.now() < end) { const result = await probe(); if (result !== undefined) return result; await new Promise(resolveDelay => setTimeout(resolveDelay, 50)); }
  throw new Error(`timed out waiting for ${description}`);
}
try {
  assert.ok(chromePath, "set CHROME to a Chrome/Chromium executable");
  chrome = spawn(chromePath, ["--headless=new", "--remote-debugging-port=0", `--user-data-dir=${profile}`, `--ignore-certificate-errors-spki-list=${trust}`, "--no-first-run", "--no-default-browser-check", "about:blank"], { stdio: "ignore" });
  const exit = new Promise(resolveExit => chrome.once("exit", resolveExit));
  const endpoint = await until("Chrome DevTools", () => {
    if (chrome.exitCode !== null) throw new Error(`Chrome exited ${chrome.exitCode}`);
    const file = join(profile, "DevToolsActivePort"); if (!existsSync(file)) return;
    const [port, path] = readFileSync(file, "utf8").split("\n"); if (path) return `ws://127.0.0.1:${port}${path.trim()}`;
  });
  socket = new WebSocket(endpoint);
  await new Promise((resolveOpen, reject) => { socket.addEventListener("open", resolveOpen, { once: true }); socket.addEventListener("error", reject, { once: true }); });
  let sequence = 0; const pending = new Map(); const errors = [];
  socket.addEventListener("message", event => {
    const message = JSON.parse(event.data);
    if (message.id !== undefined) { const item = pending.get(message.id); pending.delete(message.id); if (message.error) item.reject(new Error(message.error.message)); else item.resolve(message.result); }
    if (message.method === "Runtime.exceptionThrown") errors.push(message.params.exceptionDetails.exception?.description ?? message.params.exceptionDetails.text);
  });
  const send = (method, params = {}, sessionId) => new Promise((resolveSend, reject) => { const id = ++sequence; pending.set(id, { resolve: resolveSend, reject }); socket.send(JSON.stringify({ id, method, params, sessionId })); });
  const { targetId } = await send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
  await send("Runtime.enable", {}, sessionId);
  await send("Page.navigate", { url: `https://localhost:${server.address().port}/typescript/packages/actors/test/browser-http.html` }, sessionId);
  const result = await until("browser HTTP conformance", async () => {
    if (errors.length) throw new Error(errors.join("\n"));
    const value = await send("Runtime.evaluate", { expression: "globalThis.sdkHttpResult", returnByValue: true }, sessionId);
    return value.result.value;
  }, 120000);
  assert.equal(result.status, "passed", result.detail);
  assert.equal(seen.size, 38, `expected 13 Objects, 15 Actor/Worker and 10 Stream HTTP routes: ${[...seen]}`);
  console.log(`Chrome HTTPS: ${result.detail}; ${seen.size} fixture routes observed`);
  await send("Browser.close"); await exit;
} finally {
  socket?.close();
  if (chrome && chrome.exitCode === null) { const exit = new Promise(resolveExit => chrome.once("exit", resolveExit)); chrome.kill(); await exit; }
  server.closeAllConnections(); await new Promise(resolveClose => server.close(resolveClose));
  assert.ok(resolve(profile).startsWith(`${resolve(tmpdir())}${sep}`), "browser profile must remain in the task's temporary directory");
  rmSync(profile, { recursive: true, force: true, maxRetries: 50, retryDelay: 100 });
}
