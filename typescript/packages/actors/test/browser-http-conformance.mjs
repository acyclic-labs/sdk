// Real Chrome HTTPS qualification. Trust is limited to this fixture's ephemeral
// public key in a private browser profile; customer transport security is unchanged.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, X509Certificate } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:https";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { extname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnDriver, chromeExecutable, connectBrowser, navigateChrome } from "../../sdk/test/browser-driver.mjs";
import { create, fromBinary, fromJsonString, toBinary, toJsonString } from "@bufbuild/protobuf";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import * as objectsWire from "../../objects/generated/proto/objects/v1/objects_pb.js";
import { MemoryObjectsV1 } from "../../objects/dist/v1.js";
import { ObjectsV1Memory, objects_v1_http_type, decode_objects_v1_json, encode_objects_v1_json } from "../../objects/generated/wasm/acyclic_objects_wasm.js";
import { MemoryStreamProvider } from "../../stream/dist/memory.js";
import { StreamClient } from "../../stream/dist/client.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const require = createRequire(import.meta.url);
function rustRoutes(packageName, example) {
  const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", packageName, "--example", example], { cwd: root, encoding: "utf8" });
  assert.equal(generated.status, 0, generated.stderr);
  return Object.fromEntries(JSON.parse(generated.stdout));
}
const actorRoutes = rustRoutes("acyclic-actors", "actors-http-routes");
const protobufRoot = resolve(require.resolve("@bufbuild/protobuf"), "../../..");
const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
assert.equal(generated.status, 0, generated.stderr);
const identity = JSON.parse(generated.stdout);
const spki = new X509Certificate(identity.certificate).publicKey.export({ type: "spki", format: "der" });
const trust = createHash("sha256").update(spki).digest("base64");
await MemoryObjectsV1.create();
const objects = new ObjectsV1Memory();
const stream = new MemoryStreamProvider();
const seen = new Set();
let cancellationDispatched = false;
let cancellationEntered;
const entered = new Promise(resolve => { cancellationEntered = resolve; });
let observeCancellationClose;
const closed = new Promise(resolve => { observeCancellationClose = resolve; });
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
function actorResponse(method, input) {
  if (method.name === "AddSubscription") {
    assert.equal(input.subscription.streamPath, "events/input");
    assert.equal(input.subscription.start.start.value, 9007199254740993n);
  }
  if (["DeployNamespace", "DeployWorker", "UpdateNamespace"].includes(method.name)) return { deployment: {name: "example", codeSha256: new Uint8Array(32).fill(1), revision: 1n} };
  if (method.name === "CheckpointActor") { assert.equal(input.actorId, "browser-actor"); assert.equal(input.idempotencyKey, "checkpoint-browser"); return { actor: { actorId: input.actorId, codeSha256: new Uint8Array(32).fill(1), homeRegion: "eu", state: 1, checkpointEpoch: 9n, configurationRevision: 1n } }; }
  if (method.name === "DeleteAll") { assert.equal(input.actorId, "browser-actor"); assert.equal(input.idempotencyKey, "delete-browser"); return {}; }
  if (["InvokeActor", "InvokeWorker"].includes(method.name)) return { status: 201, body: new Uint8Array([5]) };

  if (["InspectActor", "AddSubscription", "RemoveSubscription", "ResumeSubscription"].includes(method.name)) return { actor: { actorId: input.actorId ?? "browser-actor", codeSha256: new Uint8Array(32).fill(1), homeRegion: "eu", state: 1, checkpointEpoch: 9n, configurationRevision: 1n } };
  return {};
}
function grpcWebFrame(flags, payload) {
  const frame = Buffer.alloc(payload.length + 5);
  frame[0] = flags;
  frame.writeUInt32BE(payload.length, 1);
  Buffer.from(payload).copy(frame, 5);
  return frame;
}
function grpcWebResponse(payload, status = 0, message = "") {
  const trailers = Buffer.from(`grpc-status: ${status}\r\ngrpc-message: ${encodeURIComponent(message)}\r\n`);
  return Buffer.concat([grpcWebFrame(0, payload), grpcWebFrame(0x80, trailers)]);
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
    const isObjects = pathname.startsWith("/v1/objects/");
    const isStream = pathname.startsWith("/v1/stream/");
    const grpcWebService = request.headers["content-type"]?.startsWith("application/grpc-web")
      ? [ActorsService].find(service => pathname.startsWith(`/${service.typeName}/`) || service.methods.some(method => pathname === `/${method.name}`))
      : undefined;
    if (request.headers.authorization !== "Bearer conformance") {
      if (grpcWebService) {
        response.writeHead(200, { "content-type": "application/grpc-web+proto", "access-control-expose-headers": "grpc-status,grpc-message" });
        response.end(grpcWebResponse(new Uint8Array(), 16, "capability denied")); return;
      }
      response.writeHead(403, { "content-type": "application/json" });
      response.end(isObjects ? '{"code":"ERROR_CODE_ACCESS_DENIED"}' : isStream ? '{"code":"access_denied"}' : '{"code":"ERROR_CODE_CAPABILITY_DENIED"}'); return;
    }
    const chunks = []; let total = 0;
    for await (const chunk of request) { total += chunk.length; assert.ok(total <= 16 * 1024 * 1024); chunks.push(chunk); }
    const data = Buffer.concat(chunks);
    if (grpcWebService) {
      const prefix = `/${grpcWebService.typeName}/`;
      const methodName = pathname.startsWith(prefix) ? pathname.slice(prefix.length) : pathname.slice(1);
      const method = grpcWebService.methods.find(candidate => candidate.name === methodName);
      assert.ok(method, `unknown gRPC-Web route ${methodName}`);
      assert.ok(data.length >= 5);
      const frameLength = data.readUInt32BE(1);
      assert.equal(data[0] & 0x80, 0);
      assert.equal(frameLength, data.length - 5);
      const input = fromBinary(method.input, data.subarray(5));
      if (input.jobId === "oversize") {
        response.writeHead(200, { "content-type": "application/grpc-web+proto" });
        response.end(grpcWebResponse(new Uint8Array(128))); return;
      }
      if (input.actorId === "cancel") {
        assert.equal(cancellationDispatched, false, "one cancellation request must dispatch");
        cancellationDispatched = true;
        const finished = new Promise(resolveClose => response.once("close", resolveClose));
        cancellationEntered();
        await finished;
        observeCancellationClose(!response.writableEnded);
        return;
      }
      seen.add(`${grpcWebService.typeName}/${method.name}`);
      const body = toBinary(method.output, create(method.output, actorResponse(method, input)));
      response.writeHead(200, { "content-type": "application/grpc-web+proto", "access-control-expose-headers": "grpc-status,grpc-message" });
      response.end(grpcWebResponse(body)); return;
    }
    if (isObjects) {
      const route = pathname.slice("/v1/objects/".length);
      const input = objects_v1_http_type(route, false); const output = objects_v1_http_type(route, true);
      seen.add(`objects/${route}`);
      const result = await objects.invoke(route, decode_objects_v1_json(input, data, 1 << 30));
      const payload = Buffer.from(encode_objects_v1_json(output, result, 1 << 30));
      response.writeHead(200, { "content-type": "application/json" });
      for (let offset = 0; offset < payload.length; offset += 97) response.write(payload.subarray(offset, offset + 97));
      response.end(); return;
    }
    if (isStream) {
      const route = pathname.slice("/v1/stream/".length); seen.add(`stream/${route}`);
      response.setHeader("content-type", "application/json");
      const result = await streamDispatch(route, revive("", JSON.parse(data.toString("utf8"))));
      response.end(JSON.stringify(result, jsonReplacer)); return;
    }
    for (const [service, routes] of [[ActorsService, actorRoutes]]) for (const method of service.methods) {
      const route = "/" + routes[method.localName].replace("{sha256hex}", "01".repeat(32)).replace("{alias}", "current");
      if (route !== pathname) continue;
      const input = fromJsonString(method.input, data.toString("utf8"));
      if (input.actorId === "oversize" || input.jobId === "oversize") { response.writeHead(200, { "content-type": "application/json" }).end(" ".repeat(64)); return; }
      seen.add(`${service.typeName}/${method.name}`);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(toJsonString(method.output, create(method.output, actorResponse(method, input)))); return;
    }
    response.writeHead(404).end();
  } catch (error) {
    if (pathname.startsWith("/acyclic.")) console.error(`browser fixture handler failed for ${pathname}`, error);
    response.writeHead(400, { "content-type": "application/json" });
    if (pathname.startsWith("/v1/objects/")) response.end(encode_objects_v1_json("ErrorDetail", toBinary(objectsWire.ErrorDetailSchema, create(objectsWire.ErrorDetailSchema, { code: typeof error.code === "number" ? error.code : objectsWire.ErrorCode.UNAVAILABLE })), 1024));
    else response.end(JSON.stringify({ code: error.code ?? "unavailable" }));
  }
});
await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
const profile = mkdtempSync(join(tmpdir(), "acyclic-sdk-http-browser-"));
let launched; let browser;
try {
  const chromePath = chromeExecutable("CHROME=/path/to/chrome node test/browser-http-conformance.mjs");
  launched = await spawnDriver(chromePath, ["--headless=new", "--remote-debugging-port=0", `--user-data-dir=${profile}`, `--ignore-certificate-errors-spki-list=${trust}`, "--no-first-run", "--no-default-browser-check", "about:blank"], /DevTools listening on (ws:\/\/\S+)/, "set CHROME and rerun node test/browser-http-conformance.mjs");
  browser = await connectBrowser(launched.endpoint, "node test/browser-http-conformance.mjs");
  const send = browser.send;
  const { targetId } = await send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
  await send("Runtime.enable", {}, sessionId);
  const outcome = navigateChrome(browser, sessionId, `https://localhost:${server.address().port}/typescript/packages/actors/test/browser-http.html`);
  // The fixture has decoded the authenticated request before the client aborts.
  const ready = await Promise.race([entered.then(() => true), outcome.then(() => false)]);
  assert.equal(ready, true, "browser completed before dispatching the cancellation request");
  await send("Runtime.evaluate", { expression: "globalThis.abortActorsRequest()" }, sessionId);
  const [result, cancelled] = await Promise.all([outcome, closed]);
  assert.equal(result.status, "passed", result.text);
  assert.equal(cancelled, true, "the response must close without completing");
  assert.equal(cancellationDispatched, true);
  console.log("Chrome Actors gRPC-Web: authenticated request decoded before abort; pending response closed without completion");
  assert.equal(seen.size, 20 + ActorsService.methods.length, `expected 10 Objects, ${ActorsService.methods.length} Actor and 10 Stream HTTP routes: ${[...seen]}`);
  console.log(`Chrome HTTPS: ${result.text}; ${seen.size} fixture routes observed`);
  const memory = await navigateChrome(browser, sessionId, `https://localhost:${server.address().port}/typescript/packages/objects/test/browser-wasm.html`);
  assert.equal(memory.status, "passed", memory.text);
  console.log("Chrome Objects v1 default export: memory blob PUT, range GET and idempotent re-PUT passed");
  await send("Browser.close"); await launched.exited;
} finally {
  browser?.close();
  if (launched) {
    if (launched.child.exitCode === null && launched.child.signalCode === null) launched.child.kill();
    await launched.exited;
  }
  server.closeAllConnections(); await new Promise(resolveClose => server.close(resolveClose));
  assert.ok(resolve(profile).startsWith(`${resolve(tmpdir())}${sep}`), "browser profile must remain in the task's temporary directory");
  rmSync(profile, { recursive: true, force: true });
}
