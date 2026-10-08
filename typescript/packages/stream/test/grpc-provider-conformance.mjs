import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { zstdCompressSync } from "node:zlib";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { createClient, ConnectError, Code } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager, connectNodeAdapter } from "@connectrpc/connect-node";
import { GrpcStreamProvider } from "../dist/grpc.js";
import { idempotencyKey, commitId, StreamError } from "../dist/types.js";
import { ensureStreamWasm } from "../dist/contract.js";
import { WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";
import { Codec, ReadResponseSchema, RecordBatchSchema, StreamService } from "../generated/proto/stream/v1/stream_pb.js";

const file = fileURLToPath(import.meta.url);
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const key = text => idempotencyKey(new TextEncoder().encode(text));
const body = text => new TextEncoder().encode(text);
if (process.argv.includes("--client")) {
  let config = "";
  for await (const chunk of process.stdin) config += chunk;
  const options = JSON.parse(config);
  const provider = new GrpcStreamProvider(options);
  const prefix = process.versions.bun ? "bun" : "node";
  const path = `${prefix}/events`;
  await assert.rejects(provider.tail(path), error => error instanceof StreamError && error.code === "stream_not_found");
  assert.equal(await provider.inspectIdempotency(key(`${prefix}-missing`)), undefined);
  const appended = await provider.append(path, [body("one"), body("two")], { ifTail: 0n, idempotencyKey: key(`${prefix}-append`) });
  assert.equal(appended.ok, true);
  assert.equal(appended.end, 2n);
  const session = new Http2SessionManager(new URL(options.endpoint), {}, { ca: options.caCertificate });
  try {
    const retiredService = { ...StreamService, typeName: "acyclic.stream.v2.StreamService" };
    retiredService.methods = StreamService.methods.map(method => ({ ...method, parent: retiredService }));
    const obsolete = createClient(retiredService, createGrpcTransport({ baseUrl: options.endpoint, sessionManager: session, defaultTimeoutMs: 2000 }));
    await assert.rejects(obsolete.tail({ path }, { headers: { authorization: `Bearer ${options.token}` } }), error => error instanceof ConnectError && error.code === Code.Unimplemented);
  } finally { session.abort(); }
  assert.equal((await provider.inspectIdempotency(key(`${prefix}-append`))).outcome.type, "append");
  assert.deepEqual(await provider.append(path, [body("one"), body("two")], { ifTail: 0n, idempotencyKey: key(`${prefix}-append`) }), appended);
  await assert.rejects(provider.append(path, [body("changed")], { ifTail: 0n, idempotencyKey: key(`${prefix}-append`) }), error => error instanceof StreamError && error.code === "idempotency_mismatch");
  assert.deepEqual(await provider.append(path, [body("conflict")], { ifTail: 0n }), { ok: false, code: "tail_conflict", actualTail: 2n });
  const records = [];
  for await (const record of provider.read(path, { from: 0n, limit: 2 })) records.push(record);
  assert.deepEqual(records.map(item => new TextDecoder().decode(item.value)), ["one", "two"]);
  const mixedPath = `mixed/${prefix}`;
  await provider.append(mixedPath, ["one", "two", "three", "four", "five"].map(body), { ifTail: 0n });
  const mixed = [];
  for await (const record of provider.read(mixedPath, { from: 1n, limit: 4 })) mixed.push(record);
  assert.deepEqual(mixed.map(item => [item.sequence, new TextDecoder().decode(item.value)]), [[1n, "two"], [2n, "three"], [3n, "four"], [4n, "five"]]);
  const controller = new AbortController();
  let followed = 0;
  for await (const record of provider.follow(path, { from: 0n, signal: controller.signal })) {
    assert.equal(record.sequence, BigInt(followed++));
    if (followed === 2) controller.abort();
  }
  assert.equal(followed, 2);
  assert.equal((await provider.fork(path, `${prefix}/fork`, { atTail: 1n, idempotencyKey: key(`${prefix}-fork`) })).tail, 1n);
  assert.deepEqual((await provider.childrenPage({ parent: prefix, limit: 8 })).children.map(item => item.path), [`${prefix}/events`, `${prefix}/fork`]);
  const committed = await provider.commit({ conditions: [{ path, ifTail: 2n }, { path: `${prefix}/other`, ifAbsent: true }], mutations: [{ append: { path, values: [body("three")] } }, { append: { path: `${prefix}/other`, values: [body("other")] } }] }, { idempotencyKey: key(`${prefix}-commit`) });
  assert.equal(committed.ok, true);
  assert.equal(committed.tails[path], 3n);
  assert.equal((await provider.readCommit(committed.commitId)).mutations.length, 2);
  assert.equal((await provider.inspectIdempotency(key(`${prefix}-commit`))).outcome.type, "commit");
  const conflict = await provider.commit({ conditions: [{ path, ifTail: 0n }], mutations: [{ append: { path, values: [body("rejected")] } }] }, { idempotencyKey: key(`${prefix}-conflict`) });
  assert.equal(conflict.ok, false);
  assert.equal(await provider.tail(path), 3n);
  await assert.rejects(provider.commit({ conditions: [{ path: `${prefix}/invalid-cut`, ifAbsent: true }], mutations: [{ fork: { source: path, destination: `${prefix}/invalid-cut`, atTail: 4n, values: [] } }] }, { idempotencyKey: key(`${prefix}-invalid-cut`) }), error => error instanceof StreamError && error.code === "invalid_argument");
  const denied = new GrpcStreamProvider({ ...options, token: "wrong" });
  await assert.rejects(denied.tail(path), error => error instanceof StreamError && error.code === "access_denied");
  await assert.rejects(provider.tail("invalid//path"), error => error instanceof StreamError && error.code === "invalid_path");
  await assert.rejects(provider.readCommit(commitId(new Uint8Array(32))), error => error instanceof StreamError && error.code === "commit_not_found");
  await assert.rejects(async () => { for await (const _ of provider.read("malformed", { from: 0n, limit: 1 })) {} }, error => error instanceof StreamError && error.code === "invalid_response");
  for (const codec of [0, 3]) {
    await assert.rejects(async () => { for await (const _ of provider.read(`invalid-codec-${codec}`, { from: 0n, limit: 1 })) {} }, error => error instanceof StreamError && error.code === "invalid_response");
  }
  const bounded = new GrpcStreamProvider({ ...options, maximumMessageBytes: 64 });
  await assert.rejects(async () => { for await (const _ of bounded.read("oversize", { from: 0n, limit: 1 })) {} }, error => error instanceof StreamError && error.code === "capacity_exhausted");
  console.log(`${prefix}: Stream provider replay, follow cancellation, forks, multi-path Commit, idempotency, authentication, errors and bounds passed`);
  process.exit(0);
}

await ensureStreamWasm();
const memory = new WasmStream();
const identity = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
assert.equal(identity.status, 0, identity.stderr);
const tls = JSON.parse(identity.stdout);
// One Read frame per batch, optionally Zstandard-compressed.
function readFrame(records, zstd) {
  const data = toBinary(RecordBatchSchema, create(RecordBatchSchema, { records }));
  return create(ReadResponseSchema, { codec: zstd ? Codec.ZSTD : Codec.NONE, data: zstd ? zstdCompressSync(data) : data, decodedLength: BigInt(data.length) });
}
const operation = name => name.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
function authenticate(context) { if (context.requestHeader.get("authorization") !== "Bearer conformance") throw new ConnectError("missing bearer", Code.Unauthenticated); }
function canonicalError(error) {
  if (error instanceof ConnectError) return error;
  const codes = { not_found: Code.NotFound, invalid_path: Code.InvalidArgument, invalid_argument: Code.InvalidArgument, out_of_range: Code.OutOfRange, already_exists: Code.AlreadyExists, limit_exceeded: Code.InvalidArgument, idempotency_mismatch: Code.FailedPrecondition, prefix_not_retained: Code.FailedPrecondition };
  return new ConnectError(error.code ?? String(error), codes[error.code] ?? Code.Unavailable);
}
const adapter = connectNodeAdapter({ routes(router) {
  const implementation = {};
  for (const method of StreamService.methods) {
    if (method.methodKind === "server_streaming") {
      implementation[method.localName] = async function* (request, context) {
        authenticate(context);
        if (request.path === "invalid-codec-0" || request.path === "invalid-codec-3") {
          const frame = readFrame([{ sequence: 0n, value: new Uint8Array([1]), commitId: new Uint8Array(32) }], false);
          frame.codec = request.path === "invalid-codec-0" ? 0 : 3;
          yield frame;
          return;
        }
        if (request.path === "malformed" || request.path === "oversize") {
          yield readFrame([{ sequence: request.path === "malformed" ? 1n : 0n, value: new Uint8Array(request.path === "oversize" ? 256 : 1), commitId: new Uint8Array(32) }], false);
          return;
        }
        if (method.localName === "follow") {
          const handle = await memory.open_follow(toBinary(method.input, request));
          const close = () => handle.close();
          context.signal.addEventListener("abort", close, { once: true });
          try { for (;;) { const bytes = await handle.next(); if (bytes === null) return; yield fromBinary(method.output, bytes); } }
          finally { context.signal.removeEventListener("abort", close); handle.close(); handle.free(); }
        } else if (method.localName === "read") {
          // Regroup into one compressed batch followed by plain single-record frames.
          let records;
          try { records = (await memory.read(toBinary(method.input, request))).flatMap(bytes => fromBinary(RecordBatchSchema, fromBinary(ReadResponseSchema, bytes).data).records); }
          catch (error) { throw canonicalError(error); }
          const half = Math.ceil(records.length / 2);
          if (half > 0) yield readFrame(records.slice(0, half), true);
          for (const record of records.slice(half)) yield readFrame([record], false);
        } else {
          try { for (const bytes of await memory[method.localName](toBinary(method.input, request))) yield fromBinary(method.output, bytes); }
          catch (error) { throw canonicalError(error); }
        }
      };
    } else {
      implementation[method.localName] = async (request, context) => {
        authenticate(context);
        try { return fromBinary(method.output, await memory.dispatch(operation(method.localName), toBinary(method.input, request))); }
        catch (error) { throw canonicalError(error); }
      };
    }
  }
  router.service(StreamService, implementation);
} });
const server = createSecureServer({ key: tls.key, cert: tls.certificate }, adapter);
await new Promise(resolve => server.listen(0, "localhost", resolve));
try {
  for (const runtime of [process.execPath, "bun"]) await new Promise((resolve, reject) => {
    const child = spawn(runtime, [file, "--client"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
    child.stdin.end(JSON.stringify({ endpoint: `https://localhost:${server.address().port}`, token: "conformance", caCertificate: tls.certificate }));
    child.on("error", reject);
    child.on("exit", code => code === 0 ? resolve() : reject(new Error(`Stream provider conformance exited ${code}`)));
  });
} finally { await new Promise(resolve => server.close(resolve)); memory.free(); }

