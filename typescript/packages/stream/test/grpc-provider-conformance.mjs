import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { createClient, ConnectError, Code } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager, connectNodeAdapter } from "@connectrpc/connect-node";
const installed = process.argv.includes("--installed");
const installedRequire = createRequire(pathToFileURL(join(process.cwd(), "package.json")));
const grpcUrl = installed ? pathToFileURL(installedRequire.resolve("@acyclic-labs/stream/grpc")) : new URL("../dist/grpc.js", import.meta.url);
const packageUrl = new URL("../", grpcUrl);
if (installed && grpcUrl.href === new URL("../dist/grpc.js", import.meta.url).href) throw new Error("--installed requires a freshly packed consumer, not the workspace package");
const { createStreamGrpcClient, GrpcStreamProvider } = await import(grpcUrl.href);
const { idempotencyKey, commitId, StreamError } = await import(installed ? pathToFileURL(installedRequire.resolve("@acyclic-labs/stream")).href : new URL("../dist/types.js", import.meta.url).href);
const { ensureStreamWasm } = await import(new URL("dist/contract.js", packageUrl).href);
const { WasmStream } = await import(new URL("generated/wasm/acyclic_stream_wasm.js", packageUrl).href);
const { Codec, ReadResponseSchema, RecordBatchSchema, StreamService } = await import(installed ? pathToFileURL(installedRequire.resolve("@acyclic-labs/stream/proto")).href : new URL("../generated/proto/stream/v1/stream_pb.js", import.meta.url).href);

const file = fileURLToPath(import.meta.url);
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const key = text => idempotencyKey(new TextEncoder().encode(text));
const body = text => new TextEncoder().encode(text);

const ackModes = { explicit: 1, success: 2, failure: 3, delayed: 4, optout: 5, token: 6, cursor: 7, identity: 8, count: 9, codec: 10, signal: 11, dropRead: 12, dropFollow: 13, pressure: 14, emptyToken: 15, absent: 16, unsupported: 17 };
function deliveryToken(mode, prefix, serial = 0, issuer = 1) {
  const token = new Uint8Array(32);
  token[0] = ackModes[mode];
  token[1] = prefix === "bun" ? 1 : 0;
  token[2] = serial;
  token[31] = issuer;
  return token;
}
async function deliveryAcknowledgements(options, prefix) {
  const provider = new GrpcStreamProvider(options);
  const state = (mode, field) => provider.tail(`ack-state/${prefix}/${mode}/${field}`);
  const waitFor = async (mode, field, minimum = 1n) => {
    const deadline = Date.now() + 2000;
    while (await state(mode, field) < minimum) {
      assert.ok(Date.now() < deadline, `missing ${mode} ${field}`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  };
  const first = async iterator => {
    let timer;
    try {
      return await Promise.race([iterator.next(), new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("ACK blocked record delivery")), 250); })]);
    } finally { clearTimeout(timer); }
  };
  const open = (mode, client = provider, follow = false, signal) => (follow
    ? client.follow(`ack-test/${prefix}/${mode}`, { from: 0n, ...(signal === undefined ? {} : { signal }) })
    : client.read(`ack-test/${prefix}/${mode}`, { from: 0n, limit: mode === "pressure" ? 64 : 2 }))[Symbol.asyncIterator]();
  const raw = createStreamGrpcClient(options);
  for (const length of [0, 31, 33]) {
    await assert.rejects(provider.acknowledgeDelivery(new Uint8Array(length)), error => error.code === "invalid_argument");
    assert.throws(() => raw.acknowledgeDelivery({ deliveryToken: new Uint8Array(length) }), error => error.code === "invalid_argument");
  }
  assert.equal(await state("explicit", "started"), 0n);
  const token = deliveryToken("explicit", prefix);
  await provider.acknowledgeDelivery(token);
  await provider.acknowledgeDelivery(token);
  await raw.acknowledgeDelivery({ deliveryToken: token });
  assert.equal(await state("explicit", "received"), 1n, "fixture receipt is idempotent");
  const other = new GrpcStreamProvider({ ...options, endpoint: options.otherEndpoint });
  await assert.rejects(other.acknowledgeDelivery(token), error => error.code === "access_denied");
  await assert.rejects(new GrpcStreamProvider({ ...options, token: "wrong" }).acknowledgeDelivery(token), error => error.code === "access_denied");
  assert.equal(await state("explicit", "received"), 1n);
  await assert.rejects(provider.acknowledgeDelivery(deliveryToken("unsupported", prefix)), error => error.code === "unsupported");
  for (const mode of ["success", "failure", "delayed", "unsupported"]) {
    const iterator = open(mode);
    assert.equal((await first(iterator)).value.sequence, 0n);
    await waitFor(mode, "started");
    if (mode === "success") await waitFor(mode, "received");
    assert.equal((await first(iterator)).value.sequence, 1n);
    if (mode === "delayed") {
      await waitFor(mode, "cancelled");
      assert.equal(await state(mode, "received"), 0n, "500ms deadline cancels a stalled ACK");
    }
    if (mode === "delayed") await iterator.return();
    else assert.equal((await iterator.next()).done, true, "failed ACKs must not change read completion");
  }
  for (const mode of ["token", "emptyToken", "cursor", "identity", "count", "codec"]) {
    const iterator = mode === "count"
      ? provider.read(`ack-test/${prefix}/${mode}`, { from: 0n, limit: 1 })[Symbol.asyncIterator]()
      : open(mode);
    await assert.rejects(iterator.next(), error => error.code === "invalid_response");
    if (mode !== "count") await assert.rejects(open(mode, provider, true).next(), error => error.code === "invalid_response");
    if (mode === "token" || mode === "emptyToken") await assert.rejects(open(mode, new GrpcStreamProvider({ ...options, autoAcknowledge: false })).next(), error => error.code === "invalid_response");
    assert.equal(await state(mode, "started"), 0n, `invalid ${mode} frame must not ACK or yield even its valid first record`);
  }
  for (const mode of ["optout", "absent"]) {
    const iterator = open(mode, mode === "optout" ? new GrpcStreamProvider({ ...options, autoAcknowledge: false }) : provider);
    assert.equal((await first(iterator)).value.sequence, 0n);
    assert.equal((await first(iterator)).value.sequence, 1n);
    await iterator.return();
    assert.equal(await state(mode, "started"), 0n);
  }
  for (const [mode, follow] of [["dropRead", false], ["dropFollow", true]]) {
    const iterator = open(mode, provider, follow);
    await first(iterator);
    await waitFor(mode, "started");
    await iterator.return();
    await waitFor(mode, "cancelled");
  }
  const controller = new AbortController();
  const signalled = open("signal", provider, true, controller.signal);
  await first(signalled);
  await waitFor("signal", "started");
  controller.abort();
  await waitFor("signal", "cancelled");
  assert.equal((await signalled.next()).done, true);
  const pressured = open("pressure");
  for (let sequence = 0; sequence < 64; sequence++) assert.equal((await first(pressured)).value.sequence, BigInt(sequence));
  await waitFor("pressure", "cancelled");
  await new Promise(resolve => setTimeout(resolve, 100));
  assert.ok(await state("pressure", "started") < 64n, "pressure must drop receipts without dropping records");
  await pressured.return();
  if (process.argv.includes("--native")) {
    const nativeUrl = installed ? pathToFileURL(installedRequire.resolve("@acyclic-labs/stream/native")) : new URL("../dist/native.js", import.meta.url);
    const { NativeStreamProvider } = await import(nativeUrl.href);
    const native = await NativeStreamProvider.connect(options.endpoint, options.token, body(options.caCertificate));
    for (const length of [0, 31, 33]) await assert.rejects(native.acknowledgeDelivery(new Uint8Array(length)), error => error.code === "invalid_argument");
    await native.acknowledgeDelivery(token);
    await assert.rejects(native.acknowledgeDelivery(deliveryToken("unsupported", prefix)), error => error.code === "unsupported");
    assert.equal(await state("explicit", "received"), 1n);
    const otherNative = await NativeStreamProvider.connect(options.otherEndpoint, options.token, body(options.caCertificate));
    await assert.rejects(otherNative.acknowledgeDelivery(token), error => error.code === "access_denied");
    const optedOut = await NativeStreamProvider.connect(options.endpoint, options.token, body(options.caCertificate), undefined, { autoAcknowledge: false });
    const disabled = open("optout", optedOut, true);
    assert.equal((await first(disabled)).value.sequence, 0n);
    await disabled.return();
    assert.equal(await state("optout", "started"), 0n);
    const iterator = open("dropFollow", native, true);
    assert.equal((await first(iterator)).value.sequence, 0n);
    await waitFor("dropFollow", "started", 2n);
    await iterator.return();
    await waitFor("dropFollow", "cancelled", 2n);
    const failed = [];
    for await (const record of native.read(`ack-test/${prefix}/failure`, { from: 0n, limit: 2 })) failed.push(record.sequence);
    assert.deepEqual(failed, [0n, 1n], "native ACK failure does not change a read result");
  }
}
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
  await deliveryAcknowledgements(options, prefix);
  console.log(`${prefix}: Stream provider replay, follow cancellation, forks, Commit, delivery ACK isolation/bounds/cleanup and explicit APIs passed (${installed ? "installed archive" : "workspace"}${process.argv.includes("--native") ? ", N-API bridge" : ""})`);
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

const ackStates = new Map();
function ackState(prefix, mode) {
  const key = `${prefix}/${mode}`;
  if (!ackStates.has(key)) ackStates.set(key, { started: 0n, cancelled: 0n, received: 0n, receipts: new Set() });
  return ackStates.get(key);
}
function aborted(signal) {
  if (signal.aborted) return Promise.resolve();
  return new Promise(resolve => signal.addEventListener("abort", resolve, { once: true }));
}
function createAdapter(issuer) { return connectNodeAdapter({ routes(router) {
  const implementation = {};
  for (const method of StreamService.methods) {
    if (method.methodKind === "server_streaming") {
      implementation[method.localName] = async function* (request, context) {
        authenticate(context);
        if (request.path.startsWith("ack-test/")) {
          const [, prefix, mode] = request.path.split("/");
          const records = [0n, 1n].map(sequence => ({ sequence, value: body("receipt"), commitId: new Uint8Array(32).fill(7), committedAtMicros: 1n }));
          if (mode === "cursor") records[1].sequence = 9n;
          if (mode === "identity") records[1].commitId = new Uint8Array(31);
          if (mode === "pressure") {
            for (let index = 0; index < 64; index++) {
              const frame = readFrame([{ ...records[0], sequence: BigInt(index) }], true);
              frame.deliveryToken = deliveryToken(mode, prefix, index, issuer);
              yield frame;
            }
          } else {
            const frame = readFrame(records, true);
            if (mode !== "absent") frame.deliveryToken = mode === "token" ? new Uint8Array(31) : mode === "emptyToken" ? new Uint8Array(0) : deliveryToken(mode, prefix, 0, issuer);
            if (mode === "codec") frame.codec = 3;
            yield frame;
          }
          if (mode === "success" || mode === "failure" || mode === "unsupported") return;
          await aborted(context.signal);
          return;
        }
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
        if (method.localName === "tail" && request.path.startsWith("ack-state/")) {
          const [, prefix, mode, field] = request.path.split("/");
          return { tail: ackState(prefix, mode)[field] };
        }
        if (method.localName === "acknowledgeDelivery") {
          const token = request.deliveryToken;
          if (token.length !== 32) throw new ConnectError("invalid token", Code.InvalidArgument);
          if (token[31] !== issuer) throw new ConnectError("another issuing endpoint", Code.PermissionDenied);
          const mode = Object.keys(ackModes).find(name => ackModes[name] === token[0]);
          const state = ackState(token[1] === 1 ? "bun" : "node", mode);
          state.started++;
          if (["delayed", "signal", "dropRead", "dropFollow"].includes(mode) || mode === "pressure" && token[2] === 0) {
            await aborted(context.signal);
            state.cancelled++;
            throw new ConnectError("cancelled", Code.Canceled);
          }
          if (mode === "failure" || mode === "pressure") throw new ConnectError("receipt unavailable", Code.Unavailable);
          if (mode === "unsupported") throw new ConnectError("method not implemented", Code.Unimplemented);
          const receipt = Buffer.from(token).toString("hex");
          if (!state.receipts.has(receipt)) { state.receipts.add(receipt); state.received++; }
          return {};
        }
        try { return fromBinary(method.output, await memory.dispatch(operation(method.localName), toBinary(method.input, request))); }
        catch (error) { throw canonicalError(error); }
      };
    }
  }
  router.service(StreamService, implementation);
} }); }
const server = createSecureServer({ key: tls.key, cert: tls.certificate }, createAdapter(1));
const otherServer = createSecureServer({ key: tls.key, cert: tls.certificate }, createAdapter(2));
const closeServer = ownFixtureServer(server);
const closeOtherServer = ownFixtureServer(otherServer);
try {
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  await new Promise(resolve => otherServer.listen(0, "localhost", resolve));
  const runtimes = process.argv.includes("--node-only") ? [process.execPath] : process.argv.includes("--bun-only") ? ["bun"] : [process.execPath, "bun"];
  for (const runtime of runtimes) await new Promise((resolve, reject) => {
    const flags = ["--client", ...(installed ? ["--installed"] : []), ...(process.argv.includes("--native") ? ["--native"] : [])];
    const child = spawn(runtime, [file, ...flags], { cwd: installed ? process.cwd() : root, stdio: ["pipe", "inherit", "inherit"] });
    child.stdin.end(JSON.stringify({ endpoint: `https://localhost:${server.address().port}`, otherEndpoint: `https://localhost:${otherServer.address().port}`, token: "conformance", caCertificate: tls.certificate }));
    child.on("error", reject);
    child.on("exit", code => code === 0 ? resolve() : reject(new Error(`Stream provider conformance exited ${code}`)));
  });
} finally { await closeServer(); await closeOtherServer(); memory.free(); }

