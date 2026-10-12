import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createSecureServer } from "node:http2";
import { once } from "node:events";
import { test } from "node:test";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { createFilesystemGrpcClient } from "../dist/grpc.js";
import * as wire from "../generated/proto/filesystem/v2/filesystem_pb.js";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

const cert = readFileSync(new URL("../../machines/test/fixtures/server.pem", import.meta.url), "utf8");
const key = readFileSync(new URL("../../machines/test/fixtures/server-key.pem", import.meta.url), "utf8");
const workspace = { workspaceId: new Uint8Array(16).fill(1), name: "fixture" };
const generation = { workspace, generationId: new Uint8Array(16).fill(2) };
const operationId = new Uint8Array(16).fill(3);
const operation = { idempotencyKey: operationId };
const responses = {
  getGeneration: { generation, parents: [generation] },
  read: { contents: new Uint8Array([0, 255]) },
  import: { outcome: { status: wire.MutationStatus.COMMITTED, generation } },
  issueMountCredential: { endpoint: "https://fixture", expiresAtUnixSeconds: 9007199254740993n, credential: { case: "bearerToken", value: "mount-fixture" } },
  issueS3Credential: { endpoint: "https://fixture", credential: { case: "s3", value: { bucket: "fixture", region: "fixture", accessKeyId: "fixture", secretAccessKey: "fixture" } } },
  observe: { state: "committed", outcome: { status: wire.MutationStatus.COMMITTED, generation } },
  cancel: { operation: { state: "committed" } },
};
const requests = {
  handshake: { protocol: { protocol: { version: "2", descriptorDigest: "fixture" } } },
  createWorkspace: { name: "fixture", profile: wire.FilesystemProfile.PORTABLE, operation },
  openWorkspace: { selector: { case: "name", value: "fixture" } },
  deleteWorkspace: { workspace, operation },
  getHead: { workspace },
  getGeneration: { generation },
  read: { generation, path: "file", range: { offset: 9007199254740993n, length: 7n } },
  stat: { generation, path: "file" },
  listDirectory: { generation, path: "/", page: { maximumItems: 3 } },
  readLink: { generation, path: "link" },
  planExtents: { generation, path: "file", range: { offset: 9n, length: 7n } },
  applyTransaction: { base: generation, operation, maximumConflicts: 3 },
  rebaseTransaction: { base: generation, operation, maximumConflicts: 3 },
  forkWorkspace: { source: generation, destinationName: "fork", operation },
  diff: { from: generation, to: generation, maximumChanges: 3 },
  rebase: { workspace, maximumConflicts: 3, maximumGenerations: 4, maximumChanges: 3, operation },
  planJoin: { source: generation, target: generation, maximumChanges: 3, maximumConflicts: 2, maximumGenerations: 4 },
  applyJoin: { plan: { planId: new Uint8Array([4]), source: generation, expectedTarget: generation }, operation },
  checkpoint: { generation, identity: "checkpoint", operation },
  pin: { generation, identity: "pin", operation },
  export: { generation, after: new Uint8Array([5]), maximumObjects: 3, maximumBytes: 1024n },
  import: { workspace, operationId, cursor: new Uint8Array([6]), objectId: new Uint8Array([7]), contents: new Uint8Array([0, 255]), terminal: false },
  issueMountCredential: { workspace, generation, writable: true, expiresAfterSeconds: 30n, operation },
  issueS3Credential: { workspace, generation, writable: false, expiresAfterSeconds: 30n, operation },
  getSourceState: { workspace },
  reconcileSource: { workspace, operation },
  rescanSource: { workspace, operation },
  sealSource: { workspace, operation },
  observe: { workspace, operationId },
  cancel: { workspace, operationId },
};

async function fixture(t, implementation, overrides = {}) {
  const server = createSecureServer({ cert, key }, connectNodeAdapter({
    routes(router) { router.service(wire.FilesystemService, implementation); },
  }));
  const close = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const options = { endpoint: `https://127.0.0.1:${server.address().port}`, token: "fixture", caCertificate: cert, ...overrides };
  const client = createFilesystemGrpcClient(options);
  t.after(async () => { client.close(); await close(); });
  return { ...client, options };
}

test("all 30 pinned filesystem RPCs forward exact protobufs and bearer credentials", async t => {
  assert.equal(wire.FilesystemService.methods.length, 30);
  assert.deepEqual(Object.keys(requests).sort(), wire.FilesystemService.methods.map(method => method.localName).sort());
  const called = new Set();
  const implementation = {};
  for (const method of wire.FilesystemService.methods) {
    for (const field of Object.keys(requests[method.localName])) {
      assert.ok([...method.input.fields, ...method.input.oneofs].some(candidate => candidate.localName === field), `${method.name}.${field}`);
    }
    const request = create(method.input, requests[method.localName]);
    const response = create(method.output, responses[method.localName]);
    const check = context => {
      assert.equal(context.requestHeader.get("authorization"), "Bearer fixture");
      called.add(method.localName);
    };
    if (method.methodKind === "client_streaming") {
      implementation[method.localName] = async (stream, context) => {
        check(context);
        const chunks = [];
        for await (const chunk of stream) chunks.push(chunk);
        assert.deepEqual(chunks, [request, create(method.input, { ...requests.import, terminal: true })]);
        return response;
      };
    } else if (method.methodKind === "server_streaming") {
      implementation[method.localName] = async function* (query, context) {
        check(context);
        assert.deepEqual(query, request);
        yield { cursor: new Uint8Array([1]), objectId: new Uint8Array([2]), contents: new Uint8Array([0, 255]) };
        yield { terminal: true };
      };
    } else {
      implementation[method.localName] = (query, context) => {
        check(context);
        assert.deepEqual(query, request);
        return response;
      };
    }
  }
  const { filesystem } = await fixture(t, implementation);
  for (const method of wire.FilesystemService.methods) {
    const request = create(method.input, requests[method.localName]);
    if (method.methodKind === "client_streaming") {
      async function* chunks() { yield request; yield create(method.input, { ...requests.import, terminal: true }); }
      assert.deepEqual(await filesystem[method.localName](chunks()), create(method.output, responses[method.localName]));
    } else if (method.methodKind === "server_streaming") {
      const chunks = [];
      for await (const chunk of filesystem[method.localName](request)) chunks.push(chunk);
      assert.deepEqual(chunks, [
        create(method.output, { cursor: new Uint8Array([1]), objectId: new Uint8Array([2]), contents: new Uint8Array([0, 255]) }),
        create(method.output, { terminal: true }),
      ]);
    } else {
      assert.deepEqual(await filesystem[method.localName](request), create(method.output, responses[method.localName]));
    }
  }
  assert.equal(called.size, 30);
});

test("operation errors retain gRPC status and do not retry a mutation", async t => {
  let calls = 0;
  const { filesystem } = await fixture(t, {
    cancel() { calls++; throw new ConnectError("operation already committed", Code.FailedPrecondition); },
  });
  await assert.rejects(filesystem.cancel(requests.cancel), error => error.code === Code.FailedPrecondition);
  assert.equal(calls, 1);
});

test("caller cancellation stops an active export and retains the disabled deadline", { timeout: 5000 }, async t => {
  let calls = 0;
  let stopped;
  const cancelled = new Promise(resolve => { stopped = resolve; });
  const { filesystem } = await fixture(t, {
    async *export(query, context) {
      calls++;
      assert.deepEqual(query, create(wire.ExportRequestSchema, requests.export));
      assert.equal(context.requestHeader.has("grpc-timeout"), false);
      const aborted = once(context.signal, "abort").then(stopped);
      yield { contents: new Uint8Array([1]) };
      await aborted;
    },
  });
  const controller = new AbortController();
  const stream = filesystem.export(requests.export, { timeoutMs: 0, signal: controller.signal })[Symbol.asyncIterator]();
  assert.deepEqual((await stream.next()).value.contents, new Uint8Array([1]));
  controller.abort();
  await assert.rejects(stream.next(), error => error.code === Code.Canceled);
  await cancelled;
  assert.equal(calls, 1);
});

test("a pending mutation deadline cancels server work without redispatch", { timeout: 5000 }, async t => {
  let calls = 0;
  let stopped;
  const cancelled = new Promise(resolve => { stopped = resolve; });
  const { filesystem } = await fixture(t, {
    async cancel(query, context) {
      calls++;
      assert.deepEqual(query, create(wire.CancelRequestSchema, requests.cancel));
      assert.ok(context.timeoutMs() > 0 && context.timeoutMs() <= 250);
      await once(context.signal, "abort");
      stopped();
      return {};
    },
  });
  await assert.rejects(filesystem.cancel(requests.cancel, { timeoutMs: 250 }), error => error.code === Code.DeadlineExceeded);
  await cancelled;
  assert.equal(calls, 1);
});

test("transport requires HTTPS, a bounded bearer and bounded messages", async t => {
  for (const overrides of [{ endpoint: "http://localhost" }, { token: "" }, { token: "x\ny" }, { token: "x".repeat(12289) }, { maximumMessageBytes: 0 }, { caCertificate: "" }]) {
    assert.throws(() => createFilesystemGrpcClient({ endpoint: "https://localhost", token: "fixture", ...overrides }));
  }
  const { filesystem } = await fixture(t, { read() { return { contents: new Uint8Array(1024) }; } }, { maximumMessageBytes: 256 });
  await assert.rejects(filesystem.read(requests.read), error => error.code === Code.ResourceExhausted);
});
