import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import test from "node:test";
import { create, fromBinary } from "@bufbuild/protobuf";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import * as actors from "../../actors/generated/proto/actors/v1/actors_pb.js";
import initializeActors, { ActorsClient as RustActorsClient } from "../../actors/generated/wasm/acyclic_actors_wasm.js";
import * as objects from "../../objects/generated/proto/objects/v1/objects_pb.js";
import { MemoryObjectsV1 } from "../../objects/dist/v1.js";
import { WorkersDeploymentClient } from "../dist/deployment.js";
import initializeManifest from "../generated/wasm/acyclic_manifest.js";

const module = await readFile(new URL("../generated/wasm/acyclic_manifest_bg.wasm", import.meta.url));
await initializeManifest({ module_or_path: module });
await initializeActors({ module_or_path: await readFile(new URL("../../actors/generated/wasm/acyclic_actors_wasm_bg.wasm", import.meta.url)) });
const sibling = await readFile(new URL("../../objects/generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url));
const fixture = JSON.parse(await readFile(new URL("../../../../rust/crates/manifest/fuzz/corpus/manifest_json/valid.json", import.meta.url), "utf8"));
const code = new TextEncoder().encode("import sibling from './selected.wasm'; export default {fetch(){return new Response(String(sibling))}}");
const sha256 = value => createHash("sha256").update(value).digest("hex");
const intent = value => new TextEncoder().encode(value);

function selection(bytes) {
  const manifest = structuredClone(fixture);
  const environment = manifest.environments[0];
  const entry = environment.entries[0];
  entry.name = "worker";
  entry.module = "worker";
  entry.artifactSha256 = sha256(code);
  entry.assets = [{path: "selected.wasm", sha256: sha256(bytes), size: bytes.length,
    mediaType: "application/wasm", role: "module"}];
  entry.bindings = [];
  entry.secrets = [];
  entry.runtime = {kind: "worker", limits: {cpuMillis: 100, memoryBytes: 268435456, timeoutMillis: 30000}};
  environment.bindings = [];
  environment.grants = [];
  environment.secrets = [];
  environment.crons = [];
  return {manifest: JSON.stringify(manifest), environment: "production", entry: "worker", artifact: code,
    files: [{path: "selected.wasm", bytes}]};
}

// Every call reaches the original real Rust account. This wrapper only records
// its original replies and ownership; it produces no data or completion itself.
function counted(account) {
  const replies = [];
  const routes = [];
  let opened = 0, closed = 0;
  return {
    replies, routes,
    get opened() { return opened; },
    get closed() { return closed; },
    open(route, input) {
      opened++;
      routes.push(route);
      const original = account.open(route, input);
      let done = false;
      return {
        async next() {
          const reply = await original.next();
          if (reply !== null) replies.push({route, reply});
          return reply;
        },
        close() {
          if (done) return;
          done = true;
          closed++;
          original.close();
        },
      };
    },
  };
}

async function deployPublished(published, port) {
  const credential = Promise.withResolvers();
  const renewing = Promise.withResolvers();
  const token = randomUUID();
  const expected = Uint8Array.from(published.request);
  const frames = [];
  const calls = [];
  const clients = [];
  const adapter = connectNodeAdapter({ routes(router) {
    router.service(actors.ActorsService, { deployWorker(request, context) {
      assert.ok(context.requestHeader.get("authorization") === `Bearer ${token}`);
      calls.push(context.method.name);
      return create(actors.DeploymentResponseSchema, { deployment: {
        name: request.name, codeSha256: request.codeSha256, revision: 1n, entrySource: request.entrySource,
      } });
    } });
  } });
  const server = createServer((request, response) => {
    const chunks = [];
    const read = request.read;
    request.read = function(size) {
      const chunk = read.call(this, size);
      if (chunk !== null) chunks.push(chunk);
      return chunk;
    };
    request.once("end", () => frames.push({path: request.url, body: Buffer.concat(chunks)}));
    adapter(request, response);
  });
  const close = ownFixtureServer(server);
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const workers = new WorkersDeploymentClient({ endpoint: `http://127.0.0.1:${server.address().port}`,
      credential() { renewing.resolve(); return credential.promise; },
      binding: { async connect(endpoint, token, signal) {
        const original = await RustActorsClient.connect(endpoint, token, signal);
        clients.push(original);
        return { withCredential: (...args) => original.call_with_credential(...args) };
      } },
    }, port);
    // Mutation while the original credential waits cannot change the selected
    // request or include bytes outside its view in the real Rust transport.
    const backing = new Uint8Array(expected.length + 4);
    backing.set(expected, 2);
    const pending = workers.deploy({...published, request: backing.subarray(2, -2)});
    await renewing.promise;
    assert.equal(clients.length, 0);
    backing.fill(0);
    credential.resolve(token);
    const reply = await pending;
    assert.deepEqual(calls, ["DeployWorker"]);
    assert.equal(frames.length, 1);
    assert.equal(frames[0].path, "/acyclic.actors.v1.ActorsService/DeployWorker");
    assert.equal(frames[0].body[0], 0);
    assert.equal(frames[0].body.readUInt32BE(1), expected.length);
    assert.deepEqual(new Uint8Array(frames[0].body.subarray(5)), expected);
    const request = fromBinary(actors.DeploymentRequestSchema, expected);
    assert.equal(reply.deployment.name, request.name);
    assert.equal(reply.deployment.revision, 1n);
    assert.deepEqual(reply.deployment.codeSha256, request.codeSha256);
    assert.deepEqual(reply.deployment.entrySource.digest, request.entrySource.digest);
    assert.equal(reply.deployment.entrySource.length, request.entrySource.length);
    assert.equal(reply.deployment.entrySource.environment, request.entrySource.environment);
    assert.equal(reply.deployment.entrySource.entry, request.entrySource.entry);
  } finally {
    for (const client of clients) client.free();
    await close();
  }
}

test("Worker entry publication retains the selected closure through the original account", async () => {
  const memory = await MemoryObjectsV1.create();
  const port = counted(memory);
  const workers = new WorkersDeploymentClient({endpoint: "http://127.0.0.1:1234",
    credential: () => assert.fail("publication requested Actors credentials")}, port);
  assert.equal(port.opened, 0);
  const selected = selection(sibling);
  const first = intent("selected-worker-first");
  await assert.rejects(workers.entries.publishDeployment(first, {...selected, files: []}, "first"),
    error => error.name === "ManifestError" && error.rule === "missing-asset");
  await assert.rejects(workers.entries.publishDeployment(first, {...selected, files: [{path: "selected.wasm", bytes: module}]}, "first"),
    error => error.name === "ManifestError" && (error.rule === "asset-size" || error.rule === "asset-digest"));
  await assert.rejects(workers.entries.publishDeployment(first, selected, ""), error => error.name === "ManifestError");
  const cancellation = new AbortController();
  cancellation.abort();
  await assert.rejects(workers.entries.publishDeployment(first, selected, "first", cancellation.signal),
    error => error.name === "AbortError");
  assert.equal(port.opened, 0);

  const published = await workers.entries.publishDeployment(first, selected, "first");
  assert.deepEqual(port.routes, ["blobs/put", "blobs/put", "blobs/put", "blobs/retain"]);
  assert.equal(port.opened, port.closed);
  const descriptor = fromBinary(objects.VerifiedSchema, published.object.body);
  const request = fromBinary(actors.DeploymentRequestSchema, published.request);
  assert.equal(request.name, "worker");
  assert.equal(Buffer.from(request.codeSha256).toString("hex"), sha256(code));
  assert.deepEqual(request.entrySource.digest, descriptor.digest.blake3);
  assert.equal(request.entrySource.length, descriptor.length);
  assert.equal(request.entrySource.environment, "production");
  assert.equal(request.entrySource.entry, "worker");
  assert.deepEqual(published.object.revision, port.replies[2].reply.revision);
  assert.deepEqual(published.retention.revision, port.replies[3].reply.revision);
  assert.ok(published.object.revision);
  assert.ok(published.retention.revision);
  assert.equal(fromBinary(objects.RetainResponseSchema, published.retention.body).references, 1n);
  await deployPublished(published, port);
  const stored = await memory.get({digest: descriptor.digest, maximumBytes: descriptor.length});
  const closure = JSON.parse(new TextDecoder().decode(stored.body));
  assert.equal(closure.manifest.environments.length, 1);
  assert.equal(closure.manifest.environments[0].entries.length, 1);
  assert.equal(closure.assets.length, 1);
  assert.deepEqual((await memory.get({digest: {blake3: Uint8Array.from(closure.artifact.digest)}, maximumBytes: BigInt(code.length)})).body, code);

  // The same JS and sibling basename select a different immutable closure when
  // the actual compiled sibling module changes. Each intent owns its own root.
  const second = intent("selected-worker-second");
  const changed = await workers.entries.publishDeployment(second, selection(module), "second");
  const changedRequest = fromBinary(actors.DeploymentRequestSchema, changed.request);
  const changedDescriptor = fromBinary(objects.VerifiedSchema, changed.object.body);
  assert.deepEqual(changedRequest.codeSha256, request.codeSha256);
  assert.notDeepEqual(changedRequest.entrySource.digest, request.entrySource.digest);
  const foreign = await MemoryObjectsV1.create();
  await assert.rejects(foreign.head(descriptor.digest), error => error.code === objects.ErrorCode.NOT_FOUND);
  const released = await workers.entries.release(first);
  assert.equal(fromBinary(objects.ReleaseResponseSchema, released.body).existed, true);
  assert.ok(released.revision);
  assert.equal((await memory.delete(descriptor.digest)).status, objects.DeleteStatus.DELETED);
  assert.equal((await memory.delete(changedDescriptor.digest)).status, objects.DeleteStatus.REFERENCED);
  await assert.rejects(memory.head(descriptor.digest), error => error.code === objects.ErrorCode.NOT_FOUND);
  const restored = await memory.get({digest: changedDescriptor.digest, maximumBytes: changedDescriptor.length});
  const changedClosure = JSON.parse(new TextDecoder().decode(restored.body));
  assert.deepEqual((await memory.get({digest: {blake3: Uint8Array.from(changedClosure.assets[0].digest)}, maximumBytes: BigInt(module.length)})).body, new Uint8Array(module));
  assert.equal(port.opened, port.closed);
});

test("Worker entry cancellation preserves its reason and closes the original parked operation", async () => {
  const memory = await MemoryObjectsV1.create();
  const selected = selection(sibling);
  for (const operation of ["publication", "release"]) {
    for (const abort of [true, false]) {
      const reading = Promise.withResolvers();
      const resume = Promise.withResolvers();
      const account = counted(memory);
      const originals = [];
      const port = { open(route, input) {
        const original = account.open(route, input);
        originals.push(original);
        return {
          async next() { reading.resolve(); await resume.promise; return original.next(); },
          close() { original.close(); resume.resolve(); },
        };
      } };
      const workers = new WorkersDeploymentClient({endpoint: "http://127.0.0.1:1234",
        credential: () => assert.fail("Objects operation requested Actors credentials")}, port);
      const controller = new AbortController();
      const reason = new Error("publication cancelled by its caller");
      const pending = operation === "publication"
        ? workers.entries.publishDeployment(intent("cancelled"), selected, "cancelled", controller.signal)
        : workers.entries.release(intent("cancelled"), controller.signal);
      await reading.promise;
      const rejected = assert.rejects(pending, error => abort ? error === reason : error.code === objects.ErrorCode.UNAVAILABLE);
      if (abort) controller.abort(reason);
      else { for (const original of originals) original.close(); resume.resolve(); }
      await rejected;
      assert.equal(account.closed, account.opened);
    }
  }
});
