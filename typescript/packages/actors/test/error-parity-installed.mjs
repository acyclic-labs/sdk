// Run against an assembled, installed package with ACTORS_TLS_IDENTITY set.
import assert from "node:assert/strict";
import { readFile, rename } from "node:fs/promises";
import { createSecureServer } from "node:http2";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const mode = process.env.ACTORS_ERROR_MODE ?? "native";
assert.ok(mode === "native" || mode === "wasm");
const actorsRoot = join(resolve(process.env.ACTORS_INSTALLED_ROOT ?? process.cwd()), "node_modules/@acyclic-labs/actors");
const nativeRoot = join(actorsRoot, "generated/native");
const movedNative = `${nativeRoot}.error-parity-${process.pid}`;
const identity = JSON.parse(await readFile(process.env.ACTORS_TLS_IDENTITY, "utf8"));
const server = createSecureServer({ key: identity.key, cert: identity.certificate });
const sockets = new Set();
let requests = 0;
server.on("connection", socket => {
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
  socket.on("error", () => {});
});
server.on("stream", stream => { requests += 1; stream.close(); });
await new Promise(resolve => server.listen(0, "localhost", resolve));
if (mode === "wasm") await rename(nativeRoot, movedNative);

async function invalidArgument(action) {
  await assert.rejects(action, error => {
    // Check the public field directly: parsing JSON from message masks ABI gaps.
    assert.equal(error.code, "invalid_argument");
    return true;
  });
}

try {
  const { ActorsClient } = await import("@acyclic-labs/actors");
  const endpoint = `https://localhost:${server.address().port}`;
  const options = { endpoint, token: "conformance", caCertificate: Buffer.from(identity.certificate) };
  // Browser transport permits loopback HTTP for tests; non-loopback HTTP is invalid on both targets.
  await invalidArgument(() => new ActorsClient({ ...options, endpoint: "http://actors.example" }).transport);
  await invalidArgument(() => new ActorsClient({ ...options, token: "bad\nheader" }).transport);
  const client = new ActorsClient(options);
  assert.equal(await client.transport, mode === "native" ? "grpc" : "grpc-web");
  for (const operation of ["createActor", "updateActor", "inspectActor", "addSubscription", "removeSubscription", "resumeSubscription", "checkpointActor", "invokeActor"]) {
    await invalidArgument(() => client[operation]({}));
  }
  if (mode === "native") {
    const binding = await import(pathToFileURL(join(nativeRoot, "binding.cjs")).href);
    const Native = binding.NativeActorsClient ?? binding.default.NativeActorsClient;
    const result = await Native.connectWithCaResult(endpoint, "conformance", Buffer.from(identity.certificate));
    assert.ok(result.client);
    const malformed = await result.client.createActorResult(Buffer.from([255]));
    assert.equal(malformed.error.code, "invalid_argument");
  } else {
    const binding = await import(pathToFileURL(join(actorsRoot, "generated/wasm/acyclic_actors_wasm.js")).href);
    const inner = await binding.ActorsClient.connect(endpoint, "conformance");
    await invalidArgument(() => inner.create_actor(Uint8Array.of(255)));
    inner.free();
  }
  assert.equal(requests, 0, "invalid requests must be rejected before RPC dispatch");
  console.log(JSON.stringify({ status: "passed", mode, configErrors: 2, semanticOperations: 8, malformedWire: 1, requests }));
} finally {
  if (mode === "wasm") await rename(movedNative, nativeRoot);
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
}
