// Qualify the ordinary installed neutral parent and host-selected companion.
import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { readFile } from "node:fs/promises";
import { fromBinary } from "@bufbuild/protobuf";
import { ActorsClient } from "@acyclic-labs/actors";
import { InspectActorRequestSchema } from "@acyclic-labs/actors/proto";
import { CredentialCache, CredentialSnapshot } from "@acyclic-labs/auth/credential";

const identity = JSON.parse(await readFile(process.env.ACTORS_TLS_IDENTITY, "utf8"));
const server = createSecureServer({ key: identity.key, cert: identity.certificate });
const sockets = new Set();
let dispatched = 0, failure, grpcStatus = "7";
server.on("connection", socket => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
server.on("request", (request, response) => {
  const chunks = [];
  request.on("data", chunk => chunks.push(chunk));
  request.on("end", () => {
    try {
      assert.equal(request.headers.authorization, "Bearer conformance");
      assert.equal(request.url, "/acyclic.actors.v1.ActorsService/InspectActor");
      const frame = Buffer.concat(chunks);
      assert.equal(frame[0], 0);
      assert.equal(frame.readUInt32BE(1), frame.length - 5);
      assert.equal(fromBinary(InspectActorRequestSchema, frame.subarray(5)).actorId, "native-companion");
      dispatched++;
      response.writeHead(200, { "content-type": "application/grpc" });
      response.addTrailers({ "grpc-status": grpcStatus, "grpc-message": "qualified-native" });
      response.end();
    } catch (error) { failure = error; response.destroy(error); }
  });
});
try {
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  let refreshes = 0;
  const cache = new CredentialCache(async () => {
    refreshes++;
    return new CredentialSnapshot(refreshes === 1 ? "conformance" : "replacement", "source", 100);
  }, () => ({ revision: "source", nowUnixSeconds: 1 }));
  const client = new ActorsClient({ endpoint: `https://localhost:${server.address().port}`, credential: cache.provider, caCertificate: Buffer.from(identity.certificate) });
  assert.equal(await client.transport, "grpc", "ordinary Node/Bun must select the installed native companion");
  const original = await cache.resolve();
  await assert.rejects(client.inspectActor({ actorId: "native-companion" }), error => error.code === "permission_denied" && error.message === "qualified-native" && error.metadata.credentialRejected === false);
  assert.equal(await cache.resolve(), original, "policy denial preserves the exact lease");
  grpcStatus = "16";
  await assert.rejects(client.inspectActor({ actorId: "native-companion" }), error => error.code === "unauthenticated" && error.metadata.credentialRejected === true);
  assert.equal((await cache.resolve()).token(), "replacement", "native rejection reaches the shared cache");
  assert.equal(refreshes, 2);
  assert.ifError(failure);
  assert.equal(dispatched, 2);
  console.log(JSON.stringify({ status: "passed", transport: "grpc", authenticatedDecodedRequests: dispatched }));
} finally {
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
}
