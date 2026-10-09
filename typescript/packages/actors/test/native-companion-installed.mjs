// Qualify the ordinary installed neutral parent and host-selected companion.
import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { readFile } from "node:fs/promises";
import { fromBinary } from "@bufbuild/protobuf";
import { ActorsClient } from "@acyclic-labs/actors";
import { InspectActorRequestSchema } from "@acyclic-labs/actors/proto";

const identity = JSON.parse(await readFile(process.env.ACTORS_TLS_IDENTITY, "utf8"));
const server = createSecureServer({ key: identity.key, cert: identity.certificate });
const sockets = new Set();
let dispatched = 0, failure;
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
      response.addTrailers({ "grpc-status": "7", "grpc-message": "qualified-native" });
      response.end();
    } catch (error) { failure = error; response.destroy(error); }
  });
});
try {
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  const client = new ActorsClient({ endpoint: `https://localhost:${server.address().port}`, token: "conformance", caCertificate: Buffer.from(identity.certificate) });
  assert.equal(await client.transport, "grpc", "ordinary Node/Bun must select the installed native companion");
  await assert.rejects(client.inspectActor({ actorId: "native-companion" }, { signal: AbortSignal.timeout(10_000) }), error => error.code === "permission_denied" && error.message === "qualified-native");
  assert.ifError(failure);
  assert.equal(dispatched, 1);
  console.log(JSON.stringify({ status: "passed", transport: "grpc", authenticatedDecodedRequests: dispatched }));
} finally {
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
}
