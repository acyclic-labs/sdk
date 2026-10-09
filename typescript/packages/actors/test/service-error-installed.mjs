// Run against a freshly assembled installed package; trust the public test CA at process startup.
import assert from "node:assert/strict";
import { readFile, rename } from "node:fs/promises";
import { createSecureServer } from "node:http2";
import { join, resolve } from "node:path";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

const mode = process.env.ACTORS_ERROR_MODE ?? "native";
assert.ok(mode === "native" || mode === "wasm");
const actorsRoot = join(resolve(process.env.ACTORS_INSTALLED_ROOT ?? process.cwd()), "node_modules/@acyclic-labs/actors");
const nativeRoot = join(actorsRoot, "generated/native");
const movedNative = `${nativeRoot}.service-errors-${process.pid}`;
const identity = JSON.parse(await readFile(process.env.ACTORS_TLS_IDENTITY, "utf8"));
const server = createSecureServer({ key: identity.key, cert: identity.certificate, allowHTTP1: true });
const sockets = new Set();
let moved = false;
let dispatched = 0;
let serverFailure;
const timeout = setTimeout(() => { throw new Error("service error parity timed out"); }, 30_000);
server.on("connection", socket => {
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
  socket.on("error", () => {});
});

try {
  if (mode === "wasm") { await rename(nativeRoot, movedNative); moved = true; }
  const { ActorsClient } = await import("@acyclic-labs/actors");
  const { ErrorSchema, InspectActorRequestSchema } = await import("@acyclic-labs/actors/proto");
  const cases = [
    ...[0, 2, 99].map(code => {
      const serviceMessage = `service diagnostic ${code} % \u2713`;
      return { code, serviceMessage, bytes: toBinary(ErrorSchema, create(ErrorSchema, { code, message: serviceMessage })) };
    }),
    { bytes: new Uint8Array() },
    { bytes: Uint8Array.of(255, 0, 128) },
  ];
  server.on("request", async (request, response) => {
    try {
      assert.equal(request.headers.authorization, "Bearer conformance");
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      assert.equal(body[0], 0);
      assert.equal(body.readUInt32BE(1), body.length - 5);
      const decoded = fromBinary(InspectActorRequestSchema, body.subarray(5));
      const index = Number(decoded.actorId.slice("case-".length));
      assert.equal(decoded.actorId, `case-${index}`);
      assert.equal(index, dispatched++);
      const original = `original grpc diagnostic ${index} % \u2713`;
      const trailers = { "grpc-status": "7", "grpc-message": encodeURIComponent(original) };
      if (cases[index].bytes.length) trailers["grpc-status-details-bin"] = Buffer.from(cases[index].bytes).toString("base64");
      if (mode === "native") {
        response.writeHead(200, { "content-type": "application/grpc" });
        response.addTrailers(trailers);
        response.end();
      } else {
        const payload = Buffer.from(Object.entries(trailers).map(([key, value]) => `${key}: ${value}\r\n`).join(""));
        const frame = Buffer.alloc(5);
        frame[0] = 128;
        frame.writeUInt32BE(payload.length, 1);
        response.writeHead(200, { "content-type": "application/grpc-web+proto", "access-control-allow-origin": "*" });
        response.end(Buffer.concat([frame, payload]));
      }
    } catch (error) {
      serverFailure = error;
      response.destroy(error);
    }
  });
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  const client = new ActorsClient({ endpoint: `https://localhost:${server.address().port}`, token: "conformance", caCertificate: Buffer.from(identity.certificate) });
  assert.equal(await client.transport, mode === "native" ? "grpc" : "grpc-web");
  for (const [index, expected] of cases.entries()) {
    const original = `original grpc diagnostic ${index} % \u2713`;
    await assert.rejects(() => client.inspectActor({ actorId: `case-${index}` }), error => {
      assert.equal(error.code, "permission_denied");
      assert.equal(error.message, original);
      const metadata = error.metadata;
      assert.ok(metadata);
      assert.equal(metadata.code, error.code);
      assert.equal(metadata.message, original);
      assert.equal(metadata.grpcCode, 7);
      assert.equal(metadata.grpcName, "permission_denied");
      assert.equal(metadata.serviceCode ?? undefined, expected.code);
      assert.equal(metadata.serviceMessage ?? undefined, expected.serviceMessage);
      assert.ok(metadata.rawDetails instanceof Uint8Array);
      assert.deepEqual(Array.from(metadata.rawDetails), Array.from(expected.bytes));
      return true;
    });
  }
  assert.ifError(serverFailure);
  assert.equal(dispatched, cases.length);
  console.log(JSON.stringify({ status: "passed", mode, authenticatedDecodedRequests: dispatched, decodedServiceCodes: [0, 2, 99], emptyAndMalformedDetails: true, originalMessageAndRawBytes: true }));
} finally {
  clearTimeout(timeout);
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
  if (moved) await rename(movedNative, nativeRoot);
}
