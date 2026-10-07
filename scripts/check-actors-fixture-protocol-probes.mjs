import assert from "node:assert/strict";
import net from "node:net";
import tls from "node:tls";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { ActorsService } from "../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";
import { startConformanceFixture } from "../typescript/packages/actors/test/grpc-conformance-fixture.mjs";

const require = createRequire(new URL("../typescript/packages/actors/package.json", import.meta.url));
const { createGrpcTransport } = await import(pathToFileURL(require.resolve("@connectrpc/connect-node")));
const { createClient } = await import(pathToFileURL(require.resolve("@connectrpc/connect")));

const fixture = await startConformanceFixture();
try {
  const endpoint = new URL(fixture.options.endpoint);
  await new Promise(resolve => {
    const socket = net.connect(Number(endpoint.port), endpoint.hostname);
    socket.on("connect", () => socket.end("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"));
    socket.on("error", () => {});
    socket.on("close", resolve);
    socket.resume();
  });
  await new Promise(resolve => {
    const socket = tls.connect({
      host: endpoint.hostname, port: Number(endpoint.port),
      ca: fixture.options.caCertificate, ALPNProtocols: ["http/1.1"],
    });
    socket.on("error", () => {});
    socket.on("close", resolve);
  });
  const client = createClient(ActorsService, createGrpcTransport({
    baseUrl: fixture.options.endpoint,
    nodeOptions: { ca: fixture.options.caCertificate },
  }));
  const response = await client.inspectActor({ actorId: "actor-a" }, {
    headers: { authorization: `Bearer ${fixture.options.token}` }, timeoutMs: 5000,
  });
  assert.equal(response.actor?.actorId, "actor-a");
  console.log("ACTORS_FIXTURE_PROTOCOL_PROBES_PASS");
} finally {
  await fixture.close();
}
