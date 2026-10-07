import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { createServer } from "node:http";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { create, fromJsonString, toJsonString } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import { HTTP_ROUTES as actorRoutes } from "../dist/routes.js";

export const CONFORMANCE_TOKEN = "conformance";
export const CONFORMANCE_ACTOR_ID = "actor-a";
export const CONFORMANCE_ACTOR_OBSERVATION = Object.freeze({
  actorId: CONFORMANCE_ACTOR_ID,
  codeSha256: new Uint8Array(32).fill(1),
  homeRegion: "eu",
  state: 1,
  subscriptions: [],
  checkpointEpoch: 9n,
  configurationRevision: 0n,
});

export const services = [ActorsService];
export const expected = services.reduce((count, service) => count + service.methods.length, 0);
const root = fileURLToPath(new URL("../../../../", import.meta.url));

export function responseInitializer(method) {
  if (method.name === "InvokeActor") return { status: 201, headers: [{ name: "location", value: "/result" }] };
  if (["CreateActor", "UpdateActor", "InspectActor", "AddSubscription", "RemoveSubscription", "ResumeSubscription", "CheckpointActor"].includes(method.name)) {
    return { actor: { ...CONFORMANCE_ACTOR_OBSERVATION } };
  }
  if (method.name === "InvokeVersion") return { resolvedSha256: new Uint8Array(32).fill(1) };
  if (method.name === "InvokeDeployment") return { resolvedSha256: new Uint8Array(32).fill(2), resolvedRevision: 8n };
  if (method.name === "SubmitJob") return { job: { jobId: "job-a", state: 1, resolvedSha256: new Uint8Array(32).fill(7) } };
  if (method.name === "InspectJob") return { job: { jobId: "job-a", state: 3, resolvedSha256: new Uint8Array(32).fill(7), result: { body: new Uint8Array([3, 4]) } } };
  return {};
}

export function inspectActorRequest(method, request) {
  if (method.name === "SubmitJob") {
    assert.equal(request.input.source.case, "object");
    assert.deepEqual([request.input.source.value.bucket, request.input.source.value.key], ["customer-input", "video/input.mp4"]);
    assert.equal(request.limits.outputBytes, 1024n);
  }
  if (method.name === "InvokeActor") assert.equal(request.headers[0].value, "application/json");
  if (method.name === "AddSubscription") {
    assert.equal(request.actorId, CONFORMANCE_ACTOR_ID);
    assert.equal(request.subscription.streamPath, "events/input");
    assert.equal(request.subscription.start.start.value, 9007199254740993n);
  }
  if (method.name === "CheckpointActor") {
    assert.equal(request.actorId, CONFORMANCE_ACTOR_ID);
    assert.equal(request.idempotencyKey, "checkpoint-a");
  }
}

/**
 * Start the one canonical local Actors/Workers fixture used by conformance.
 * The returned options object is safe to pass to Rust, Node, Bun, Python, or
 * JVM consumers; close() owns both listeners and is idempotent.
 */
export async function startConformanceFixture({ onUnaryRequest } = {}) {
  const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
  assert.equal(generated.status, 0, generated.stderr);
  const identity = JSON.parse(generated.stdout);
  const seen = new Map();
  const httpSeen = new Map();
  const httpServer = createServer(async (request, response) => {
    try {
      let body = "";
      for await (const chunk of request) body += chunk;
      if (request.headers.authorization !== `Bearer ${CONFORMANCE_TOKEN}`) {
        response.writeHead(401, { "content-type": "application/json" });
        response.end(JSON.stringify({ code: "ERROR_CODE_CAPABILITY_DENIED", message: "missing bearer" }));
        return;
      }
      for (const service of services) {
        const routes = actorRoutes;
        for (const method of service.methods) {
          const path = "/" + routes[method.localName].replace("{sha256hex}", "01".repeat(32)).replace("{alias}", "current");
          if (request.url !== path) continue;
          const input = fromJsonString(method.input, body);
          if (method.name === "InspectActor" && input.actorId === "oversize") {
            response.end(" ".repeat(64));
            return;
          }
          httpSeen.set(method.name, (httpSeen.get(method.name) ?? 0) + 1);
          inspectActorRequest(method, input);
          if (method.name === "SelectDeployment") assert.equal(input.expectedRevision, 7n);
          response.writeHead(200, { "content-type": "application/json" });
          response.end(toJsonString(method.output, create(method.output, responseInitializer(method))));
          return;
        }
      }
      response.writeHead(404).end();
    } catch (error) {
      response.writeHead(500).end(String(error));
    }
  });
  const adapter = connectNodeAdapter({
    routes(router) {
      for (const service of services) {
        const routes = actorRoutes;
        const implementation = {};
        for (const method of service.methods) {
          const inspect = (request, context) => {
            if (context.requestHeader.get("authorization") !== `Bearer ${CONFORMANCE_TOKEN}`) throw new ConnectError("missing bearer", Code.Unauthenticated);
            seen.set(`${service.typeName}/${method.name}`, (seen.get(`${service.typeName}/${method.name}`) ?? 0) + 1);
            inspectActorRequest(method, request);
            if (method.name === "SelectDeployment") assert.equal(request.expectedRevision, 7n);
            if (method.name === "Commit") assert.deepEqual(request.mutations.map(item => item.mutation.value.path), ["a", "b"]);
          };
          if (method.methodKind === "server_streaming") {
            implementation[method.localName] = async function* (request, context) {
              inspect(request, context);
              yield create(method.output);
              yield create(method.output);
            };
          } else {
            implementation[method.localName] = async (request, context) => {
              if (method.methodKind === "client_streaming") {
                let frames = 0;
                for await (const frame of request) { if (frames === 0) inspect(frame, context); frames++; }
                assert.equal(frames, 2, `${method.name} request stream`);
              } else {
                inspect(request, context);
                await onUnaryRequest?.(method, request, context);
              }
              return create(method.output, responseInitializer(method));
            };
          }
        }
        router.service(service, implementation);
      }
    },
  });
  const server = createSecureServer({ key: identity.key, cert: identity.certificate }, adapter);
  const handleExpectedConnectionReset = error => {
    if (error?.code === "ECONNRESET") return;
    if (error?.code === "ERR_HTTP2_ERROR" && error?.message === "Protocol error") return;
    // A failed protocol probe closes its connection, not the shared fixture.
    if (error?.code === "ERR_SSL_HTTP_REQUEST" || error?.code === "ERR_SSL_NO_APPLICATION_PROTOCOL") return;
    throw error;
  };
  server.on("connection", socket => socket.on("error", handleExpectedConnectionReset));
  server.on("session", session => session.on("error", handleExpectedConnectionReset));
  server.on("sessionError", handleExpectedConnectionReset);
  server.on("tlsClientError", handleExpectedConnectionReset);
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  await new Promise(resolve => httpServer.listen(0, "localhost", resolve));
  const options = {
    endpoint: `https://localhost:${server.address().port}`,
    httpEndpoint: `http://localhost:${httpServer.address().port}`,
    token: CONFORMANCE_TOKEN,
    caCertificate: identity.certificate,
    actorId: CONFORMANCE_ACTOR_ID,
    expectedObservation: {
      actorId: CONFORMANCE_ACTOR_OBSERVATION.actorId,
      codeSha256: Array.from(CONFORMANCE_ACTOR_OBSERVATION.codeSha256),
      homeRegion: CONFORMANCE_ACTOR_OBSERVATION.homeRegion,
      state: CONFORMANCE_ACTOR_OBSERVATION.state,
      subscriptions: [],
      checkpointEpoch: Number(CONFORMANCE_ACTOR_OBSERVATION.checkpointEpoch),
      configurationRevision: Number(CONFORMANCE_ACTOR_OBSERVATION.configurationRevision),
    },
  };
  let closed = false;
  return {
    identity,
    options,
    seen,
    httpSeen,
    async close() {
      if (closed) return;
      closed = true;
      await new Promise(resolve => httpServer.close(resolve));
      await new Promise(resolve => server.close(resolve));
    },
  };
}


