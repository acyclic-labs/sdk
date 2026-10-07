import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";

const root = process.env.ACYCLIC_SDK_ROOT;
if (!root) throw new Error("ACYCLIC_SDK_ROOT must name the SDK checkout");
process.stderr.write("actors fixture: loading generated Actors service\n");
const actors = await import(pathToFileURL(`${root}/typescript/packages/actors/generated/proto/actors/v1/actors_pb.js`));
const { ActorsService } = actors;
export const CONFORMANCE_TOKEN = "conformance";

process.stderr.write("actors fixture: generating certificate\n");
const certificate = spawnSync(`${root}/target/debug/examples/conformance-certificate.exe`, [], { encoding: "utf8" });
assert.equal(certificate.status, 0, certificate.stderr);
const identity = JSON.parse(certificate.stdout);
process.stderr.write("actors fixture: certificate ready\n");
const observation = {
  actorId: "actor-a",
  codeSha256: new Uint8Array(32).fill(1),
  homeRegion: "eu",
  state: 1,
  subscriptions: [{
    subscriptionId: "events",
    streamPath: "events/input",
    state: 1,
    deliveredCursor: 9007199254740993n,
    completedCursor: 9007199254740994n,
    recoverableCursor: 9007199254740995n,
    placementAnchor: true,
    retryCount: 2,
    failureCode: "none",
    failedCursor: 9007199254740996n,
  }],
  checkpointEpoch: 9n,
  configurationRevision: 0n,
};

export async function startConformanceFixture({ onUnaryRequest } = {}) {
  const adapter = connectNodeAdapter({
    routes(router) {
      const implementation = {};
      for (const method of ActorsService.methods) {
        implementation[method.localName] = async (request, context) => {
          if (context.requestHeader.get("authorization") !== `Bearer ${CONFORMANCE_TOKEN}`) {
            throw new ConnectError("missing bearer", Code.Unauthenticated);
          }
          if (method.name === "CreateActor") {
            assert.equal(request.codeSha256.length, 32);
            assert.equal(request.codeSha256[0], 1);
            assert.equal(request.homeRegion, "eu");
            assert.equal(request.bindings[0].name, "b");
            assert.equal(request.bindings[0].capability, "cap");
            assert.equal(request.bindings[0].resource, "res");
            assert.equal(request.limits.handlerTimeoutMillis, 10n);
            assert.equal(request.limits.memoryBytes, 20n);
            assert.equal(request.limits.checkpointBytes, 30n);
            assert.equal(request.subscriptions[0].subscriptionId, "events");
            assert.equal(request.subscriptions[0].streamPath, "events/input");
            assert.equal(request.subscriptions[0].start.start.value, 9007199254740993n);
            assert.equal(request.idempotencyKey, "create-idem");
          }
          if (method.name === "UpdateActor") {
            assert.equal(request.actorId, "actor-a");
            assert.equal(request.codeSha256[0], 1);
            assert.equal(request.bindings[0].name, "b");
            assert.equal(request.limits.handlerTimeoutMillis, 10n);
            assert.equal(request.expectedConfigurationRevision, 0n);
            assert.equal(request.idempotencyKey, "update-idem");
          }
          if (method.name === "InspectActor") assert.equal(request.actorId, "actor-a");
          if (method.name === "AddSubscription") {
            assert.equal(request.actorId, "actor-a");
            assert.equal(request.subscription.subscriptionId, "events");
            assert.equal(request.subscription.streamPath, "events/input");
            assert.equal(request.subscription.start.start.value, 9007199254740993n);
            assert.equal(request.idempotencyKey, "add-idem");
          }
          if (method.name === "RemoveSubscription") {
            assert.equal(request.actorId, "actor-a");
            assert.equal(request.subscriptionId, "events/input");
            assert.equal(request.idempotencyKey, "remove-idem");
          }
          if (method.name === "ResumeSubscription") {
            assert.equal(request.actorId, "actor-a");
            assert.equal(request.subscriptionId, "events/input");
            assert.equal(request.idempotencyKey, "resume-idem");
          }
          if (method.name === "CheckpointActor") {
            assert.equal(request.actorId, "actor-a");
            assert.equal(request.idempotencyKey, "checkpoint-a");
          }
          if (method.name === "InvokeActor") {
            assert.equal(request.actorId, "actor-a");
            assert.equal(request.method, "POST");
            assert.equal(request.url, "/");
            assert.deepEqual(Array.from(request.body), [0, 255]);
            assert.equal(request.headers[0].name, "content-type");
            assert.equal(request.headers[0].value, "application/json");
          }
          await onUnaryRequest?.(method, request, context);
          if (method.name === "InvokeActor") return create(method.output, { status: 201, headers: [{ name: "location", value: "/result" }] });
          return create(method.output, { actor: { ...observation } });
        };
      }
      router.service(ActorsService, implementation);
    },
  });
  const server = createSecureServer({ key: identity.key, cert: identity.certificate }, adapter);
  const ignoreExpectedReset = error => {
    if (error?.code === "ECONNRESET" || error?.code === "ERR_HTTP2_ERROR" || error?.code === "ERR_SSL_HTTP_REQUEST" || error?.code === "ERR_SSL_NO_APPLICATION_PROTOCOL") return;
    throw error;
  };
  server.on("connection", socket => socket.on("error", ignoreExpectedReset));
  server.on("session", session => session.on("error", ignoreExpectedReset));
  server.on("sessionError", ignoreExpectedReset);
  server.on("tlsClientError", ignoreExpectedReset);
  process.stderr.write("actors fixture: listening\n");
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  process.stderr.write("actors fixture: listening ready\n");
  const options = {
    endpoint: `https://localhost:${server.address().port}`,
    token: CONFORMANCE_TOKEN,
    caCertificate: identity.certificate,
  };
  let closed = false;
  return {
    options,
    async close() {
      if (closed) return;
      closed = true;
      await new Promise(resolve => server.close(resolve));
    },
  };
}

