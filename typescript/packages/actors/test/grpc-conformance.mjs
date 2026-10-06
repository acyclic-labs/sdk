import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { createServer } from "node:http";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { create, fromJsonString, toJsonString } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../../workers/generated/proto/workers/v1/workers_pb.js";
import { StreamService } from "../../stream/generated/proto/stream/v2/stream_pb.js";
import { BucketsService, ObjectsService, MultipartService } from "../../objects/generated/proto/objects/v2/objects_pb.js";
import { createActorsGrpcClient } from "../dist/grpc.js";
import { createWorkersGrpcClient } from "../../workers/dist/grpc.js";
import { createStreamGrpcClient } from "../../stream/dist/grpc.js";
import { createObjectsV2GrpcClients } from "../../objects/dist/v2-grpc.js";
import { HttpActorsClient } from "../dist/http.js";
import { HttpWorkersClient } from "../../workers/dist/http.js";
import { ACTORS_METHODS as actorRoutes } from "../dist/generated-client.js";
import { WORKERS_METHODS as workerRoutes } from "../../workers/dist/generated-client.js";

const services = [ActorsService, WorkersService, StreamService, BucketsService, ObjectsService, MultipartService];
const expected = services.reduce((count, service) => count + service.methods.length, 0);
const file = fileURLToPath(import.meta.url);
const root = fileURLToPath(new URL("../../../../", import.meta.url));

function responseInitializer(method) {
  if (method.name === "InvokeActor") return { status: 201, headers: [{ name: "location", value: "/result" }] };
  if (method.name === "CheckpointActor") return { actor: { actorId: "actor-a", checkpointEpoch: 9n } };
  if (method.name === "InvokeVersion") return { resolvedSha256: new Uint8Array(32).fill(1) };
  if (method.name === "InvokeDeployment") return { resolvedSha256: new Uint8Array(32).fill(2), resolvedRevision: 8n };
  if (method.name === "SubmitJob") return { job: { jobId: "job-a", state: 1, resolvedSha256: new Uint8Array(32).fill(7) } };
  if (method.name === "InspectJob") return { job: { jobId: "job-a", state: 3, resolvedSha256: new Uint8Array(32).fill(7), result: { body: new Uint8Array([3, 4]) } } };
  return {};
}

function inspectActorRequest(method, request) {
  if (method.name === "SubmitJob") {
    assert.equal(request.input.source.case, "object");
    assert.deepEqual([request.input.source.value.bucket, request.input.source.value.key], ["customer-input", "video/input.mp4"]);
    assert.equal(request.limits.outputBytes, 1024n);
  }
  if (method.name === "InvokeActor") assert.equal(request.headers[0].value, "application/json");
  if (method.name === "AddSubscription") {
    assert.equal(request.actorId, "actor-a");
    assert.equal(request.subscription.streamPath, "events/input");
    assert.equal(request.subscription.start.start.value, 9007199254740993n);
  }
  if (method.name === "CheckpointActor") {
    assert.equal(request.actorId, "actor-a");
    assert.equal(request.idempotencyKey, "checkpoint-a");
  }
}

function inspectResponse(method, response) {
  if (method.name === "SubmitJob") assert.deepEqual(response.job.resolvedSha256, new Uint8Array(32).fill(7));
  if (method.name === "InspectJob") assert.deepEqual(response.job.result.body, new Uint8Array([3, 4]));
  if (method.name === "InvokeActor") assert.equal(response.headers[0].name, "location");
  if (method.name === "CheckpointActor") assert.equal(response.actor.checkpointEpoch, 9n);
  if (method.name === "InvokeVersion") {
    assert.deepEqual(response.resolvedSha256, new Uint8Array(32).fill(1));
    assert.equal(response.resolvedRevision, undefined);
  }
  if (method.name === "InvokeDeployment") {
    assert.deepEqual(response.resolvedSha256, new Uint8Array(32).fill(2));
    assert.equal(response.resolvedRevision, 8n);
  }
}

if (process.argv.includes("--client")) {
  let configText = "";
  for await (const chunk of process.stdin) configText += chunk;
  const options = JSON.parse(configText);
  const objects = createObjectsV2GrpcClients(options);
  const clients = [createActorsGrpcClient(options), createWorkersGrpcClient(options), createStreamGrpcClient(options), objects.buckets, objects.objects, objects.multipart];
  let count = 0;
  for (const [index, service] of services.entries()) {
    for (const method of service.methods) {
      const initializer = {};
      if (method.name === "InvokeActor") Object.assign(initializer, {
        actorId: "actor-a", method: "GET", url: "https://example.test/",
        headers: [{ name: "content-type", value: "application/json" }],
      });
      if (method.name === "SelectDeployment") initializer.expectedRevision = 7n;
      if (method.name === "InvokeVersion") Object.assign(initializer, { versionSha256: new Uint8Array(32).fill(1), method: "GET", url: "https://example.test/" });
      if (method.name === "InvokeDeployment") Object.assign(initializer, { alias: "current", method: "GET", url: "https://example.test/" });
      if (method.name === "AddSubscription") Object.assign(initializer, {
        actorId: "actor-a",
        subscription: { subscriptionId: "input", streamPath: "events/input", start: { start: { case: "cursor", value: 9007199254740993n } } },
        idempotencyKey: "subscribe-a",
      });
      if (method.name === "CheckpointActor") Object.assign(initializer, { actorId: "actor-a", idempotencyKey: "checkpoint-a" });
      if (method.name === "SubmitJob") Object.assign(initializer, {
        target: { target: { case: "deploymentAlias", value: "current" } },
        input: { source: { case: "object", value: { bucket: "customer-input", key: "video/input.mp4" } } },
        limits: { timeoutMillis: 1000n, memoryBytes: 1024n, outputBytes: 1024n },
        retry: { maxAttempts: 2 }, idempotencyKey: "job-a",
      });
      if (method.name === "Commit") initializer.mutations = [
        { mutation: { case: "append", value: { path: "a", records: [new Uint8Array([1])] } } },
        { mutation: { case: "append", value: { path: "b", records: [new Uint8Array([2])] } } },
      ];
      const request = create(method.input, initializer);
      if (method.methodKind === "server_streaming") {
        let frames = 0;
        for await (const response of clients[index][method.localName](request)) {
          assert.equal(response.$typeName, method.output.typeName);
          frames++;
        }
        assert.equal(frames, 2, `${method.name} response stream`);
      } else {
        const input = method.methodKind === "client_streaming" ? (async function* () { yield request; yield request; })() : request;
        const response = await clients[index][method.localName](input);
        assert.equal(response.$typeName, method.output.typeName);
        inspectResponse(method, response);
      }
      count++;
      if (index < 2) {
        const http = index === 0 ? new HttpActorsClient({ ...options, endpoint: options.httpEndpoint }) : new HttpWorkersClient({ ...options, endpoint: options.httpEndpoint });
        inspectResponse(method, await http[method.localName](request));
      }
    }
  }
  const denied = createActorsGrpcClient({ ...options, token: "wrong" });
  await assert.rejects(denied.inspectActor({ actorId: "a" }), error => error instanceof ConnectError && error.code === Code.Unauthenticated);
  assert.equal(count, expected);
  console.log(`${process.versions.bun ? "Bun" : "Node"}: ${count} authenticated gRPC methods, streaming and denied authentication passed`);
  process.exit(0);
}

const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
assert.equal(generated.status, 0, generated.stderr);
const identity = JSON.parse(generated.stdout);
const seen = new Map();
const httpSeen = new Map();
const httpServer = createServer(async (request, response) => {
  try {
    let body = "";
    for await (const chunk of request) body += chunk;
    if (request.headers.authorization !== "Bearer conformance") {
      response.writeHead(401, { "content-type": "application/json" });
      response.end(JSON.stringify({ code: "ERROR_CODE_CAPABILITY_DENIED", message: "missing bearer" }));
      return;
    }
    for (const [index, service] of services.slice(0, 2).entries()) {
      const routes = index === 0 ? actorRoutes : workerRoutes;
      for (const method of service.methods) {
        const path = "/" + routes[method.localName].path.replace("{sha256hex}", "01".repeat(32)).replace("{alias}", "current");
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
  } catch (error) { response.writeHead(500).end(String(error)); }
});
const adapter = connectNodeAdapter({
  routes(router) {
    for (const service of services) {
      const implementation = {};
      for (const method of service.methods) {
        const inspect = (request, context) => {
          if (context.requestHeader.get("authorization") !== "Bearer conformance") throw new ConnectError("missing bearer", Code.Unauthenticated);
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
            } else inspect(request, context);
            const response = create(method.output, responseInitializer(method));
            return response;
          };
        }
      }
      router.service(service, implementation);
    }
  },
});
const server = createSecureServer({ key: identity.key, cert: identity.certificate }, adapter);
await new Promise(resolve => server.listen(0, "localhost", resolve));
await new Promise(resolve => httpServer.listen(0, "localhost", resolve));
const options = { endpoint: `https://localhost:${server.address().port}`, httpEndpoint: `http://localhost:${httpServer.address().port}`, token: "conformance", caCertificate: identity.certificate };
try {
  for (const runtime of [process.execPath, "bun"]) {
    await new Promise((resolve, reject) => {
      const child = spawn(runtime, [file, "--client"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
      child.stdin.end(JSON.stringify(options));
      child.on("error", reject);
      child.on("exit", code => code === 0 ? resolve() : reject(new Error(`${runtime} conformance exited ${code}`)));
    });
  }
  await new Promise((resolve, reject) => {
    const child = spawn("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "transport-conformance"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
    child.stdin.end(JSON.stringify(options));
    child.on("error", reject);
    child.on("exit", code => code === 0 ? resolve() : reject(new Error(`Rust conformance exited ${code}`)));
  });
  assert.equal(seen.size, expected);
  for (const [method, calls] of seen) assert.equal(calls, method.includes(".actors.") || method.includes(".workers.") ? 3 : 2, method);
  assert.equal(httpSeen.size, 15);
  for (const [method, calls] of httpSeen) assert.equal(calls, 3, method);
} finally {
  await new Promise(resolve => httpServer.close(resolve));
  await new Promise(resolve => server.close(resolve));
}
