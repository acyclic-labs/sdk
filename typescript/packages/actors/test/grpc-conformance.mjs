import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { createActorsGrpcClient } from "../dist/grpc.js";
import { createWorkersGrpcClient } from "../../workers/dist/grpc.js";
import { createStreamGrpcClient } from "../../stream/dist/grpc.js";
import { createObjectsV2GrpcClients } from "../../objects/dist/v2-grpc.js";
import { HttpActorsClient } from "../dist/http.js";
import { HttpWorkersClient } from "../../workers/dist/http.js";
import { expected, services, startConformanceFixture } from "./grpc-conformance-fixture.mjs";

const file = fileURLToPath(import.meta.url);
const root = fileURLToPath(new URL("../../../../", import.meta.url));

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
      if (method.name === "InvokeActor") initializer.headers = [{ name: "content-type", value: "application/json" }];
      if (method.name === "SelectDeployment") initializer.expectedRevision = 7n;
      if (method.name === "InvokeVersion") initializer.versionSha256 = new Uint8Array(32).fill(1);
      if (method.name === "InvokeDeployment") initializer.alias = "current";
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

const fixture = await startConformanceFixture();
try {
  for (const runtime of [process.execPath, "bun"]) {
    await new Promise((resolve, reject) => {
      const child = spawn(runtime, [file, "--client"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
      child.stdin.end(JSON.stringify(fixture.options));
      child.on("error", reject);
      child.on("exit", code => code === 0 ? resolve() : reject(new Error(`${runtime} conformance exited ${code}`)));
    });
  }
  await new Promise((resolve, reject) => {
    const child = spawn("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "transport-conformance"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
    child.stdin.end(JSON.stringify(fixture.options));
    child.on("error", reject);
    child.on("exit", code => code === 0 ? resolve() : reject(new Error(`Rust conformance exited ${code}`)));
  });
  assert.equal(fixture.seen.size, expected);
  for (const [method, calls] of fixture.seen) assert.equal(calls, method.includes(".actors.") || method.includes(".workers.") ? 3 : 2, method);
  assert.equal(fixture.httpSeen.size, 15);
  for (const [method, calls] of fixture.httpSeen) assert.equal(calls, 3, method);
} finally {
  await fixture.close();
}
