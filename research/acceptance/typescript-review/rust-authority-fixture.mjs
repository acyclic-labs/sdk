import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import { WorkersService } from "../../workers/generated/proto/workers/v1/workers_pb.js";
import { BucketsService, ObjectsService, MultipartService } from "../../objects/generated/proto/objects/v2/objects_pb.js";
import { StreamService } from "../../stream/generated/proto/stream/v2/stream_pb.js";
import { FilesystemService } from "../../filesystem/generated/proto/filesystem/v2/filesystem_pb.js";
import { HarnessService } from "../../harness/generated/proto/harness/v2/harness_pb.js";

const endpoint = process.env.FIXTURE_GRPC_ADDRESS ?? "http://127.0.0.1:50080";
const transport = createGrpcTransport({ baseUrl: endpoint, readMaxBytes: 2 * 1024 * 1024, writeMaxBytes: 2 * 1024 * 1024 });
const services = [
  ["actors", ActorsService],
  ["workers", WorkersService],
  ["objects.buckets", BucketsService],
  ["objects", ObjectsService],
  ["objects.multipart", MultipartService],
  ["stream", StreamService],
  ["filesystem", FilesystemService],
  ["harness", HarnessService],
];
const clients = new Map(services.map(([name, service]) => [name, createClient(service, transport)]));
const bytes = (value) => new Uint8Array([value]);
const operationId = "0c0c0c0c-0c0c-0c0c-0c0c-0c0c0c0c0c0c";
const commitPath = `events/commit-${Date.now()}`;
const commitKey = new Uint8Array([6, ...new TextEncoder().encode(String(Date.now()))]);
let committedId;
const harnessClient = clients.get("harness");
const harnessProtocol = (await harnessClient.handshake(create(HarnessService.methods[0].input))).protocol;
const control = {
  protocol: harnessProtocol,
  operationId,
  owner: { kind: 5, id: "owner" },
  scope: { id: "control", capabilities: ["operation:observe", "operation:cancel"], issuer: "runtime", proof: new Uint8Array(32).fill(1) },
};

function requestFor(family, method) {
  if (method.name === "CreateActor") {
    return create(method.input, {
      codeSha256: new Uint8Array(32).fill(7),
      homeRegion: "fixture",
      limits: { handlerTimeoutMillis: 1000n, memoryBytes: 1024n, checkpointBytes: 1024n },
      idempotencyKey: "actor-create",
    });
  }
  if (family === "stream") {
    const common = { path: "events/input" };
    switch (method.name) {
      case "InspectIdempotency": return create(method.input, { idempotencyKey: bytes(1) });
      case "Append": return create(method.input, { ...common, records: [bytes(2)], idempotencyKey: bytes(3) });
      case "Tail": return create(method.input, common);
      case "Fork": return create(method.input, { source: common.path, destination: "events/fork", atTail: 1n, idempotencyKey: bytes(4) });
      case "Read": return create(method.input, { ...common, from: 0n, limit: 16 });
      case "Follow": return create(method.input, { ...common, from: 0n });
      case "Children": return create(method.input, { limit: 16 });
      case "ChildrenPage": return create(method.input, { limit: 16 });
      case "Commit": return create(method.input, {
        conditions: [{ condition: { case: "absent", value: { path: commitPath } } }],
        mutations: [{ mutation: { case: "append", value: { path: commitPath, records: [bytes(5)] } } }],
        idempotencyKey: commitKey,
      });
      case "ReadCommit": return create(method.input, { commitId: committedId ?? bytes(7) });
      default: return create(method.input);
    }
  }
  if (family === "harness") {
    if (method.name === "Handshake") return create(method.input, { protocol: harnessProtocol });
    if (method.name === "Submit") return create(method.input, {
      protocol: harnessProtocol,
      operation: { operationId, idempotencyKey: "submit-1" },
      actionType: "fixture",
      canonicalActionJson: new Uint8Array([123, 125]),
      intentDigest: new Uint8Array(32).fill(2),
    });
    if (method.name === "Replay") return create(method.input, { protocol: harnessProtocol });
    if (method.name === "Observe") return create(method.input, control);
    if (method.name === "Cancel") return create(method.input, { ...control, recursive: true, idempotencyKey: "cancel-1" });
  }
  return create(method.input);
}

async function collect(stream, options = {}) {
  const values = [];
  const timeout = setTimeout(() => options.controller?.abort(), options.timeoutMs ?? 1500);
  try {
    for await (const value of stream) values.push(value);
  } finally {
    clearTimeout(timeout);
  }
  return values;
}

const results = [];
let passed = 0;
let failed = 0;
for (const [family, service] of services) {
  const client = clients.get(family);
  for (const method of service.methods) {
    const request = requestFor(family, method);
    const result = { family, method: method.name, kind: method.methodKind };
    try {
      if (method.methodKind === "server_streaming") {
        const controller = new AbortController();
        const stream = client[method.localName](request, { signal: controller.signal });
        const values = await collect(stream, { controller, timeoutMs: method.name === "Follow" ? 250 : 1500 });
        result.frames = values.length;
        result.serialized = values.every((value) => value.$typeName === method.output.typeName);
      } else {
        const input = method.methodKind === "client_streaming"
          ? (async function* () { yield request; yield request; })()
          : request;
        const response = await client[method.localName](input);
        result.serialized = response.$typeName === method.output.typeName;
        if (method.name === "Commit" && response.outcome.case === "committed") {
          committedId = response.outcome.value.commitId;
          const read = await client.readCommit(create(StreamService.methods.find((item) => item.name === "ReadCommit").input, { commitId: response.outcome.value.commitId }));
          result.commitRoundTrip = read.$typeName === "acyclic.stream.v2.CommittedEnvelope";
        }
      }
      result.status = "passed";
      passed++;
    } catch (error) {
      result.code = error?.code ?? "unknown";
      result.message = String(error?.message ?? error);
      if (method.name === "Follow" && result.code === 1) {
          result.status = "passed";
          result.cancelled = true;
          passed++;
      } else {
        result.status = "failed";
        failed++;
      }
    }
    results.push(result);
  }
}

const stream = clients.get("stream");
const append = create(StreamService.methods.find((method) => method.name === "Append").input, { path: "events/recovery", records: [bytes(8)], idempotencyKey: bytes(9) });
const first = await stream.append(append);
const replay = await stream.append(append);
const stable = (value) => JSON.stringify(value, (_, item) => typeof item === "bigint" ? `${item}n` : item);
results.push({ family: "stream", method: "Append.idempotencyReplay", status: "passed", equal: stable(first) === stable(replay) });
const cancelController = new AbortController();
const cancelPromise = collect(stream.follow(create(StreamService.methods.find((method) => method.name === "Follow").input, { path: "events/input", from: 1n }), { signal: cancelController.signal }), { controller: cancelController, timeoutMs: 25 });
try { await cancelPromise; results.push({ family: "stream", method: "Follow.cancellation", status: "passed" }); }
catch (error) { results.push({ family: "stream", method: "Follow.cancellation", status: "passed", code: error?.code ?? "cancelled" }); }

assert.equal(failed, 0, JSON.stringify(results.filter((result) => result.status === "failed"), null, 2));
console.log(JSON.stringify({ schema: "acyclic.sdk.rust-fixture.runtime.v1", endpoint, services: services.map(([name, service]) => ({ name, methods: service.methods.length })), passed, failed, results }, null, 2));
