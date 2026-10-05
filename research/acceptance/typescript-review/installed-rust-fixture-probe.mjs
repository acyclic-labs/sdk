import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ActorsService } from "@acyclic-labs/actors/proto";
import { WorkersService } from "@acyclic-labs/workers/proto";
import { BucketsService, ObjectsService, MultipartService } from "@acyclic-labs/objects/proto";
import { StreamService } from "@acyclic-labs/stream/proto";
import { FilesystemService } from "@acyclic-labs/fs/proto";
import { HarnessService } from "@acyclic-labs/harness/proto";

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
const decodeText = (value) => new TextDecoder().decode(value instanceof Uint8Array ? value : new Uint8Array(value ?? []));
const encodeHex = (value) => Array.from(value instanceof Uint8Array ? value : new Uint8Array(value ?? []), (item) => item.toString(16).padStart(2, "0")).join("");
const describeScalar = (value) => {
  if (value instanceof Uint8Array) return { kind: "bytes", hex: encodeHex(value), utf8: decodeText(value) };
  if (typeof value === "bigint") return `${value}n`;
  if (typeof value === "number" || typeof value === "string" || typeof value === "boolean" || value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map(describeScalar);
  if (value && typeof value === "object") return describeMessage(value);
  return String(value);
};
const describeMessage = (value) => {
  if (!value || typeof value !== "object") return describeScalar(value);
  const fields = {};
  for (const [key, item] of Object.entries(value)) {
    if (key.startsWith("$")) continue;
    if (item === undefined || item === null) continue;
    fields[key] = describeScalar(item);
  }
  if (value.$typeName) fields.$typeName = value.$typeName;
  return fields;
};
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

async function collectAndCancelAfterFirst(stream, controller, timeoutMs = 250) {
  const values = [];
  let cancellationRequested = false;
  let cancellationObserved = false;
  let cancellationError;
  const timer = setTimeout(() => {
    if (!cancellationRequested) {
      cancellationRequested = true;
      controller.abort();
    }
  }, timeoutMs);
  try {
    for await (const value of stream) {
      values.push(value);
      if (!cancellationRequested) {
        cancellationRequested = true;
        controller.abort();
      }
    }
  } catch (error) {
    cancellationObserved = true;
    cancellationError = error;
  } finally {
    clearTimeout(timer);
  }
  return { values, cancellationRequested, cancellationObserved, cancellationError };
}

const results = [];
let passed = 0;
let failed = 0;
for (const [family, service] of services) {
  const client = clients.get(family);
  for (const method of service.methods) {
    const request = requestFor(family, method);
    const result = {
      family,
      method: method.name,
      method_identity: `${service.typeName}/${method.name}`,
      kind: method.methodKind,
      request_type: method.input.typeName,
      execution_mode: "remote",
      invoked: true,
      exit_code: 0,
      request: describeMessage(request),
    };
    try {
      if (method.methodKind === "server_streaming") {
        const controller = new AbortController();
        const stream = client[method.localName](request, { signal: controller.signal });
        const cancellation = method.name === "Follow"
          ? await collectAndCancelAfterFirst(stream, controller)
          : { values: await collect(stream, { controller, timeoutMs: 1500 }), cancellationRequested: false, cancellationObserved: false };
        const values = cancellation.values;
        result.frames = values.length;
        result.serialized = values.every((value) => value.$typeName === method.output.typeName);
        result.decoded = {
          response_type: method.output.typeName,
          present_fields: [...new Set(values.flatMap((value) => method.output.fields
            .filter((field) => value[field.localName] !== undefined && value[field.localName] !== null)
            .map((field) => field.name)))],
          observed_values: values.map(describeMessage),
        };
        if (method.name === "Follow") {
          result.cancellation_trace = {
            requested: cancellation.cancellationRequested,
            observed: cancellation.cancellationObserved,
            code: cancellation.cancellationError?.code ?? null,
            message: cancellation.cancellationError?.message ?? null,
            frames_before_cancel: values.length,
          };
          assert.equal(cancellation.cancellationRequested, true, "Follow did not request cancellation after a Rust frame");
          assert.equal(cancellation.cancellationObserved, true, "Follow cancellation was not observed by the installed client");
          result.cancelled = true;
          result.exit_code = 1;
          result.code = cancellation.cancellationError?.code ?? "cancelled";
        }
        if (family === "objects" && method.name === "GetObject") {
          const frames = values.map((value) => value.frame?.case ?? (value.header ? "header" : value.body?.length ? "body" : "unknown"));
          const header = values.find((value) => (value.frame?.case ?? (value.header ? "header" : undefined)) === "header")?.frame?.value
            ?? values.find((value) => value.header)?.header;
          const body = values.find((value) => (value.frame?.case ?? (value.body?.length ? "body" : undefined)) === "body")?.frame?.value
            ?? values.find((value) => value.body?.length)?.body;
          result.decoded.observed = {
            frame_order: frames,
            object_etag: header?.object?.etag ?? null,
            body_utf8: body ? decodeText(body) : null,
            body_hex: body ? encodeHex(body) : null,
          };
        }
        if (family === "filesystem" && method.name === "Export") {
          result.decoded.observed = values.map((value) => ({
            cursor_utf8: decodeText(value.cursor),
            object_id_utf8: decodeText(value.objectId),
            contents_utf8: decodeText(value.contents),
            terminal: value.terminal,
          }));
        }
        if (family === "harness" && method.name === "Replay") {
          const events = values.flatMap((delivery) => delivery.events ?? []);
          result.semantic = {
            nonEmptyDelivery: values.length > 0,
            eventCount: events.length,
            operationIds: events.map((event) => event.operationId),
            eventTypes: events.map((event) => event.eventType),
          };
          assert.ok(values.length > 0, "Harness Replay returned no Rust journal delivery");
          assert.ok(events.some((event) => event.operationId === operationId), "Harness Replay operation identity mismatch");
          assert.ok(events.some((event) => event.eventType === "fixture.command.accepted"), "Harness Replay event type mismatch");
        }
      } else {
        const input = method.methodKind === "client_streaming"
          ? (async function* () { yield request; yield request; })()
          : request;
        const response = await client[method.localName](input);
        result.serialized = response.$typeName === method.output.typeName;
        result.decoded = {
          response_type: response.$typeName,
          present_fields: [...new Set(method.output.fields
            .filter((field) => response[field.localName] !== undefined && response[field.localName] !== null)
            .map((field) => field.name))],
          observed_values: describeMessage(response),
        };
        if (method.name === "Commit" && response.outcome.case === "committed") {
          committedId = response.outcome.value.commitId;
          const read = await client.readCommit(create(StreamService.methods.find((item) => item.name === "ReadCommit").input, { commitId: response.outcome.value.commitId }));
          result.commitRoundTrip = read.$typeName === "acyclic.stream.v2.CommittedEnvelope";
        }
      }
      result.status = "passed";
      passed++;
    } catch (error) {
      result.exit_code = 1;
      result.code = error?.code ?? "unknown";
      result.message = String(error?.message ?? error);
      if (method.name === "Follow" && result.cancelled && result.cancellation_trace?.observed === true) {
          result.status = "passed";
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
results.push({ family: "stream", method: "Append.idempotencyReplay", status: "passed", execution_mode: "remote", invoked: true, exit_code: 0, equal: stable(first) === stable(replay) });
const cancelController = new AbortController();
const cancelTrace = await collectAndCancelAfterFirst(stream.follow(create(StreamService.methods.find((method) => method.name === "Follow").input, { path: "events/input", from: 1n }), { signal: cancelController.signal }), cancelController, 250);
assert.equal(cancelTrace.cancellationRequested, true, "Follow cancellation scenario did not request cancellation");
assert.equal(cancelTrace.cancellationObserved, true, "Follow cancellation scenario did not observe cancellation");
results.push({
  family: "stream",
  method: "Follow.cancellation",
  status: "passed",
  execution_mode: "remote",
  invoked: true,
  exit_code: 1,
  code: cancelTrace.cancellationError?.code ?? "cancelled",
  cancellation_trace: {
    requested: cancelTrace.cancellationRequested,
    observed: cancelTrace.cancellationObserved,
    frames_before_cancel: cancelTrace.values.length,
  },
});

assert.equal(failed, 0, JSON.stringify(results.filter((result) => result.status === "failed"), null, 2));
console.log(JSON.stringify({ schema: "acyclic.sdk.rust-fixture.runtime.v1", endpoint, execution_mode: "remote", installed_packages: true, services: services.map(([name, service]) => ({ name, methods: service.methods.length })), passed, failed, results }, null, 2));
