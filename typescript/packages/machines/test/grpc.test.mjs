import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createSecureServer } from "node:http2";
import { once } from "node:events";
import { test } from "node:test";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { createMachinesGrpcClient } from "../dist/grpc.js";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

const caCertificate = readFileSync(new URL("./fixtures/server.pem", import.meta.url), "utf8");
const key = readFileSync(new URL("./fixtures/server-key.pem", import.meta.url), "utf8");
const protocol = { major: 1, minor: 1 };
const identity = value => ({ value: new Uint8Array(16).fill(value) });
const machine = identity(1);
const operation = identity(2);

async function fixture(t, implementation) {
  const server = createSecureServer({ cert: caCertificate, key }, connectNodeAdapter({
    routes(router) {
      router.service(wire.MachinesService, implementation);
    },
  }));
  const closeServer = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const options = { endpoint: `https://127.0.0.1:${server.address().port}`, token: "fixture", caCertificate };
  const client = createMachinesGrpcClient(options);
  t.after(async () => { client.close(); await closeServer(); });
  return { ...client, options };
}

function authenticate(context) {
  if (context.requestHeader.get("authorization") !== "Bearer fixture") throw new ConnectError("missing bearer", Code.Unauthenticated);
}

test("Cloud create returns an admission; inspect and destroy retain exact identities", async t => {
  const request = create(wire.CreateMachineRequestSchema, {
    protocol, idempotencyKey: identity(3),
    image: { kind: wire.ImageKind.MANAGED_OCI, immutableReference: { case: "managedDigest", value: new Uint8Array(32).fill(4) } },
    compatibility: { mode: wire.CompatibilityMode.BEST_EFFORT },
    suspension: { policy: { case: "manual", value: true } },
    expiration: { kind: wire.ExpirationKind.NEVER },
    networkPolicyDigest: new Uint8Array(32).fill(5), budgets: { spendMicros: 100n, concurrency: 1 },
  });
  const calls = [];
  const { machines } = await fixture(t, {
    create(query, context) {
      authenticate(context);
      assert.deepEqual(query, request);
      calls.push("create");
      return { machine, operation, contract: { image: query.image } };
    },
    inspectMachine(query, context) {
      authenticate(context);
      assert.deepEqual(query, create(wire.InspectMachineRequestSchema, { protocol, machine }));
      calls.push("inspect");
      return { machine, status: wire.MachineStatus.RUNNING, createdAtUnixMs: 9007199254740993n };
    },
    destroyMachine(query, context) {
      authenticate(context);
      assert.deepEqual(query, create(wire.MachineMutationRequestSchema, { protocol, machine, idempotencyKey: identity(6) }));
      calls.push("destroy");
      return { machine, operation };
    },
  });
  const admission = await machines.create(request);
  assert.deepEqual(admission.machine, create(wire.MachineIdSchema, machine));
  assert.deepEqual(admission.operation, create(wire.OperationIdSchema, operation));
  const observation = await machines.inspectMachine({ protocol, machine: admission.machine });
  assert.equal(observation.status, wire.MachineStatus.RUNNING);
  assert.equal(observation.createdAtUnixMs, 9007199254740993n);
  assert.deepEqual((await machines.destroyMachine({ protocol, machine: admission.machine, idempotencyKey: identity(6) })).machine, admission.machine);
  assert.deepEqual(calls, ["create", "inspect", "destroy"]);
});

test("watch delivers before completion and cancellation reaches the server", async t => {
  let cancelled;
  const cancellation = new Promise(resolve => { cancelled = resolve; });
  const { machines } = await fixture(t, {
    async *watchOperation(query, context) {
      authenticate(context);
      assert.deepEqual(query.operation, create(wire.OperationIdSchema, operation));
      context.signal.addEventListener("abort", cancelled, { once: true });
      yield { operation, status: wire.OperationStatus.PENDING };
      await cancellation;
    },
  });
  const controller = new AbortController();
  const iterator = machines.watchOperation({ protocol, operation }, { signal: controller.signal })[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.status, wire.OperationStatus.PENDING);
  controller.abort();
  await assert.rejects(iterator.next(), error => error instanceof ConnectError && error.code === Code.Canceled);
  await cancellation;
});

test("authentication failures preserve gRPC status and requests are not retried", async t => {
  let attempts = 0;
  const { options } = await fixture(t, {
    create(_query, context) {
      attempts++;
      authenticate(context);
      throw new ConnectError("unavailable", Code.Unavailable);
    },
  });
  const wrong = createMachinesGrpcClient({ ...options, token: "wrong" });
  const valid = createMachinesGrpcClient(options);
  t.after(() => { wrong.close(); valid.close(); });
  await assert.rejects(wrong.machines.create({ protocol }), error => error.code === Code.Unauthenticated);
  await assert.rejects(valid.machines.create({ protocol }), error => error.code === Code.Unavailable);
  assert.equal(attempts, 2);
});

test("message limits reject oversized responses", async t => {
  const { options } = await fixture(t, {
    inspectMachine() { return { machine: { value: new Uint8Array(1024) } }; },
  });
  const bounded = createMachinesGrpcClient({ ...options, maximumMessageBytes: 128 });
  t.after(() => bounded.close());
  await assert.rejects(bounded.machines.inspectMachine({ protocol, machine }), error => error.code === Code.ResourceExhausted);
});

test("invalid transport configuration is rejected before opening a session", () => {
  const options = { endpoint: "https://example.test", token: "fixture" };
  for (const endpoint of ["http://example.test", "https://user@example.test", "https://example.test?q=1", "https://example.test/#fragment"]) {
    assert.throws(() => createMachinesGrpcClient({ ...options, endpoint }), TypeError);
  }
  for (const token of [" ", "a\nb", "a\rb", "a\0b", "é".repeat(6145)]) {
    assert.throws(() => createMachinesGrpcClient({ ...options, token }), TypeError);
  }
  for (const maximumMessageBytes of [0, 1.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => createMachinesGrpcClient({ ...options, maximumMessageBytes }), RangeError);
  }
  for (const caCertificate of ["", "x".repeat(65537)]) {
    assert.throws(() => createMachinesGrpcClient({ ...options, caCertificate }), RangeError);
  }
});
