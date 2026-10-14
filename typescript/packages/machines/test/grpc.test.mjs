import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createSecureServer } from "node:http2";
import { once } from "node:events";
import { test } from "node:test";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { createMachinesGrpcClient, MAX_OUTPUT_READ_BYTES } from "../dist/grpc.js";
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

async function exec(client, request, options) {
  let exitCode;
  for await (const frame of client.exec(request, options)) {
    if (frame.output.case === "exitCode") exitCode = frame.output.value;
  }
  return exitCode;
}

test("Cloud create, exec, console read and destroy retain exact identities", async t => {
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
    async *exec(query, context) {
      authenticate(context);
      assert.deepEqual(query, create(wire.ExecRequestSchema, { protocol, machine, argv: ["/bin/echo", "sdk-token"], timeoutMs: 60000 }));
      assert.ok(context.timeoutMs() > 30000);
      calls.push("exec");
      yield { machine, output: { case: "data", value: new TextEncoder().encode("sdk-token\n") } };
      yield { machine, output: { case: "exitCode", value: 0 } };
    },
    readOutput(query, context) {
      authenticate(context);
      assert.deepEqual(query, create(wire.ReadOutputRequestSchema, { protocol, machine, offset: 0n, limit: MAX_OUTPUT_READ_BYTES }));
      calls.push("read");
      return { machine, offset: 0n, data: new TextEncoder().encode("sdk-token\n") };
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
  assert.equal(await exec(machines, { protocol, machine: admission.machine, argv: ["/bin/echo", "sdk-token"], timeoutMs: 60000 }), 0);
  assert.equal(new TextDecoder().decode((await machines.readOutput({ protocol, machine: admission.machine, offset: 0n, limit: MAX_OUTPUT_READ_BYTES })).data), "sdk-token\n");
  assert.deepEqual((await machines.destroyMachine({ protocol, machine: admission.machine, idempotencyKey: identity(6) })).machine, admission.machine);
  assert.deepEqual(calls, ["create", "inspect", "exec", "read", "destroy"]);
});

test("exec preserves nonzero exit status, cancellation, timeout and unavailable without redispatch", async t => {
  let attempts = 0;
  let started;
  let cancelled;
  let completed;
  const starts = new Promise(resolve => { started = resolve; });
  const cancels = new Promise(resolve => { cancelled = resolve; });
  const finishes = new Promise(resolve => { completed = resolve; });
  const { machines } = await fixture(t, {
    async *exec(query, context) {
      authenticate(context);
      attempts++;
      if (query.argv[0] === "unavailable") throw new ConnectError("owner unknown", Code.Unavailable);
      if (query.argv[0] === "cancel") {
        context.signal.addEventListener("abort", cancelled, { once: true });
        started();
        await cancels;
        completed();
      }
      if (query.argv[0] === "timeout") throw new ConnectError("command deadline", Code.DeadlineExceeded);
      yield { machine, output: { case: "exitCode", value: -9 } };
    },
  });
  const request = { protocol, machine, argv: ["exit"], timeoutMs: 1000 };
  assert.equal(await exec(machines, request), -9);
  await assert.rejects(exec(machines, { ...request, argv: ["unavailable"] }), error => error.code === Code.Unavailable);
  await assert.rejects(exec(machines, { ...request, argv: ["timeout"] }), error => error.code === Code.DeadlineExceeded);
  const controller = new AbortController();
  const pending = exec(machines, { ...request, argv: ["cancel"] }, { signal: controller.signal });
  const rejection = assert.rejects(pending, error => error.code === Code.Canceled);
  await starts;
  controller.abort();
  await rejection;
  await finishes;
  assert.equal(attempts, 4);
});

test("exec yields output before the command completes", async t => {
  let finish;
  const completion = new Promise(resolve => { finish = resolve; });
  const { machines } = await fixture(t, {
    async *exec(_query, context) {
      authenticate(context);
      yield { machine, output: { case: "data", value: new Uint8Array([0, 255]) } };
      await completion;
      yield { machine, output: { case: "exitCode", value: 0 } };
    },
  });
  const iterator = machines.exec({ protocol, machine, argv: ["echo"], timeoutMs: 1000 });
  const first = await iterator.next();
  assert.deepEqual(first.value.output, { case: "data", value: new Uint8Array([0, 255]) });
  finish();
  assert.deepEqual((await iterator.next()).value.output, { case: "exitCode", value: 0 });
  assert.equal((await iterator.next()).done, true);
});

for (const mode of ["early-exit", "malformed"]) {
  test(`exec cancels ${mode} without canceling another active RPC`, { timeout: 5000 }, async t => {
    let cancelled;
    let finishWatch;
    let watchCancelled = false;
    const cancellation = new Promise(resolve => { cancelled = resolve; });
    const watchCompletion = new Promise(resolve => { finishWatch = resolve; });
    const { machines } = await fixture(t, {
      async *exec(_query, context) {
        authenticate(context);
        context.signal.addEventListener("abort", cancelled, { once: true });
        yield { machine, output: { case: "data", value: new Uint8Array(mode === "malformed" ? 0 : 1) } };
        await cancellation;
      },
      async *watchOperation(_query, context) {
        authenticate(context);
        context.signal.addEventListener("abort", () => { watchCancelled = true; finishWatch(); }, { once: true });
        yield { operation, status: wire.OperationStatus.PENDING };
        await watchCompletion;
        yield { operation, status: wire.OperationStatus.SUCCEEDED };
      },
    });
    const watch = machines.watchOperation({ protocol, operation }, { timeoutMs: 0 })[Symbol.asyncIterator]();
    assert.equal((await watch.next()).value.status, wire.OperationStatus.PENDING);
    const request = { protocol, machine, argv: ["sleep"], timeoutMs: 60000 };
    if (mode === "malformed") {
      await assert.rejects(exec(machines, request, { timeoutMs: 0 }), error => error.code === Code.DataLoss);
    } else {
      for await (const frame of machines.exec(request, { timeoutMs: 0 })) {
        assert.equal(frame.output.case, "data");
        break;
      }
    }
    await cancellation;
    assert.equal(watchCancelled, false);
    finishWatch();
    assert.equal((await watch.next()).value.status, wire.OperationStatus.SUCCEEDED);
    assert.equal((await watch.next()).done, true);
  });
}

test("exec rejects incomplete, malformed and post-terminal streams", async t => {
  const { machines } = await fixture(t, {
    async *exec(query) {
      const mode = query.argv[0];
      if (mode === "missing") return;
      if (mode === "unknown") { yield { machine }; return; }
      if (mode === "empty" || mode === "oversized") {
        yield { machine, output: { case: "data", value: new Uint8Array(mode === "empty" ? 0 : 65537) } };
        return;
      }
      if (mode === "partial-error") {
        yield { machine, output: { case: "data", value: new Uint8Array([1]) } };
        throw new ConnectError("guest owner lost", Code.Unavailable);
      }
      yield { machine, output: { case: "exitCode", value: 0 } };
      yield { machine, output: mode === "duplicate" ? { case: "exitCode", value: 1 } : { case: "data", value: new Uint8Array([1]) } };
    },
  });
  for (const mode of ["missing", "unknown", "empty", "oversized", "duplicate", "tail", "partial-error"]) {
    await assert.rejects(exec(machines, { protocol, machine, argv: [mode], timeoutMs: 1000 }), error => error.code === (mode === "partial-error" ? Code.Unavailable : Code.DataLoss));
  }
});

test("console reads preserve uint64 offsets, discarded-byte gaps and temporary empty output", async t => {
  const offset = 9007199254740993n;
  const seen = [];
  const { machines } = await fixture(t, {
    readOutput(query, context) {
      authenticate(context);
      seen.push(query.offset);
      return { machine, offset: query.offset + (seen.length === 1 ? 4n : 0n), data: seen.length === 2 ? new Uint8Array() : new Uint8Array([0, 255]) };
    },
  });
  const first = await machines.readOutput({ protocol, machine, offset, limit: 2 });
  assert.equal(first.offset, offset + 4n);
  assert.deepEqual(first.data, new Uint8Array([0, 255]));
  const next = first.offset + BigInt(first.data.length);
  assert.equal((await machines.readOutput({ protocol, machine, offset: next, limit: 2 })).data.length, 0);
  assert.deepEqual((await machines.readOutput({ protocol, machine, offset: next, limit: 2 })).data, first.data);
  assert.deepEqual(seen, [offset, next, next]);
});

test("an exec RPC deadline cancels the pending call without redispatch", async t => {
  let attempts = 0;
  let cancelled;
  const cancellation = new Promise(resolve => { cancelled = resolve; });
  const { machines } = await fixture(t, {
    async *exec(_query, context) {
      attempts++;
      context.signal.addEventListener("abort", cancelled, { once: true });
      await cancellation;
      yield { machine, output: { case: "exitCode", value: 0 } };
    },
  });
  await assert.rejects(exec(machines, { protocol, machine, argv: ["sleep"], timeoutMs: 60000 }, { timeoutMs: 100 }), error => error.code === Code.DeadlineExceeded);
  await cancellation;
  assert.equal(attempts, 1);
});

test("exec and console request bounds fail before reaching the server", async t => {
  let attempts = 0;
  const { machines } = await fixture(t, {
    async *exec() { attempts++; yield { machine }; },
    readOutput() { attempts++; return { machine }; },
  });
  const request = { protocol, machine, argv: ["echo"], timeoutMs: 1 };
  for (const argv of [[], [""], ["a\0b"], ["echo", "a\0b"], Array(65).fill("a"), ["é".repeat(2049)], Array(9).fill("a".repeat(4096))]) {
    await assert.rejects(exec(machines, { ...request, argv }), RangeError);
  }
  for (const timeoutMs of [0, 60001, 1.5]) await assert.rejects(exec(machines, { ...request, timeoutMs }), RangeError);
  for (const limit of [0, 65537, 1.5]) await assert.rejects(machines.readOutput({ protocol, machine, offset: 0n, limit }), RangeError);
  for (const offset of [-1n, 1n << 64n]) await assert.rejects(machines.readOutput({ protocol, machine, offset, limit: 1 }), RangeError);
  assert.equal(attempts, 0);
});

test("exec and console reject substituted machines and invalid output ranges", async t => {
  const { machines } = await fixture(t, {
    async *exec() { yield { machine: identity(7), output: { case: "exitCode", value: 0 } }; },
    readOutput(query) {
      if (query.limit === 1) return { machine: identity(7), offset: query.offset };
      if (query.limit === 2) return { machine, offset: query.offset - 1n };
      if (query.limit === 3) return { machine, offset: query.offset, data: new Uint8Array(4) };
      return { machine, offset: (1n << 64n) - 1n, data: new Uint8Array(1) };
    },
  });
  await assert.rejects(exec(machines, { protocol, machine, argv: ["echo"], timeoutMs: 1 }), error => error.code === Code.DataLoss);
  for (const limit of [1, 2, 3, 4]) await assert.rejects(machines.readOutput({ protocol, machine, offset: 1n, limit }), error => error.code === Code.DataLoss);
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
