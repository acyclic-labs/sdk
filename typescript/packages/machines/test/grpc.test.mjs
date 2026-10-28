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
const protocol = { major: 1, minor: 2 };
const identity = value => ({ value: new Uint8Array(16).fill(value) });
const machine = identity(1);
const operation = identity(2);

async function fixture(t, implementation, rawExec = false) {
  const server = createSecureServer({ cert: caCertificate, key }, connectNodeAdapter({
    routes(router) {
      router.service(wire.MachinesService, implementation.exec === undefined || rawExec ? implementation : {
        ...implementation,
        async *exec(input, context) { yield* implementation.exec(await receiveCommand(input), context); },
      });
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

function commandRequest(request) {
  const { argv, stdin = new Uint8Array(), ...start } = request;
  return { ...create(wire.ExecStartSchema, start), argv, stdin };
}

// Internal service vectors, not a provider capture or a production command engine.
async function receiveCommand(input) {
  let start;
  let complete = false;
  let eof = false;
  let argument = [];
  const argv = [], stdin = [];
  for await (const frame of input) {
    const { case: kind, value } = frame.input;
    if (kind === "start" && start === undefined) start = value;
    else if (start && kind === "argument" && !complete) {
      assert.ok(value.data.byteLength <= 65_536);
      argument.push(value.data);
      if (value.end) {
        argv.push(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(argument)));
        argument = [];
      }
    } else if (start && kind === "commandComplete" && !complete && argument.length === 0 && argv.length > 0) complete = true;
    else if (kind === "stdin" && complete && !eof) {
      assert.ok(value.byteLength > 0 && value.byteLength <= 65_536);
      stdin.push(value);
    } else if (kind === "stdinEof" && complete && !eof) eof = true;
    else assert.fail("invalid command frame order");
  }
  assert.ok(complete && eof);
  return { ...start, argv, stdin: new Uint8Array(Buffer.concat(stdin)) };
}

test("native binary file IO preserves ranges, short writes, errno and original identity", async t => {
  const access = { machine, path: "/workspace/$(literal) ' file", offset: 9007199254740993n, deadlineUnixMs: BigInt(Date.now() + 60000) };
  const data = new Uint8Array([0, 255, 10]);
  const calls = [];
  const { machines } = await fixture(t, {
    readFile(query, context) {
      authenticate(context);
      calls.push("read");
      assert.deepEqual(query.access, create(wire.FileAccessSchema, access));
      if (query.limit === 1) return { machine, offset: access.offset, result: { case: "errno", value: 13 } };
      if (query.limit === 2) return { machine: identity(99), offset: access.offset, result: { case: "data", value: data } };
      return { machine, offset: access.offset, result: { case: "data", value: data } };
    },
    writeFile(query, context) {
      authenticate(context);
      calls.push("write");
      assert.deepEqual(query.data, data);
      if (query.truncate) throw new ConnectError("completion lost", Code.Unavailable);
      return { machine, offset: query.access.offset, result: { case: "written", value: 2 } };
    },
  });
  const read = await machines.readFile({ protocol, access, limit: 3 });
  assert.deepEqual(read.result, { case: "data", value: data });
  assert.deepEqual((await machines.readFile({ protocol, access, limit: 1 })).result, { case: "errno", value: 13 });
  assert.deepEqual((await machines.writeFile({ protocol, access, data })).result, { case: "written", value: 2 });
  await assert.rejects(machines.readFile({ protocol, access, limit: 2 }), error => error.code === Code.DataLoss);
  const dispatched = calls.length;
  for (const invalid of [
    { ...access, path: "relative" }, { ...access, path: "/a\0b" },
    { ...access, offset: (1n << 64n) - 1n }, { ...access, deadlineUnixMs: 1n },
  ]) await assert.rejects(machines.readFile({ protocol, access: invalid, limit: 3 }), RangeError);
  await assert.rejects(machines.writeFile({ protocol, access, data, truncate: true }), RangeError);
  assert.equal(calls.length, dispatched);
  await assert.rejects(machines.writeFile({ protocol, access: { ...access, offset: 0n }, data, truncate: true }), error => error.code === Code.Unavailable);
  assert.equal(calls.length, dispatched + 1);
});

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
      assert.deepEqual(query, commandRequest({ protocol, machine, argv: ["/bin/echo", "sdk-token"], timeoutMs: 60000 }));
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

test("exec carries binary stdin, cwd and authority and separates stderr before its stop receipt", async t => {
  let cancelled;
  const cancellation = new Promise(resolve => { cancelled = resolve; });
  const request = {
    protocol, machine, argv: ["bash", "--noprofile", "--norc", "-c", `read -r line; printf '%s' "$line"; #${"é😀".repeat(16000)}`, "", ...Array(65).fill("argument")],
    timeoutMs: 300000, stdin: Uint8Array.from({ length: 150000 }, (_, index) => index % 3 === 0 ? 0 : index % 3 === 1 ? 255 : 10), cwd: "/workspace/sub directory",
    expiryUnixMs: 4102444800000n, maximumOutputBytes: 4n,
  };
  const stopped = { kind: wire.ExecStopKind.EXPIRED, message: "scope elapsed", cleanupCompleted: true };
  const { machines } = await fixture(t, {
    async *exec(query, context) {
      authenticate(context);
      assert.deepEqual(query, commandRequest(request));
      context.signal.addEventListener("abort", cancelled, { once: true });
      yield { machine, output: { case: "data", value: new Uint8Array([0, 255]) } };
      yield { machine, output: { case: "stderr", value: new Uint8Array([254, 10]) } };
      yield { machine, output: { case: "stopped", value: stopped } };
      await cancellation;
    },
  });
  const frames = [];
  for await (const frame of machines.exec(request)) frames.push(frame.output);
  assert.deepEqual(frames, [
    { case: "data", value: new Uint8Array([0, 255]) },
    { case: "stderr", value: new Uint8Array([254, 10]) },
    { case: "stopped", value: create(wire.ExecStopSchema, stopped) },
  ]);
  await cancellation;
});

test("exec delivers output while command input is backpressured and drop cancels both directions", { timeout: 5000 }, async t => {
  const cancellation = Promise.withResolvers();
  let complete = false;
  const { machines } = await fixture(t, {
    async *exec(input, context) {
      authenticate(context);
      context.signal.addEventListener("abort", cancellation.resolve, { once: true });
      const reader = input[Symbol.asyncIterator]();
      assert.equal((await reader.next()).value.input.case, "start");
      // Do not grant input progress. Output must remain independently readable.
      yield { machine, output: { case: "data", value: new Uint8Array([7]) } };
      await cancellation.promise;
      complete = true;
    },
  }, true);
  const output = machines.exec({ protocol, machine, argv: ["bash", "-c", "#".repeat(200000)], stdin: new Uint8Array(200000), timeoutMs: 300000 });
  assert.deepEqual((await output.next()).value.output, { case: "data", value: new Uint8Array([7]) });
  assert.equal(complete, false);
  await output.return();
  await cancellation.promise;
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

test("exec rejects incomplete and malformed streams and completes at its terminal event", async t => {
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
  for (const mode of ["missing", "unknown", "empty", "oversized", "partial-error"]) {
    await assert.rejects(exec(machines, { protocol, machine, argv: [mode], timeoutMs: 1000 }), error => error.code === (mode === "partial-error" ? Code.Unavailable : Code.DataLoss));
  }
  for (const mode of ["duplicate", "tail"]) {
    assert.equal(await exec(machines, { protocol, machine, argv: [mode], timeoutMs: 1000 }), 0);
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
  for (const argv of [[], [""], ["a\0b"], ["echo", "a\0b"]]) {
    await assert.rejects(exec(machines, { ...request, argv }), RangeError);
  }
  for (const timeoutMs of [0, 0x100000000, 1.5]) await assert.rejects(exec(machines, { ...request, timeoutMs }), RangeError);
  for (const cwd of ["", "a\0b"]) await assert.rejects(exec(machines, { ...request, cwd }), RangeError);
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
      assert.equal(context.requestHeader.get("grpc-timeout"), null);
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

test("return interrupts a pending stream pull and terminal watch needs no EOF", { timeout: 5000 }, async t => {
  const execCancelled = Promise.withResolvers();
  const watchCancelled = Promise.withResolvers();
  const { machines } = await fixture(t, {
    async *exec(_query, context) {
      context.signal.addEventListener("abort", execCancelled.resolve, { once: true });
      yield { machine, output: { case: "data", value: new Uint8Array([1]) } };
      await execCancelled.promise;
    },
    async *watchOperation(_query, context) {
      authenticate(context);
      assert.equal(context.requestHeader.get("grpc-timeout"), null);
      context.signal.addEventListener("abort", watchCancelled.resolve, { once: true });
      yield { operation, status: wire.OperationStatus.FAILED };
      await watchCancelled.promise;
    },
  });
  const stream = machines.exec({ protocol, machine, argv: ["sleep"], timeoutMs: 300000 });
  assert.equal((await stream.next()).value.output.case, "data");
  const pending = assert.rejects(stream.next(), error => error.code === Code.Canceled);
  await stream.return();
  await pending;
  await execCancelled.promise;
  const watch = machines.watchOperation({ protocol, operation });
  assert.equal((await watch.next()).value.status, wire.OperationStatus.FAILED);
  assert.equal((await watch.next()).done, true);
  await watchCancelled.promise;
});

test("watch refuses substituted identities, invalid status and incomplete EOF without reopening", async t => {
  let attempts = 0;
  const { machines } = await fixture(t, {
    async *watchOperation(query) {
      attempts++;
      const mode = query.operation.value[0];
      if (mode === 1) yield { operation: identity(99), status: wire.OperationStatus.SUCCEEDED };
      else if (mode === 2) yield { operation: query.operation };
      else if (mode === 3) yield { operation: query.operation, status: wire.OperationStatus.PENDING };
      else yield { operation: query.operation, status: wire.OperationStatus.INDETERMINATE };
    },
  });
  for (const mode of [1, 2, 3]) {
    await assert.rejects(async () => {
      for await (const _frame of machines.watchOperation({ protocol, operation: identity(mode) })) {}
    }, error => error.code === Code.DataLoss);
  }
  const stream = machines.watchOperation({ protocol, operation: identity(4) });
  assert.equal((await stream.next()).value.status, wire.OperationStatus.INDETERMINATE);
  assert.equal((await stream.next()).done, true);
  assert.equal(attempts, 4);
});

test("original guest deadlines bound stalled credentials even with timeout zero", { timeout: 5000 }, async t => {
  let dispatched = 0;
  const { options } = await fixture(t, {
    readFile() { dispatched++; },
    writeFile() { dispatched++; },
    async *exec() { dispatched++; },
  });
  const deadlineArmed = [];
  const client = createMachinesGrpcClient({ ...options, credential(signal) {
    deadlineArmed.push(signal);
    return new Promise(() => {});
  } });
  t.after(() => client.close());
  const expired = error => error instanceof ConnectError && error.code === Code.DeadlineExceeded;
  for (const method of ["readFile", "writeFile", "exec"]) {
    const deadline = BigInt(Date.now() + 100);
    const access = { machine, path: "/file", deadlineUnixMs: deadline };
    if (method === "exec") await assert.rejects(exec(client.machines, { protocol, machine, argv: ["sleep"], timeoutMs: 300000, expiryUnixMs: deadline }, { timeoutMs: 0 }), expired);
    else if (method === "readFile") await assert.rejects(client.machines.readFile({ protocol, access, limit: 1 }, { timeoutMs: 0 }), expired);
    else await assert.rejects(client.machines.writeFile({ protocol, access, data: new Uint8Array([1]) }, { timeoutMs: 0 }), expired);
  }
  assert.equal(dispatched, 0);
  assert.equal(deadlineArmed.length, 3);
  assert.ok(deadlineArmed.every(signal => signal.aborted));
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
