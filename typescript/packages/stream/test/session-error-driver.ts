import assert from "node:assert/strict";
import { Code, ConnectError } from "@connectrpc/connect";
import { grpcFixture, tlsIdentity } from "./session-grpc-fixture.js";

const fault = process.env.STREAM_SESSION_FAULT;
const observed: unknown[] = [];
Reflect.set(globalThis, "captureSessionError", (error: { code?: unknown }) => observed.push(error.code));
if (process.versions.bun) {
  const { plugin } = await import("bun");
  plugin({ name: "session-error-vocabulary", setup(build) {
    build.onLoad({ filter: /[\\/]src[\\/]grpc\.ts$/ }, async args => {
      const source = await Bun.file(args.path).text();
      const needle = "throw sessionTransportError(error, interrupted);";
      assert(source.includes(needle), "canonical error probe no longer matches");
      const expression = fault === "aliases" ? 'providerError(error, "session")'
        : fault === "disposition" ? 'sessionTransportError(error, false)'
        : fault === "rpc-reset" ? 'sessionTransportError(error, !signal.aborted && !statusReceived && connection !== undefined && (connection.closed || connection.destroyed))'
        : "sessionTransportError(error, interrupted)";
      const traced = source.replace("rpc = request;", `rpc = request; for (const event of ["aborted", "end", "close"]) request.once(event, () => console.error("original HTTP/2 event", { event, resetCode: request.rstCode, closed: request.closed, destroyed: request.destroyed, errors: request.listenerCount("error") }));`);
      return { contents: traced.replace(needle, `const failure = ${expression}; Reflect.get(globalThis, "captureSessionError")(failure); if (Reflect.get(globalThis, "capturePhysicalError")) Reflect.get(globalThis, "capturePhysicalError")(error, interrupted, rpcCancelled, rpc?.rstCode); throw failure;`), loader: "ts" };
    });
  } });
}
const { GrpcStreamProvider } = await import("../src/grpc.js");
const { StreamError } = await import("../src/types.js");
const { streamErrorCodes, publicHttpErrorCode } = await import("../generated/wasm/acyclic_stream_wasm.js");
import type { StreamErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";

// Independent wire oracle; its completeness is checked against Rust's ALL,
// not against another hand-maintained JavaScript code inventory.
const statuses = {
  invalid_path: [Code.InvalidArgument, "invalid_path"],
  invalid_argument: [Code.InvalidArgument, "invalid_argument"],
  invalid_response: [Code.DataLoss, "invalid_response"],
  limit_exceeded: [Code.InvalidArgument, "limit_exceeded"],
  not_found: [Code.NotFound, "missing stream"],
  already_exists: [Code.AlreadyExists, "destination exists"],
  prefix_not_retained: [Code.FailedPrecondition, "prefix_not_retained"],
  out_of_range: [Code.OutOfRange, "outside the stream"],
  idempotency_mismatch: [Code.FailedPrecondition, "idempotency_mismatch"],
  capacity: [Code.ResourceExhausted, "node capacity"],
  access_denied: [Code.PermissionDenied, "denied"],
  unavailable: [Code.Unavailable, "offline"],
  hierarchy_changed: [Code.FailedPrecondition, "hierarchy_changed"],
  deadline_elapsed: [Code.FailedPrecondition, "deadline_elapsed"],
  revision_foreign: [Code.FailedPrecondition, "revision_foreign"],
  unsupported: [Code.Unimplemented, "unsupported_capability"],
} satisfies Record<StreamErrorCode, readonly [Code, string]>;
const codes = streamErrorCodes();
assert.deepEqual(Object.keys(statuses).sort(), [...codes].sort());
let failure: ConnectError | undefined;
const identity = tlsIdentity();
const fixture = await grpcFixture(identity, () => failure);
try {
  const provider = new GrpcStreamProvider(fixture.options);
  const peerStatuses = [
    ...(codes as StreamErrorCode[]).map(code => [code, ...statuses[code]] as const),
    ["unavailable", Code.Canceled, "peer cancellation"] as const,
  ];
  for (const [code, status, message] of peerStatuses) {
    failure = new ConnectError(message, status);
    observed.length = 0;
    const before = fixture.counts.sessions;
    const follow = provider.follow("events", { from: 0n })[Symbol.asyncIterator]();
    const next = follow.next();
    const error = await next.then(() => undefined, error => error);
    if (process.versions.bun) {
      assert(observed.length > 0, "error never crossed the JS transport");
      assert(observed.every(value => value === code), `JS transport must emit canonical Rust code: ${code}; received ${observed}`);
    }
    assert(error instanceof StreamError, `missing public error for ${code}`);
    assert.equal(error.code, publicHttpErrorCode(code, "session"), `public result for ${code}`);
    // Missing paths and other terminal errors must not be retried as outages.
    assert.equal(fixture.counts.sessions - before, 1, `a peer refusal never requests retry for ${code}`);
    await assert.rejects(follow.return!(), cleanup => cleanup === error);
  }
  assert.equal(fixture.counts.legacyFollows, 0);
  assert.equal(fixture.counts.connections, 1);
  const peerSessions = fixture.counts.sessions;
  failure = undefined;
  // The same missing-status error on a live connection is a definite protocol
  // failure. Its text and gRPC code are not evidence for physical reconnection.
  fixture.faults.missingStatus = true;
  const malformed = provider.follow("protocol", { from: 0n })[Symbol.asyncIterator]();
  const protocolError = await malformed.next().then(() => undefined, error => error);
  assert(protocolError instanceof StreamError && protocolError.code === "unavailable");
  await assert.rejects(malformed.return!(), cleanup => cleanup === protocolError);
  assert.equal(fixture.counts.sessions, peerSessions + 1);
  assert.equal(fixture.counts.connections, 1);
  fixture.faults.missingStatus = false;
  const follows = ["recovery/a", "recovery/b"].map(path => provider.follow(path, { from: 0n })[Symbol.asyncIterator]());
  try {
    const first = Promise.all(follows.map(follow => follow.next()));
    await Promise.race([fixture.node.until(() => fixture.node.active === 2), first]);
    fixture.node.append("recovery/a");
    fixture.node.append("recovery/b");
    assert((await first).every(item => item.value?.sequence === 0n));
    for (const sequence of [1n, 2n]) {
      const next = Promise.all(follows.map(follow => follow.next()));
      // A real RPC reset retains its HTTP/2 connection; the following loss
      // destroys that same connection. Both preserve original follow cursors.
      let resetCode: number | undefined;
      let cancelled = false;
      Reflect.set(globalThis, "capturePhysicalError", (error: Error, interrupted: boolean, reset: boolean, code: number | undefined) => {
        resetCode = code;
        cancelled = reset;
        console.error("physical HTTP/2 interruption", { interrupted, cancelled, resetCode, error });
      });
      if (sequence === 1n) fixture.resetRpc(); else fixture.disconnect();
      await Promise.race([
        fixture.node.until(() => fixture.node.connections.length === Number(sequence) + 1 && fixture.node.active === 2),
        next.then(() => { throw new Error("lost connection fabricated a record"); }, error => {
          throw new Error("physical connection loss ended the original follows", { cause: error });
        }),
      ]);
      if (process.versions.bun && sequence === 1n) assert(cancelled || resetCode === 8, "fixture must deliver an actual HTTP/2 CANCEL reset");
      assert.deepEqual(fixture.node.resumes.slice(-2).map(resume => resume.next), [sequence, sequence]);
      assert.equal(fixture.counts.connections, Number(sequence), "RPC reset must retain the original HTTP/2 connection");
      fixture.node.append("recovery/a");
      fixture.node.append("recovery/b");
      assert((await next).every(item => item.value?.sequence === sequence));
    }
    assert.equal(fixture.counts.sessions, peerSessions + 4);
    assert.equal(fixture.counts.connections, 2);
    assert.equal(fixture.counts.legacyFollows, 0);
  } finally { await Promise.all(follows.map(follow => follow.return?.())); }
  await fixture.close();
  const refused = new GrpcStreamProvider(fixture.options).follow("refused", { from: 0n })[Symbol.asyncIterator]();
  const refusal = await refused.next().then(() => undefined, error => error);
  assert(refusal instanceof StreamError && refusal.code === "unavailable");
  await assert.rejects(refused.return!(), cleanup => cleanup === refusal);
  console.log(JSON.stringify({ codes: codes.length, sessions: peerSessions, connections: 1, recoveredConnections: fixture.counts.connections }));
} finally { await fixture.close(); identity.dispose(); }
