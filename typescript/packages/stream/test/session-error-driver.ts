import assert from "node:assert/strict";
import { plugin } from "bun";
import { Code, ConnectError } from "@connectrpc/connect";
import { grpcFixture, tlsIdentity } from "./session-grpc-fixture.js";

const broken = process.env.STREAM_SESSION_FAULT === "aliases";
const observed: unknown[] = [];
Reflect.set(globalThis, "captureSessionError", (error: { code?: unknown }) => observed.push(error.code));
plugin({ name: "session-error-vocabulary", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]grpc\.ts$/ }, async args => {
    const source = await Bun.file(args.path).text();
    const needle = "throw sessionTransportError(error);";
    assert(source.includes(needle), "canonical error probe no longer matches");
    const expression = broken ? 'providerError(error, "session")' : "sessionTransportError(error)";
    return { contents: source.replace(needle, `const failure = ${expression}; Reflect.get(globalThis, "captureSessionError")(failure); throw failure;`), loader: "ts" };
  });
} });
const { GrpcStreamProvider } = await import("../src/grpc.js");
const { StreamError } = await import("../src/types.js");
const { streamErrorCodes, publicHttpErrorCode } = await import("../generated/wasm/acyclic_stream_wasm.js");
import type { StreamErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";

// Independent wire oracle; its completeness is checked against Rust's ALL,
// not against another hand-maintained JavaScript code inventory.
const statuses = {
  invalid_path: [Code.InvalidArgument, "invalid_path"],
  invalid_argument: [Code.InvalidArgument, "invalid_argument"],
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
  unsupported: [Code.Unimplemented, "unsupported_capability"],
} satisfies Record<StreamErrorCode, readonly [Code, string]>;
const codes = streamErrorCodes();
assert.deepEqual(Object.keys(statuses).sort(), [...codes].sort());
let failure: ConnectError | undefined;
const identity = tlsIdentity();
const fixture = await grpcFixture(identity, () => failure);
try {
  const provider = new GrpcStreamProvider(fixture.options);
  for (const code of codes as StreamErrorCode[]) {
    const [status, message] = statuses[code];
    failure = new ConnectError(message, status);
    observed.length = 0;
    const before = fixture.counts.sessions;
    const follow = provider.follow("events", { from: 0n, signal: AbortSignal.timeout(1000) })[Symbol.asyncIterator]();
    const error = await follow.next().then(() => undefined, error => error);
    assert(observed.length > 0, "error never crossed the JS transport");
    assert(observed.every(value => value === code), `JS transport must emit canonical Rust code: ${code}; received ${observed}`);
    assert(error instanceof StreamError, `missing public error for ${code}`);
    assert.equal(error.code, publicHttpErrorCode(code, "session"), `public result for ${code}`);
    // Missing paths and other terminal errors must not be retried as outages.
    assert.equal(fixture.counts.sessions - before, code === "unavailable" ? 3 : 1, `retry meaning for ${code}`);
    await follow.return?.();
  }
  assert.equal(fixture.counts.legacyFollows, 0);
  assert.equal(fixture.counts.connections, 1);
  console.log(JSON.stringify({ codes: codes.length, sessions: fixture.counts.sessions, connections: fixture.counts.connections }));
} finally { await fixture.close(); identity.dispose(); }
