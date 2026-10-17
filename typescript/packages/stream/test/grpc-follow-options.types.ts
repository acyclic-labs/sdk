import type { CallOptions } from "@connectrpc/connect";
import type { createStreamGrpcClient } from "../dist/grpc.js";

// Compile-only consumer of the emitted public declaration. An unused
// Unused expected-error directives fail, so restoring the old overload breaks this test.
export function followOptions(client: ReturnType<typeof createStreamGrpcClient>, signal: AbortSignal, options: CallOptions): void {
  const request = { path: "events", from: 0n };
  client.follow(request);
  client.follow(request, { timeoutMs: 500, signal });
  // @ts-expect-error Headers cannot be changed on a virtual follow.
  client.follow(request, { headers: { "x-test": "value" } });
  // @ts-expect-error The shared connection has no per-channel header callback.
  client.follow(request, { onHeader: () => {} });
  // @ts-expect-error The shared connection has no per-channel trailer callback.
  client.follow(request, { onTrailer: () => {} });
  // @ts-expect-error A wider CallOptions variable must not restore unsupported fields.
  client.follow(request, options);
  client.tail({ path: "events" }, options);
  client.children({ limit: 1 }, { timeoutMs: 500, signal });
  // @ts-expect-error Discovery also uses a virtual channel on the shared session.
  client.children({ limit: 1 }, { headers: { "x-test": "value" } });
  // @ts-expect-error Discovery has no per-channel header callback.
  client.children({ limit: 1 }, { onHeader: () => {} });
  // @ts-expect-error Discovery has no per-channel trailer callback.
  client.children({ limit: 1 }, { onTrailer: () => {} });
}
