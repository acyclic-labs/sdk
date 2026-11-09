import { expect, test } from "bun:test";
import { Code, ConnectError } from "@connectrpc/connect";
import { GrpcStreamProvider } from "../src/grpc.js";
import { StreamError } from "../src/types.js";
import { grpcStatusError } from "../generated/wasm/acyclic_stream_wasm.js";
import { grpcFixture, tlsIdentity } from "./session-grpc-fixture.js";

test("the native status decoder retains opaque gRPC details", () => {
  const details = new Uint8Array([0, 255, 128, 1]);
  const encoded = Buffer.from(details).toString("base64");
  for (const binaryHeader of [encoded, encoded.replace(/=+$/, "")]) {
    const error = grpcStatusError(Code.Unavailable, "not-leader", [
      "grpc-status-details-bin", binaryHeader,
      "acyclic-original-bin", "/wAB",
    ]) as { code: string; refusal: { details: Uint8Array; metadata: [string, Uint8Array][] } };
    expect(error.code).toBe("unavailable");
    expect(error.refusal.details).toEqual(details);
    expect(error.refusal.metadata).toEqual([["acyclic-original-bin", new TextEncoder().encode("/wAB")]]);
  }
});

test("member refusals survive unary and session gRPC calls", async () => {
  const relayed = { "acyclic-unserved": "1", "acyclic-scope-owner": "7", "acyclic-scope-term": "3" };
  const identity = tlsIdentity();
  const fixture = await grpcFixture(identity, () => new ConnectError("not-leader", Code.Unavailable, relayed));
  try {
    const provider = new GrpcStreamProvider(fixture.options);
    const follow = provider.follow("events", { from: 0n })[Symbol.asyncIterator]();
    const results = [
      await provider.tail("events").then(() => undefined, (error: unknown) => error),
      await follow.next().then(() => undefined, (error: unknown) => error),
    ];
    for (const error of results) {
      if (!(error instanceof StreamError)) throw new Error(`expected StreamError, received ${String(error)}`);
      expect(error.code).toBe("unavailable");
      const refusal = error.refusal;
      if (refusal === undefined) throw new Error("refusal collapsed before the public error");
      expect(refusal.status).toBe(Code.Unavailable);
      expect(refusal.message).toBe("not-leader");
      expect(refusal.details).toEqual(new Uint8Array());
      const decoder = new TextDecoder();
      expect(Object.fromEntries(refusal.metadata.map(([name, value]) => [name, decoder.decode(value)]))).toEqual(relayed);
    }
    await expect(follow.return!()).rejects.toBe(results[1]);
    expect(fixture.counts.sessions).toBe(1);
    expect(fixture.counts.connections).toBe(1);
  } finally {
    await fixture.close();
    identity.dispose();
  }
});
