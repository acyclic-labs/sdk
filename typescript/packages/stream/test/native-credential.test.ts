import { credentialTlsFixture } from "../../../test/credential-tls.js";
import { expect, test } from "bun:test";
import { createRequire } from "node:module";
import { Code, ConnectError } from "@connectrpc/connect";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import fc from "fast-check";
import { StreamService, TailRequestSchema, TailResponseSchema } from "../generated/proto/stream/v1/stream_pb.js";
import type * as Native from "../generated/native/binding.js";

const binding = createRequire(import.meta.url)("../generated/native/binding.cjs") as typeof Native & {
  NativeStreamClient: typeof Native.NativeStreamClient & {
    connectWithCredentialResult(endpoint: string, callback: () => Promise<string>, ca: Buffer): Promise<Native.NativeStreamConnectResult>;
  };
};
type RenewingClient = Native.NativeStreamClient & { withCredentialProvider(callback: () => Promise<string>): Native.NativeStreamClient };
const request = toBinary(TailRequestSchema, create(TailRequestSchema, { path: "credential/test" }));
async function fixture(reply: (authorization: string) => bigint) {
  return credentialTlsFixture(router => {
    router.service(StreamService, { tail(_request, context) {
      return { tail: reply(context.requestHeader.get("authorization") ?? "") };
    } });
  });
}

test("native Rust retries resolve every JavaScript credential afresh", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 6 }), async values => {
    let calls = 0;
    const sent: string[] = [];
    const { endpoint, certificate, close } = await fixture(authorization => {
      sent.push(authorization);
      if (sent.length < values.length) throw new ConnectError("retry", Code.Unavailable);
      return 7n;
    });
    try {
      const connected = await binding.NativeStreamClient.connectWithCredentialResult(endpoint, async () => values[calls++]!, Buffer.from(certificate));
      expect(connected.error).toBeUndefined();
      const client = connected.client!;
      expect(calls).toBe(0);
      const result = await client.tailResult(Buffer.from(request));
      expect(result.error).toBeUndefined();
      expect(fromBinary(TailResponseSchema, result.value!).tail).toBe(7n);
      expect(sent).toEqual(values.map(value => `Bearer ${value}`));
      expect(calls).toBe(values.length);
      const exhausted = await client.tailResult(Buffer.from(request));
      expect(exhausted.error?.code).toBe("access_denied");
      expect(sent.length).toBe(values.length);
    } finally { await close(); }
  }), { numRuns: 16 });
});

test("native provider throws, rejections, and cancellation never reach transport", async () => {
  let sent = 0;
  const { endpoint, certificate, close } = await fixture(() => { sent++; return 0n; });
  try {
    const connected = await binding.NativeStreamClient.connectWithCredentialResult(endpoint, async () => "unused", Buffer.from(certificate));
    const client = connected.client! as RenewingClient;
    for (const callback of [async (): Promise<string> => { throw new Error("rejected"); }, (): Promise<string> => { throw new Error("thrown"); }]) {
      const failed = await client.withCredentialProvider(callback).tailResult(Buffer.from(request));
      expect(failed.error?.code).toBe("access_denied");
    }
    const cancellation = new binding.NativeStreamCancellation();
    const cancelled = client.withCredentialProvider(async () => {
      cancellation.cancel();
      return "cancelled-before-send";
    });
    expect((await cancelled.tailResult(Buffer.from(request), cancellation)).error?.code).toBe("cancelled");
    expect(sent).toBe(0);
  } finally { await close(); }
});
