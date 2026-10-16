import { beforeAll, afterAll, expect, test } from "bun:test";
import { createRequire } from "node:module";
import { createSecureServer } from "node:http2";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { Code, ConnectError } from "@connectrpc/connect";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import fc from "fast-check";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { StreamService, TailRequestSchema, TailResponseSchema } from "../generated/proto/stream/v1/stream_pb.js";
import type * as Native from "../generated/native/binding.js";

const binding = createRequire(import.meta.url)("../generated/native/binding.cjs") as typeof Native & {
  NativeStreamClient: typeof Native.NativeStreamClient & {
    connectWithCredentialResult(endpoint: string, callback: () => Promise<string>, ca: Buffer): Promise<Native.NativeStreamConnectResult>;
  };
};
type RenewingClient = Native.NativeStreamClient & { withCredentialProvider(callback: () => Promise<string>): Native.NativeStreamClient };
const request = toBinary(TailRequestSchema, create(TailRequestSchema, { path: "credential/test" }));
let directory: string;
let key: string;
let cert: string;
beforeAll(() => {
  directory = mkdtempSync(join(tmpdir(), "native-credential-"));
  const keyPath = join(directory, "key.pem"), certPath = join(directory, "cert.pem");
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-keyout", keyPath, "-out", certPath], { stdio: "ignore" });
  key = readFileSync(keyPath, "utf8"); cert = readFileSync(certPath, "utf8");
});
afterAll(() => rmSync(directory, { recursive: true, force: true }));

async function fixture(reply: (authorization: string) => bigint) {
  const server = createSecureServer({ key, cert }, connectNodeAdapter({ routes(router) {
    router.service(StreamService, { tail(_request, context) {
      return { tail: reply(context.requestHeader.get("authorization") ?? "") };
    } });
  } }));
  const close = ownFixtureServer(server);
  await new Promise<void>(resolve => server.listen(0, "localhost", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("missing fixture address");
  return { endpoint: `https://localhost:${address.port}`, close };
}

test("native Rust retries resolve every JavaScript credential afresh", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 6 }), async values => {
    let calls = 0;
    const sent: string[] = [];
    const { endpoint, close } = await fixture(authorization => {
      sent.push(authorization);
      if (sent.length < values.length) throw new ConnectError("retry", Code.Unavailable);
      return 7n;
    });
    try {
      const connected = await binding.NativeStreamClient.connectWithCredentialResult(endpoint, async () => values[calls++]!, Buffer.from(cert));
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
  const { endpoint, close } = await fixture(() => { sent++; return 0n; });
  try {
    const connected = await binding.NativeStreamClient.connectWithCredentialResult(endpoint, async () => "unused", Buffer.from(cert));
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
