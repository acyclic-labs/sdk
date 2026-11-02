import { expect, test } from "bun:test";
import { SecretsClient } from "../src/secrets.js";
import { CredentialCache, CredentialSnapshot } from "../src/credential.js";
import { SecretsClient as WireClient } from "../src/wasm.js";
import { SecretKind, SecretListRequestSchema } from "../generated/proto/auth/v1/auth_pb.js";
import { create, toBinary } from "@bufbuild/protobuf";

test("WASM/TS secrets refuse missing invocation ownership before credentials and HTTP", async () => {
  const planted = "PLANTED_SECRET_DO_NOT_LOG_6547";
  let calls = 0;
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch() {
    calls++;
    return new Response(planted, { status: 403 });
  }});
  let credentials = 0;
  const credential = () => { credentials++; return "allowed-fixture"; };
  const client = new SecretsClient(server.url.origin, credential, undefined);
  const wire = new WireClient(server.url.origin, credential, undefined, undefined);
  async function refused(pending: Promise<unknown>) {
    const outcome = await pending.then(
      () => ({ code: "unexpected_success", message: "request admitted" }),
      (error: unknown) => error,
    );
    expect(outcome).toMatchObject({ code: "missing_operation_context" });
    expect(String(outcome)).not.toContain(planted);
  }
  try {
    await refused(client.list({ scope: "fixture" }));
    await refused(client.put({ scope: "fixture", name: "key", kind: SecretKind.PROTECTED, value: planted, expectedGeneration: 0n }));
    await refused(client.delete({ scope: "fixture", name: "key", expectedGeneration: 0n }));
    const canonical = toBinary(SecretListRequestSchema, create(SecretListRequestSchema, { scope: "fixture" }));
    await refused(wire.list(canonical));
    // create() alone silently drops account: both original admission boundaries
    // must still reject unknown selectors without invoking the credential owner.
    const illegal = { scope: "fixture", account: planted };
    expect(create(SecretListRequestSchema, illegal)).not.toHaveProperty("account");
    await expect(client.list(illegal)).rejects.toThrow("invalid request");
    const unknown = new Uint8Array([...canonical, 0x7a, 3, 98, 97, 100]);
    await expect(wire.list(unknown)).rejects.toThrow("invalid request");
    expect(credentials).toBe(0);
    expect(calls).toBe(0);
  } finally { client.free(); wire.free(); await server.stop(true); }
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}

test("original WASM/TS invocation owns successful HTTP, stopped inputs and open response waits", async () => {
  const planted = "PLANTED_SECRET_DO_NOT_LOG_6547";
  const deadlineUnixMillis = 0xffff_ffff_ffff_ffffn;
  for (const mode of ["ready", "cancelled", "deadline", "source-loss", "pre-stopped", "credential", "credential-source"] as const) {
    const entered = deferred<void>();
    const release = deferred<void>();
    const stopped = new AbortController();
    let calls = 0;
    let credentials = 0;
    const server = Bun.serve({ port: 0, hostname: "127.0.0.1", async fetch(request) {
      calls++;
      expect(request.headers.get("authorization")).toBe("Bearer allowed-fixture");
      if (mode === "ready") return Response.json({ generation: 7, variables: [], nextCursor: null });
      entered.resolve();
      if (mode === "cancelled") {
        await release.promise; // HTTP headers have not been published.
        return new Response(planted);
      }
      return new Response(new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode("{"));
        void release.promise.then(() => controller.close()).catch(() => {});
      }})); // Success headers, but the original response body has no EOF.
    }});
    const credential = async () => {
      credentials++;
      if (mode === "credential") { entered.resolve(); await release.promise; }
      return "allowed-fixture";
    };
    const cache = new CredentialCache(async () => {
      credentials++;
      return new CredentialSnapshot("allowed-fixture", "original", 100);
    }, () => ({ nowUnixSeconds: 1, revision: "original" }));
    if (mode === "pre-stopped") stopped.abort(planted);
    const client = new SecretsClient(server.url.origin, mode === "credential-source" ? cache.provider : credential,
      { deadlineUnixMillis, stopped: stopped.signal });
    try {
      const pending = client.list({ scope: "fixture" });
      if (mode === "ready") {
        expect((await pending).generation).toBe(7n);
        expect(calls).toBe(1);
        expect(credentials).toBe(1);
      } else {
        if (mode !== "pre-stopped") {
          await entered.promise;
          if (mode === "credential-source") cache.invalidate();
          else stopped.abort(mode === "deadline" ? "deadline" : planted);
        }
        const outcome = await pending.then(() => undefined, error => error);
        expect(outcome).toMatchObject({ code: mode === "credential-source" ? "credential_unavailable"
          : mode === "deadline" ? "operation_deadline" : "operation_cancelled" });
        expect(String(outcome)).not.toContain(planted);
        if (mode === "pre-stopped") { expect(credentials).toBe(0); expect(calls).toBe(0); }
        if (mode === "credential") expect(calls).toBe(0);
      }
    } finally {
      release.resolve(); client.free(); await server.stop(true);
    }
  }
});
