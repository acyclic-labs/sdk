import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer, type RequestListener } from "node:http";
import { SecretsClient } from "../src/secrets.js";
import { CredentialCache, CredentialSnapshot } from "../src/credential.js";
import { SecretsClient as WireClient } from "../src/wasm.js";
import { SecretKind, SecretListRequestSchema } from "../generated/proto/auth/v1/auth_pb.js";
import { create, toBinary } from "@bufbuild/protobuf";

async function serve(fetch: RequestListener) {
  const server = createServer(fetch);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => { server.removeListener("error", reject); resolve(); });
  });
  const address = server.address();
  assert(address && typeof address === "object");
  return {
    origin: `http://127.0.0.1:${address.port}`,
    async close() {
      const closed = new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
      server.closeAllConnections();
      await closed;
    },
  };
}

test("WASM/TS secrets refuse missing invocation ownership before credentials and HTTP", async () => {
  const planted = "PLANTED_SECRET_DO_NOT_LOG_6547";
  let calls = 0;
  const server = await serve((_request, response) => {
    calls++;
    response.writeHead(403).end(planted);
  });
  let credentials = 0;
  const credential = () => { credentials++; return "allowed-fixture"; };
  const client = new SecretsClient(server.origin, credential, undefined);
  const wire = new WireClient(server.origin, credential, undefined, undefined);
  async function refused(pending: Promise<unknown>) {
    const outcome = await pending.then(
      () => ({ code: "unexpected_success", message: "request admitted" }),
      (error: unknown) => error,
    );
    assert.equal((outcome as { code: string }).code, "missing_operation_context");
    assert(!String(outcome).includes(planted));
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
    assert(!Object.hasOwn(create(SecretListRequestSchema, illegal), "account"));
    await assert.rejects(client.list(illegal), /invalid request/);
    const unknown = new Uint8Array([...canonical, 0x7a, 3, 98, 97, 100]);
    await assert.rejects(wire.list(unknown), /invalid request/);
    assert.equal(credentials, 0);
    assert.equal(calls, 0);
  } finally { client.free(); wire.free(); await server.close(); }
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}

test("WASM credential ingress shares opaque admission and retains original request refusal", async () => {
  let calls = 0;
  const server = await serve((_request, response) => {
    calls++;
    response.writeHead(500).end();
  });
  const planted = "ORIGINAL_CREDENTIAL_REFUSAL_DO_NOT_DISCLOSE";
  const cases: readonly (readonly [unknown, boolean])[] = [
    ["synthetic", true], ["\uFEFF", true], ["\u0085", false],
    ["é".repeat(6144), true], ["é".repeat(6145), false],
    ["\uD800".repeat(4096), true], ["\uD800".repeat(4097), false],
    ["synthetic\r", false], ["synthetic\n", false], ["synthetic\0", false],
    [undefined, false], [1, false], [new String("synthetic"), false],
  ];
  try {
    for (const [token, accepted] of cases) {
      for (const custody of [false, true]) {
        // An accepted raw bearer belongs to the HTTP transport, whose header
        // alphabet is narrower. This control stops at the original request
        // refusal and needs no fabricated service response.
        if (accepted && !custody) continue;
        let resolved = 0;
        let checked = 0;
        const credential = Object.assign(async () => {
          resolved++;
          return token as string;
        }, custody ? { resolveRequest: async () => {
          resolved++;
          return {
            token: () => token,
            check: () => { checked++; throw new Error(planted); },
            reject: () => { throw new Error("rejection without dispatch"); },
          };
        } } : {});
        const stopped = new AbortController();
        const client = new SecretsClient(server.origin, credential, {
          deadlineUnixMillis: 0xffff_ffff_ffff_ffffn,
          stopped: stopped.signal,
        });
        try {
          const outcome = await client.list({ scope: "fixture" }).then(() => undefined, error => error);
          assert.equal(outcome?.code, "credential_unavailable");
          assert(!String(outcome).includes(planted));
          assert.equal(resolved, 1);
          assert.equal(checked, accepted ? 1 : 0);
          assert.equal(calls, 0);
        } finally { client.free(); }
      }
    }
  } finally { await server.close(); }
});

test("original WASM/TS invocation owns successful HTTP, stopped inputs and open response waits", async () => {
  const planted = "PLANTED_SECRET_DO_NOT_LOG_6547";
  const deadlineUnixMillis = 0xffff_ffff_ffff_ffffn;
  for (const mode of ["ready", "cancelled", "deadline", "source-loss", "pre-stopped", "credential", "credential-source"] as const) {
    const entered = deferred<void>();
    const release = deferred<void>();
    const stopped = new AbortController();
    let calls = 0;
    let credentials = 0;
    const server = await serve((request, response) => {
      calls++;
      assert.equal(request.headers.authorization, "Bearer allowed-fixture");
      if (mode === "ready") {
        response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ generation: 7, variables: [], nextCursor: null }));
        return;
      }
      if (mode !== "cancelled") response.writeHead(200).write("{");
      entered.resolve();
      // Cancellation holds headers; other stops hold a response before EOF.
      void release.promise.then(() => response.end());
    });
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
    const client = new SecretsClient(server.origin, mode === "credential-source" ? cache.provider : credential,
      { deadlineUnixMillis, stopped: stopped.signal });
    try {
      const pending = client.list({ scope: "fixture" });
      if (mode === "ready") {
        assert.equal((await pending).generation, 7n);
        assert.equal(calls, 1);
        assert.equal(credentials, 1);
      } else {
        if (mode !== "pre-stopped") {
          // An early transport/credential failure must end this control too.
          await Promise.race([entered.promise, pending]);
          if (mode === "credential-source") cache.invalidate();
          else stopped.abort(mode === "deadline" ? "deadline" : planted);
        }
        const outcome = await pending.then(() => undefined, error => error);
        assert.equal(outcome?.code, mode === "credential-source" ? "credential_unavailable"
          : mode === "deadline" ? "operation_deadline" : "operation_cancelled");
        assert(!String(outcome).includes(planted));
        if (mode === "pre-stopped") { assert.equal(credentials, 0); assert.equal(calls, 0); }
        if (mode === "credential") assert.equal(calls, 0);
      }
    } finally {
      release.resolve(); client.free(); await server.close();
    }
  }
});
