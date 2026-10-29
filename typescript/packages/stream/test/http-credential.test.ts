import { expect, test } from "bun:test";
import { CredentialCache, CredentialSnapshot } from "@acyclic-labs/auth/credential";
import { HttpStreamProvider } from "../src/http.js";
import { ensureStreamWasm } from "../src/contract.js";

test("HTTP success retains the dispatched lease through a held response body", async () => {
  await ensureStreamWasm();
  for (const change of ["unchanged", "account", "unavailable", "expiry", "invalidate"] as const) {
    let revision: string | undefined = "account/audience/1";
    let now = 1;
    let refreshes = 0;
    const cache = new CredentialCache(async () => {
      refreshes++;
      return new CredentialSnapshot("synthetic-http", revision!, 100);
    }, () => ({ nowUnixSeconds: now, revision }));
    await cache.resolve();
    let held!: () => void;
    let release!: () => void;
    const ready = new Promise<void>(resolve => { held = resolve; });
    const released = new Promise<void>(resolve => { release = resolve; });
    const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch() {
      return new Response(new ReadableStream<Uint8Array>({ async start(controller) {
        controller.enqueue(new TextEncoder().encode('{"tail":'));
        held();
        await released;
        controller.enqueue(new TextEncoder().encode('"0"}'));
        controller.close();
      } }), { headers: { "content-type": "application/json" } });
    } });
    try {
      const provider = new HttpStreamProvider({ endpoint: "https://example.test", credential: cache.provider,
        // Use the real loopback server for the transport, without public TLS material.
        fetcher: (input, init) => fetch(new URL(new URL(String(input)).pathname, server.url), init),
      });
      const pending = provider.tail("events");
      // Install rejection custody before releasing the actual HTTP body.
      const result = pending.then(value => ({ value }), error => ({ error }));
      await ready;
      switch (change) {
        case "account": revision = "other-account/audience/2"; break;
        case "unavailable": revision = undefined; break;
        case "expiry": now = 100; break;
        case "invalidate": cache.invalidate(); break;
      }
      release();
      const outcome = await result;
      if (change === "unchanged") expect(outcome).toEqual({ value: 0n });
      else {
        expect("error" in outcome).toBe(true);
        if ("error" in outcome) expect(outcome.error).not.toHaveProperty("code", "invalid_response");
      }
      revision = "account/audience/1";
      now = 1;
      await cache.resolve();
      expect(refreshes).toBe(change === "invalidate" ? 2 : 1);
    } finally {
      release();
      await server.stop(true);
    }
  }
});
