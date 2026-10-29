import { expect, test } from "bun:test";
import { CredentialCache, CredentialSnapshot } from "@acyclic-labs/auth/credential";
import { create, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { encode_objects_v1_json } from "../generated/wasm/acyclic_objects_wasm.js";
import { HttpObjectsV1 } from "../src/v1-http.js";
import { ensureObjectsWasm } from "../src/wasm-runtime.js";

test("blob HTTP replies retain exact credential custody and preserve newer leases", async () => {
  await ensureObjectsWasm();
  for (const change of ["unchanged", "account", "unavailable", "expiry", "invalidate", "denial", "rejection", "late-rejection"] as const) {
    let revision: string | undefined = "account/audience/1", now = 1, refreshes = 0;
    const cache = new CredentialCache(async () => new CredentialSnapshot(`synthetic-${++refreshes}`, revision!, 100),
      () => ({ nowUnixSeconds: now, revision }));
    const old = await cache.resolve();
    const status = change === "denial" ? 403 : change.endsWith("rejection") ? 401 : 200;
    const body = status === 200
      ? encode_objects_v1_json("UsageResponse", toBinary(wire.UsageResponseSchema, create(wire.UsageResponseSchema, { blobs: 1n, uniqueBytes: 2n })), 1024)
      : encode_objects_v1_json("ErrorDetail", toBinary(wire.ErrorDetailSchema, create(wire.ErrorDetailSchema, { code: wire.ErrorCode.ACCESS_DENIED })), 1024);
    let entered!: () => void, release!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const released = new Promise<void>(resolve => { release = resolve; });
    const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch() {
      entered();
      if (change === "late-rejection") await released;
      return new Response(new ReadableStream<Uint8Array>({ async start(controller) {
        const middle = Math.floor(body.length / 2);
        controller.enqueue(body.slice(0, middle));
        await released;
        controller.enqueue(body.slice(middle));
        controller.close();
      } }), { status, headers: { "content-type": "application/json" } });
    } });
    try {
      const provider = new HttpObjectsV1({ endpoint: server.url.href, credential: cache.provider });
      const result = provider.usage().then(value => ({ value }), error => ({ error }));
      await started;
      switch (change) {
        case "account": revision = "other-account/audience/2"; break;
        case "unavailable": revision = undefined; break;
        case "expiry": now = 100; break;
        case "invalidate": cache.invalidate(); break;
        case "late-rejection": cache.reject(old); await cache.resolve(); break;
      }
      release();
      const outcome = await result;
      if (change === "unchanged") expect(outcome).toHaveProperty("value.blobs", 1n);
      else expect(outcome).toHaveProperty("error.code", wire.ErrorCode.ACCESS_DENIED);
      revision = "account/audience/1"; now = 1;
      await cache.resolve();
      expect(refreshes).toBe(change === "invalidate" || change.endsWith("rejection") ? 2 : 1);
    } finally { release(); await server.stop(true); }
  }
});
