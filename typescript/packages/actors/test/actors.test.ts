import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { HttpActorsClient, CreateActorRequestSchema } from "../src/index.js";
import { ACTORS_HANDSHAKE } from "../src/generated-client.js";

function handshakeResponse(): Response {
  return Response.json({ protocol: { version: ACTORS_HANDSHAKE.version, descriptorDigest: ACTORS_HANDSHAKE.descriptorDigest }, supported: {} });
}

describe("Actors v1 generated transport", () => {
  test("uses the generated request shape on the Rust-owned create route", async () => {
    let posted = "";
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test/",
      token: "secret",
      fetcher: async (input, init) => {
        if (new URL(String(input)).pathname === ACTORS_HANDSHAKE.route) return handshakeResponse();
        expect(String(input)).toBe("https://actors.example.test/v1/actors/create");
        posted = String(init?.body);
        return new Response(JSON.stringify({ actor: { actorId: "a", codeSha256: "AQ==", homeRegion: "eu" } }));
      },
    });
    const response = await client.createActor(create(CreateActorRequestSchema, {
      codeSha256: new Uint8Array(32).fill(1), homeRegion: "eu", idempotencyKey: "create-a",
      limits: { handlerTimeoutMillis: 1000n, memoryBytes: 1024n, checkpointBytes: 1024n },
    }));
    expect(posted).toContain("create-a");
    expect(response.actor?.actorId).toBe("a");
  });
  test("runs the generated transport against a real loopback server", async () => {
    let body = "";
    const server = Bun.serve({
      port: 0,
      fetch: async request => {
        if (new URL(request.url).pathname === ACTORS_HANDSHAKE.route) return handshakeResponse();
        expect(new URL(request.url).pathname).toBe("/v1/actors/create");
        expect(request.headers.get("authorization")).toBe("Bearer loopback-token");
        body = await request.text();
        return Response.json({});
      },
    });
    try {
      const client = new HttpActorsClient({ endpoint: `http://127.0.0.1:${server.port}/`, token: "loopback-token" });
      await client.createActor(create(CreateActorRequestSchema, { homeRegion: "eu" }));
      expect(body).toContain('"homeRegion":"eu"');
    } finally {
      server.stop(true);
    }
  });
  test("forwards request cancellation to the Rust-owned HTTP operation", async () => {
    const controller = new AbortController();
    let forwarded: AbortSignal | null | undefined;
    let rejectRequest: (reason?: unknown) => void = () => {};
    const pending = new Promise<Response>((_, reject) => { rejectRequest = reject; });
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test/",
      token: "secret",
      fetcher: async (_input, init) => {
        if (new URL(String(_input)).pathname === ACTORS_HANDSHAKE.route) return handshakeResponse();
        forwarded = init?.signal;
        if (init?.signal?.aborted) return Promise.reject(new Error("fixture request aborted"));
        init?.signal?.addEventListener("abort", () => rejectRequest(new Error("fixture request aborted")), { once: true });
        return pending;
      },
    });
    const request = client.createActor(create(CreateActorRequestSchema, { homeRegion: "eu" }), controller.signal);
    controller.abort();
    await expect(request).rejects.toThrow("fixture request aborted");
    expect(forwarded).toBe(controller.signal);
  });
});
