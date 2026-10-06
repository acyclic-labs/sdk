import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { HttpActorsClient, CreateActorRequestSchema } from "../src/index.js";

describe("Actors v1 generated transport", () => {
  test("uses the generated request shape on the Rust-owned create route", async () => {
    let posted = "";
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test/",
      token: "secret",
      fetcher: async (input, init) => {
        const url = String(input);
        if (url.endsWith("/v1/sdk/actors/handshake")) {
          return new Response(JSON.stringify({
            protocol: { version: "acyclic.actors.v1", descriptorDigest: "70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c" },
            supported: { capabilities: [{ name: "actors", version: "acyclic.actors.v1" }] },
          }));
        }
        expect(url).toBe("https://actors.example.test/v1/actors/create");
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

  test("cancelling one handshake waiter leaves the shared probe for another caller", async () => {
    let handshakeCalls = 0;
    let applicationCalls = 0;
    let markStarted!: () => void;
    let releaseHandshake!: () => void;
    const started = new Promise<void>(resolve => { markStarted = resolve; });
    const released = new Promise<void>(resolve => { releaseHandshake = resolve; });
    const request = create(CreateActorRequestSchema, {
      codeSha256: new Uint8Array(32).fill(1), homeRegion: "eu", idempotencyKey: "create-a",
      limits: { handlerTimeoutMillis: 1000n, memoryBytes: 1024n, checkpointBytes: 1024n },
    });
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test/",
      token: "secret",
      fetcher: async (input, init) => {
        const url = String(input);
        if (url.endsWith("/v1/sdk/actors/handshake")) {
          handshakeCalls += 1;
          expect(init?.signal).toBeUndefined();
          const handshake = JSON.parse(String(init?.body));
          expect(handshake.protocol).toEqual({ version: "acyclic.actors.v1", descriptorDigest: "70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c" });
          expect(handshake.required.capabilities).toEqual([{ name: "actors", version: "acyclic.actors.v1" }]);
          markStarted();
          await released;
          return new Response(JSON.stringify({
            protocol: { version: "acyclic.actors.v1", descriptorDigest: "70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c" },
            supported: { capabilities: [{ name: "actors", version: "acyclic.actors.v1" }] },
          }));
        }
        applicationCalls += 1;
        return new Response(JSON.stringify({ actor: { actorId: "a", codeSha256: "AQ==", homeRegion: "eu" } }));
      },
    });
    const firstController = new AbortController();
    const first = client.createActor(request, firstController.signal);
    await started;
    firstController.abort();
    await expect(first).rejects.toMatchObject({ name: "AbortError" });
    const second = client.createActor(request);
    releaseHandshake();
    await expect(second).resolves.toMatchObject({ actor: { actorId: "a" } });
    expect({ handshakeCalls, applicationCalls }).toEqual({ handshakeCalls: 1, applicationCalls: 1 });
  });

  test("direct HTTP construction defers Rust credential validation until async admission", async () => {
    let requests = 0;
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test",
      token: "bad\r\ncredential",
      fetcher: async () => { requests += 1; throw new Error("unexpected fetch"); },
    });
    await expect(client.createActor(create(CreateActorRequestSchema, {
      codeSha256: new Uint8Array(32).fill(1), homeRegion: "eu", idempotencyKey: "create-a",
      limits: { handlerTimeoutMillis: 1000n, memoryBytes: 1024n, checkpointBytes: 1024n },
    }))).rejects.toBeInstanceOf(TypeError);
    expect(requests).toBe(0);
  });
});
