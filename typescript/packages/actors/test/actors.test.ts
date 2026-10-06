import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import fc from "fast-check";
import { HttpActorsClient, CreateActorRequestSchema, InspectActorRequestSchema } from "../src/index.js";

describe("Actors v1 generated transport", () => {
  test("uses the generated request shape on the Rust-owned create route", async () => {
    let posted = "";
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test/",
      token: "secret",
      fetcher: async (input, init) => {
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
  test("refuses redirects and header-unsafe or oversized bearer tokens", async () => {
    for (const token of [" ", "a\nb", "a\rb", "a\0b", "x".repeat(8193)]) {
      expect(() => new HttpActorsClient({ endpoint: "https://actors.example.test", token })).toThrow(TypeError);
    }
    let redirect: RequestRedirect | undefined;
    const client = new HttpActorsClient({ endpoint: "https://actors.example.test", token: "x".repeat(8192), fetcher: async (_input, init) => {
      redirect = init?.redirect;
      return new Response(JSON.stringify({}));
    } });
    await client.inspectActor(create(InspectActorRequestSchema, { actorId: "a" }));
    expect(redirect).toBe("error");
  });
  test("accepts a response exactly when its size is within the bound, however it is chunked", async () => {
    await fc.assert(fc.asyncProperty(fc.integer({ min: 2, max: 64 }), fc.integer({ min: 1, max: 64 }), fc.array(fc.integer({ min: 1, max: 8 }), { minLength: 1 }), async (size, maximumResponseBytes, cuts) => {
      const body = new TextEncoder().encode(`${" ".repeat(size - 2)}{}`);
      const client = new HttpActorsClient({ endpoint: "https://actors.example.test", token: "secret", maximumResponseBytes, fetcher: async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (let offset = 0, index = 0; offset < body.byteLength; index += 1) {
            const end = offset + (cuts[index % cuts.length] ?? 1);
            controller.enqueue(body.slice(offset, end));
            offset = end;
          }
          controller.close();
        },
      })) });
      const inspected = client.inspectActor(create(InspectActorRequestSchema, { actorId: "a" }));
      if (size <= maximumResponseBytes) await inspected;
      else await expect(inspected).rejects.toThrow("exceeds configured bound");
    }), { numRuns: 100 });
  });
});
