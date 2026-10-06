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
});
