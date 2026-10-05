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
});
