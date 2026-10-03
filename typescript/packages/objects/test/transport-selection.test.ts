import { expect, test } from "bun:test";
import { fromEnv } from "../src/v2-client.js";
import { HttpObjectsV2 } from "../src/v2-http.js";

test("Objects native default is a qualified remote transport", async () => {
  const client = await fromEnv({ endpoint: "https://objects.example", token: "fixture" });
  expect(client).not.toBeInstanceOf(HttpObjectsV2);
});

test("Objects HTTP override remains available", async () => {
  const client = await fromEnv({ endpoint: "https://objects.example", token: "fixture", transport: "http" });
  expect(client).toBeInstanceOf(HttpObjectsV2);
});
