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

test("Objects HTTP factory awaits Rust WASM before validating credentials", async () => {
  await expect(fromEnv({ endpoint: "https://objects.example", token: "bad\r\n", transport: "http" })).rejects.toThrow("invalid bearer credential");
});
