import { expect, test } from "bun:test";
import { fromEnv } from "../src/client.js";
import { HttpWorkersClient } from "../src/http.js";

test("Workers native default is a qualified remote transport", async () => {
  const client = await fromEnv({ endpoint: "https://workers.example", token: "fixture" });
  expect(client).not.toBeInstanceOf(HttpWorkersClient);
});

test("Workers HTTP override remains available", async () => {
  const client = await fromEnv({ endpoint: "https://workers.example", token: "fixture", transport: "http" });
  expect(client).toBeInstanceOf(HttpWorkersClient);
});

test("Workers HTTP factory awaits Rust WASM before validating credentials", async () => {
  await expect(fromEnv({ endpoint: "https://workers.example", token: "bad\r\n", transport: "http" })).rejects.toThrow("invalid bearer credential");
});
