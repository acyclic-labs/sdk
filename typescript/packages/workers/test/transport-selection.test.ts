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
