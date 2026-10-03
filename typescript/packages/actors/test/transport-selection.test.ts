import { expect, test } from "bun:test";
import { fromEnv } from "../src/client.js";
import { HttpActorsClient } from "../src/http.js";

test("Actors native default is a qualified remote transport", async () => {
  const client = await fromEnv({ endpoint: "https://actors.example", token: "fixture" });
  expect(client).not.toBeInstanceOf(HttpActorsClient);
});

test("Actors HTTP override remains available", async () => {
  const client = await fromEnv({ endpoint: "https://actors.example", token: "fixture", transport: "http" });
  expect(client).toBeInstanceOf(HttpActorsClient);
});
