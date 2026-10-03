import { expect, test } from "bun:test";

test("installed family roots choose a default without a transport flag", async () => {
  const actors = await import("../../../typescript/packages/actors/dist/index.js");
  const workers = await import("../../../typescript/packages/workers/dist/index.js");
  const objects = await import("../../../typescript/packages/objects/dist/index.js");
  const inference = await import("../../../typescript/packages/inference/dist/index.js");
  const machines = await import("../../../typescript/packages/machines/dist/index.js");

  const clients = [
    ["actors", await actors.fromEnv({ endpoint: "https://actors.example", token: "fixture" })],
    ["workers", await workers.fromEnv({ endpoint: "https://workers.example", token: "fixture" })],
    ["objects", await objects.fromEnv({ endpoint: "https://objects.example", token: "fixture" })],
    ["inference", inference.fromEnv({ endpoint: "https://inference.example", token: "fixture" })],
    ["machines", machines.Machines.fromEnv({ endpoint: "https://machines.example", token: "fixture" })],
  ] as const;

  for (const [family, client] of clients) {
    expect(client, family).toBeDefined();
    expect(client.constructor.name, family).not.toBe("");
  }
  expect(actors.ACTORS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(workers.WORKERS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(objects.OBJECTS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(inference.INFERENCE_REMOTE_POLICY.transport.native[0]?.kind).toBe("http");
  expect(machines.Machines.fromEnv).toBeFunction();
});

test("Rust-emitted remote policy metadata is present for generated HTTP families", async () => {
  const families = [
    ["actors", "ACTORS_REMOTE_POLICY"],
    ["workers", "WORKERS_REMOTE_POLICY"],
    ["objects", "OBJECTS_REMOTE_POLICY"],
    ["inference", "INFERENCE_REMOTE_POLICY"],
  ] as const;
  for (const [family, policy] of families) {
    const source = await Bun.file(`typescript/packages/${family}/src/generated-client.ts`).text();
    expect(source, family).toContain(`export const ${policy}`);
    expect(source, family).toContain("credentialPolicy: \"bearer-no-crlf\"");
    expect(source, family).toContain("responseLimitPolicy: \"bounded-cumulative-utf8\"");
  }
});
