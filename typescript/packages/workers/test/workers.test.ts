import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { HttpWorkersClient, InvokeVersionRequestSchema, InvokeDeploymentRequestSchema } from "../src/index.js";

describe("Workers v1 generated transport", () => {
  test("permits token-protected loopback HTTP but rejects remote plaintext", () => {
    expect(() => new HttpWorkersClient({ endpoint: "http://127.0.0.1:8787", token: "local" })).not.toThrow();
    expect(() => new HttpWorkersClient({ endpoint: "http://workers.example.test", token: "remote" })).toThrow(TypeError);
  });
  test("rejects path-like deployment aliases before sending a request", async () => {
    const client = new HttpWorkersClient({ endpoint: "https://workers.example.test", token: "secret", fetcher: async () => { throw new Error("unexpected fetch"); } });
    await expect(client.invokeDeployment(create(InvokeDeploymentRequestSchema, { alias: ".." }))).rejects.toThrow(TypeError);
  });
  test("keeps pinned and alias invocations on different Rust-owned routes", async () => {
    const seen: string[] = [];
    const client = new HttpWorkersClient({
      endpoint: "https://workers.example.test/api/",
      token: "secret",
      fetcher: async (input, init) => {
        seen.push(String(input));
        expect(init?.headers).toMatchObject({ authorization: "Bearer secret" });
        return new Response(JSON.stringify({ status: 200, resolvedSha256: "AQ==", resolvedRevision: "4" }));
      },
    });
    await client.invokeVersion(create(InvokeVersionRequestSchema, {
      versionSha256: new Uint8Array(32).fill(1), method: "GET", url: "https://example.test/",
    }));
    await client.invokeDeployment(create(InvokeDeploymentRequestSchema, {
      alias: "current", method: "GET", url: "https://example.test/",
    }));
    expect(seen[0]).toEndWith(`/v1/workers/versions/${"01".repeat(32)}/invoke`);
    expect(seen[1]).toEndWith("/v1/workers/deployments/current/invoke");
  });
});
