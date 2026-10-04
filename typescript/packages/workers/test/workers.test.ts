import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { HttpWorkersClient, InvokeVersionRequestSchema, InvokeDeploymentRequestSchema, SelectDeploymentRequestSchema } from "../src/index.js";
import { WORKERS_HANDSHAKE } from "../src/generated-client.js";

function handshakeResponse(): Response {
  return Response.json({ protocol: { version: WORKERS_HANDSHAKE.version, descriptorDigest: WORKERS_HANDSHAKE.descriptorDigest }, supported: {} });
}

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
        if (new URL(String(input)).pathname === WORKERS_HANDSHAKE.route) return handshakeResponse();
        seen.push(String(input));
        expect(new Headers(init?.headers).get("authorization")).toBe("Bearer secret");
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
  test("uses canonical protobuf JSON for bytes, uint64, and optional presence", async () => {
    let body = "";
    const client = new HttpWorkersClient({
      endpoint: "https://workers.example.test/",
      token: "secret",
      fetcher: async (_input, init) => {
        if (new URL(String(_input)).pathname === WORKERS_HANDSHAKE.route) return handshakeResponse();
        body = String(init?.body);
        return new Response(JSON.stringify({ deployment: { alias: "current", revision: "9007199254740993" } }));
      },
    });
    await client.selectDeployment(create(SelectDeploymentRequestSchema, {
      alias: "current",
      versionSha256: new Uint8Array([0, 255, 16]),
      expectedRevision: 9007199254740993n,
      idempotencyKey: "select-current",
    }));
    expect(body).toContain('"versionSha256":"AP8Q"');
    expect(body).toContain('"expectedRevision":"9007199254740993"');
    expect(body).toContain('"idempotencyKey":"select-current"');
    // The absent proto3 optional field stays absent; a JSON null would change
    // presence semantics at the service boundary.
    let absent = "";
    const absentClient = new HttpWorkersClient({
      endpoint: "https://workers.example.test/",
      token: "secret",
      fetcher: async (_input, init) => { if (new URL(String(_input)).pathname === WORKERS_HANDSHAKE.route) return handshakeResponse(); absent = String(init?.body); return new Response("{}"); },
    });
    await absentClient.selectDeployment(create(SelectDeploymentRequestSchema, { alias: "current" }));
    expect(absent).not.toContain("expectedRevision");
  });
  test("runs path interpolation and bearer auth against a real loopback server", async () => {
    const server = Bun.serve({
      port: 0,
      fetch: async request => {
        if (new URL(request.url).pathname === WORKERS_HANDSHAKE.route) return handshakeResponse();
        expect(new URL(request.url).pathname).toBe(`/v1/workers/deployments/current/invoke`);
        expect(request.headers.get("authorization")).toBe("Bearer loopback-token");
        return Response.json({});
      },
    });
    try {
      const client = new HttpWorkersClient({ endpoint: `http://127.0.0.1:${server.port}/`, token: "loopback-token" });
      await client.invokeDeployment(create(InvokeDeploymentRequestSchema, { alias: "current" }));
    } finally {
      server.stop(true);
    }
  });
  test("forwards request cancellation to the Rust-owned HTTP operation", async () => {
    const controller = new AbortController();
    let forwarded: AbortSignal | null | undefined;
    let rejectRequest: (reason?: unknown) => void = () => {};
    const pending = new Promise<Response>((_, reject) => { rejectRequest = reject; });
    const client = new HttpWorkersClient({
      endpoint: "https://workers.example.test/",
      token: "secret",
      fetcher: async (_input, init) => {
        if (new URL(String(_input)).pathname === WORKERS_HANDSHAKE.route) return handshakeResponse();
        forwarded = init?.signal;
        if (init?.signal?.aborted) return Promise.reject(new Error("fixture request aborted"));
        init?.signal?.addEventListener("abort", () => rejectRequest(new Error("fixture request aborted")), { once: true });
        return pending;
      },
    });
    const request = client.invokeDeployment(create(InvokeDeploymentRequestSchema, { alias: "current" }), controller.signal);
    controller.abort();
    await expect(request).rejects.toThrow("fixture request aborted");
    expect(forwarded).toBe(controller.signal);
  });
});
