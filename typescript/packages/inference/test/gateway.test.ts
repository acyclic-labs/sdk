import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:https";
import type { IncomingMessage, ServerResponse } from "node:http";
import { fileURLToPath } from "node:url";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { GatewayInferenceClient, InferenceTransportError } from "../src/index.js";

// Reuse the maintained Rust TLS identity producer. The client performs real
// HTTPS with this exact declared root; certificate verification stays enabled.
const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], {
  cwd: fileURLToPath(new URL("../../../../", import.meta.url)), encoding: "utf8",
});
if (generated.status !== 0) throw new Error(generated.stderr || "TLS identity producer failed");
const identity: { key: string; certificate: string } = JSON.parse(generated.stdout);

interface GatewayFixture {
  client: GatewayInferenceClient;
  endpoint: string;
  close: () => Promise<void>;
}
async function fixture(handler: (request: IncomingMessage, response: ServerResponse) => void): Promise<GatewayFixture> {
  const server = createServer({ key: identity.key, cert: identity.certificate }, handler);
  const close = ownFixtureServer(server);
  server.listen(0, "localhost");
  await once(server, "listening");
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("TLS fixture has no TCP address");
  const endpoint = `https://localhost:${address.port}`;
  const trustedFetch = ((input, init) => fetch(input, { ...init, tls: { ca: identity.certificate } })) as typeof fetch;
  return { endpoint, close, client: new GatewayInferenceClient(endpoint, () => ({ authorization: "Bearer test-customer" }), trustedFetch) };
}

test("real TLS SSE remains incremental across a fragmented UTF-8 scalar", async () => {
  const text = 'data: {"text":"é🙂"}\n\ndata: [DONE]\n\n';
  const bytes = Buffer.from(text);
  const split = bytes.indexOf(Buffer.from("é")) + 1;
  let finish: () => void = () => { throw new Error("stream has not started"); };
  const server = await fixture((_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write(bytes.subarray(0, split));
    finish = () => response.end(bytes.subarray(split));
  });
  try {
    const response = await server.client.chatCompletions('{"stream":true}');
    expect(response.bodyUsed).toBe(false);
    const reader = response.body!.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let received = 0;
    let decoded = "";
    while (received < split) {
      const chunk = await reader.read();
      expect(chunk.done).toBe(false);
      received += chunk.value!.byteLength;
      decoded += decoder.decode(chunk.value, { stream: true });
    }
    // This is before the upstream has produced the remainder or ended its body.
    expect(decoded).toBe('data: {"text":"');
    finish();
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      decoded += decoder.decode(chunk.value, { stream: true });
    }
    decoded += decoder.decode();
    expect(decoded).toBe(text);
    reader.releaseLock();
  } finally { await server.close(); }
}, 10000);

test("real upstream HTTP error and malformed JSON remain visible to the consumer", async () => {
  const server = await fixture((_request, response) => {
    response.writeHead(429, { "retry-after": "7", "content-type": "application/json" });
    response.end('{"error":');
  });
  try {
    const response = await server.client.responses("{}");
    expect(response.ok).toBe(false);
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("7");
    await expect(response.json()).rejects.toBeInstanceOf(SyntaxError);
  } finally { await server.close(); }
}, 10000);

test("consumer AbortSignal closes an actual upstream streaming connection", async () => {
  let closed: Promise<unknown> | undefined;
  const server = await fixture((_request, response) => {
    closed = once(response, "close");
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write("data: waiting\n\n");
  });
  try {
    const controller = new AbortController();
    const response = await server.client.messages('{"stream":true}', { signal: controller.signal });
    const reader = response.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("data: waiting\n\n");
    const pending = reader.read();
    controller.abort();
    await expect(pending).rejects.toHaveProperty("name", "AbortError");
    if (closed === undefined) throw new Error("upstream stream was never opened");
    await closed;
    reader.releaseLock();
  } finally { await server.close(); }
}, 10000);

test("unexpected TLS body disconnect rejects instead of becoming clean end-of-stream", async () => {
  let disconnect: () => void = () => { throw new Error("stream has not started"); };
  const server = await fixture((_request, response) => {
    response.writeHead(200, { "content-length": "100" });
    response.write("partial");
    disconnect = () => response.destroy();
  });
  try {
    const response = await server.client.models();
    const reader = response.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("partial");
    disconnect();
    await expect(reader.read()).rejects.toThrow();
    reader.releaseLock();
  } finally { await server.close(); }
}, 10000);

test("HTTPS origin and bounded customer bearer failures occur before any actual request", async () => {
  let requests = 0;
  const server = await fixture((_request, response) => { requests++; response.end(); });
  try {
    for (const endpoint of ["http://gateway.example", "https://gateway.example/v1", "https://user:pass@gateway.example", "https://gateway.example?query", "https://gateway.example#fragment"]) {
      expect(() => new GatewayInferenceClient(endpoint, () => ({ authorization: "Bearer test-customer" }))).toThrow(TypeError);
    }
    for (const authorization of ["", "Basic token", `Bearer ${"x".repeat(12 * 1024 + 1)}`]) {
      const client = new GatewayInferenceClient(server.endpoint, () => ({ authorization }));
      await expect(client.models()).rejects.toBeInstanceOf(InferenceTransportError);
    }
    expect(requests).toBe(0);
  } finally { await server.close(); }
}, 10000);
