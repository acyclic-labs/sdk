import { expect, test } from "bun:test";
import { HttpStreamProvider } from "../src/http.js";
import { STREAM_HANDSHAKE, STREAM_REMOTE_POLICY, STREAM_ROUTES, STREAM_SOURCE } from "../src/generated-client.js";

test("Stream remote policy is Rust-owned and keeps complex behavior native/WASM", () => {
  expect(STREAM_SOURCE.sourceKind).toBe("rust-model");
  expect(STREAM_REMOTE_POLICY.behaviorBinding).toBe("native-wasm");
  expect(STREAM_REMOTE_POLICY.protocol).toBe("https-or-loopback-http");
  expect(STREAM_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(STREAM_REMOTE_POLICY.transport.browser[0]?.kind).toBe("http");
  expect(Object.keys(STREAM_ROUTES)).toHaveLength(10);
  expect(STREAM_ROUTES.read.serverStreaming).toBeTrue();
  expect(STREAM_ROUTES.append.clientStreaming).toBeFalse();
});

test("Stream HTTP forwards caller cancellation to every operation", async () => {
  const client = new HttpStreamProvider({
    endpoint: "https://stream.example",
    token: "fixture",
    fetcher: async (_input, init) => await new Promise<Response>((_resolve, reject) => {
      const abort = () => reject(new DOMException("aborted", "AbortError"));
      if (init?.signal?.aborted) abort();
      else init?.signal?.addEventListener("abort", abort, { once: true });
    }),
  });
  const controller = new AbortController();
  const pending = client.append("events", [new Uint8Array([1])], undefined, controller.signal);
  controller.abort();
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });
});

test("Stream HTTP negotiates the Rust identity before application requests and caches it", async () => {
  const routes: string[] = [];
  const client = new HttpStreamProvider({
    endpoint: "https://stream.example",
    token: "fixture",
    fetcher: async (input, init) => {
      const route = new URL(String(input)).pathname;
      routes.push(route);
      expect(init?.headers).toBeDefined();
      if (route === STREAM_HANDSHAKE.route) {
        expect(init?.method).toBe("POST");
        return Response.json({ protocol: { version: STREAM_HANDSHAKE.version, descriptorDigest: STREAM_HANDSHAKE.descriptorDigest }, supported: {} });
      }
      return new Response('"0"');
    },
  });
  await expect(client.tail("events")).resolves.toBe(0n);
  await expect(client.tail("events")).resolves.toBe(0n);
  expect(routes).toEqual([STREAM_HANDSHAKE.route, "/v1/stream/tail", "/v1/stream/tail"]);
});

test("Stream HTTP treats handshake identity and authorization failures as terminal before application calls", async () => {
  let applicationCalls = 0;
  let handshakes = 0;
  const client = new HttpStreamProvider({
    endpoint: "https://stream.example",
    token: "fixture",
    fetcher: async input => {
      const route = new URL(String(input)).pathname;
      if (route !== STREAM_HANDSHAKE.route) { applicationCalls += 1; return new Response('"0"'); }
      handshakes += 1;
      if (handshakes === 1) return Response.json({ protocol: { version: "wrong", descriptorDigest: STREAM_HANDSHAKE.descriptorDigest }, supported: {} });
      return new Response("unauthorized", { status: 401 });
    },
  });
  await expect(client.tail("events")).rejects.toThrow("identity mismatch");
  await expect(client.tail("events")).rejects.toThrow("HTTP 401");
  expect({ handshakes, applicationCalls }).toEqual({ handshakes: 2, applicationCalls: 0 });
});
