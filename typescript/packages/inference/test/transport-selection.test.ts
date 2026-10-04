import { expect, test } from "bun:test";
import {
  fromEnv,
  HttpInferenceTransport,
  InferenceClient,
  INFERENCE_REMOTE_POLICY,
} from "../src/index.js";
import { INFERENCE_HANDSHAKE } from "../src/generated-client.js";

test("Rust-owned Inference policy selects the installed HTTP adapter in Node and browser", () => {
  expect(INFERENCE_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["http"]);
  expect(INFERENCE_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  const client = fromEnv({ endpoint: "https://inference.example", token: "fixture" });
  expect(client).toBeInstanceOf(InferenceClient);
  expect(client.transport).toBeInstanceOf(HttpInferenceTransport);
});

test("Inference rejects an unavailable gRPC override before endpoint access", () => {
  expect(() => fromEnv({ endpoint: "not-an-endpoint", token: "", transport: "grpc" }))
    .toThrow("transport grpc is unavailable");
});

test("Inference negotiates the authenticated Rust handshake before application calls", async () => {
  const calls: string[] = [];
  const client = fromEnv({
    endpoint: "https://inference.example",
    token: "fixture-token",
    fetcher: async (input, init) => {
      calls.push(String(input));
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer fixture-token");
      if (String(input).endsWith(INFERENCE_HANDSHAKE.route)) {
        return Response.json({
          protocol: { version: INFERENCE_HANDSHAKE.version, descriptorDigest: INFERENCE_HANDSHAKE.descriptorDigest },
          supported: { capabilities: [] },
        });
      }
      return Response.json({ models: [] });
    },
  });

  await client.listModels();
  expect(calls).toEqual([
    `https://inference.example${INFERENCE_HANDSHAKE.route}`,
    "https://inference.example/v1/inference/models/list",
  ]);
});

test("Inference does not issue an application call after handshake identity failure", async () => {
  const calls: string[] = [];
  const client = fromEnv({
    endpoint: "https://inference.example",
    token: "fixture-token",
    fetcher: async (input) => {
      calls.push(String(input));
      return Response.json({ protocol: { version: "wrong", descriptorDigest: "wrong" } });
    },
  });

  await expect(client.listModels()).rejects.toThrow("handshake identity mismatch");
  expect(calls).toHaveLength(1);
  expect(calls[0]).toContain(INFERENCE_HANDSHAKE.route);
});
