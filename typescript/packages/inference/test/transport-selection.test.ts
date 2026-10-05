import { expect, test } from "bun:test";
import {
  fromEnv,
  HttpInferenceTransport,
  InferenceClient,
  INFERENCE_REMOTE_POLICY,
} from "../src/index.js";

test("Rust-owned Inference policy selects the installed HTTP adapter in Node and browser", () => {
  expect(INFERENCE_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["http"]);
  expect(INFERENCE_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  const client = fromEnv({ endpoint: "https://inference.example", token: "fixture" });
  expect(client).toBeInstanceOf(InferenceClient);
  expect(client.transport).toBeInstanceOf(HttpInferenceTransport);
});

test("Inference rejects an unavailable gRPC override before endpoint access", () => {
  expect(() => fromEnv({ endpoint: "not-an-endpoint", token: "", transport: "grpc" }))
    .toThrow("Inference transport grpc is unavailable");
});
