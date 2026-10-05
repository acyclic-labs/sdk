import { expect, test } from "bun:test";
import {
  fromEnv,
  HttpInferenceTransport,
  InferenceClient,
  RustInferenceTransport,
  INFERENCE_REMOTE_POLICY,
} from "../src/index.js";

test("Rust-owned policy selects the best transport without consumer flags", () => {
  expect(INFERENCE_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["http"]);
  expect(INFERENCE_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  const client = fromEnv({ endpoint: "https://inference.example", token: "fixture" });
  expect(client).toBeInstanceOf(InferenceClient);
  expect(client.transport).toBeInstanceOf(RustInferenceTransport);
  expect(client.transport).not.toBeInstanceOf(HttpInferenceTransport);
});

test("legacy transport settings remain harmless compatibility input", () => {
  const client = fromEnv({ endpoint: "https://inference.example", token: "fixture", transport: "grpc" });
  expect(client.transport).toBeInstanceOf(RustInferenceTransport);
  const compatibility = new HttpInferenceTransport("https://inference.example", "fixture");
  expect(compatibility).toBeInstanceOf(RustInferenceTransport);
});
