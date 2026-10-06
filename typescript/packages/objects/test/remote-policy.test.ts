import { expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v2/objects_pb.js";
import { HttpObjectsV2 } from "../src/v2-http.js";
import { OBJECTS_REMOTE_POLICY, OBJECTS_ROUTES, OBJECTS_SOURCE } from "../src/generated-client.js";

test("Objects remote policy is Rust-owned and keeps complex behavior native/WASM", () => {
  expect(OBJECTS_SOURCE.sourceKind).toBe("rust-model");
  expect(OBJECTS_REMOTE_POLICY.behaviorBinding).toBe("native-wasm");
  expect(OBJECTS_REMOTE_POLICY.protocol).toBe("https-or-loopback-http");
  expect(Object.keys(OBJECTS_ROUTES)).toHaveLength(13);
  expect(OBJECTS_ROUTES.putObject.clientStreaming).toBeTrue();
  expect(OBJECTS_ROUTES.getObject.serverStreaming).toBeTrue();
});

test("Objects HTTP endpoint policy is shared with the Rust/WASM boundary", () => {
  expect(() => new HttpObjectsV2({ endpoint: "https://objects.example", token: "fixture" })).not.toThrow();
  expect(() => new HttpObjectsV2({ endpoint: "http://127.0.0.1:8080", token: "fixture" })).not.toThrow();
  for (const endpoint of [
    "http://objects.example",
    "https://user@objects.example",
    "https://objects.example/?query=1",
    "https://objects.example/#fragment",
  ]) {
    expect(() => new HttpObjectsV2({ endpoint, token: "fixture" })).toThrow("invalid Objects HTTP endpoint");
  }
});

test("Objects HTTP forwards caller cancellation to the Rust-owned operation", async () => {
  const client = new HttpObjectsV2({
    endpoint: "http://127.0.0.1:1",
    token: "fixture",
    fetch: async (_input, init) => await new Promise<Response>((_resolve, reject) => {
      const abort = () => reject(new DOMException("aborted", "AbortError"));
      if (init?.signal?.aborted) abort();
      else init?.signal?.addEventListener("abort", abort, { once: true });
    }),
  });
  const controller = new AbortController();
  const pending = client.createBucket(create(wire.CreateBucketRequestSchema, { name: "customer.inputs" }), controller.signal);
  controller.abort();
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });
});
