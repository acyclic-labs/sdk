import { describe, expect, test } from "bun:test";
import { create, InvokeDeploymentRequestSchema, InvokeVersionRequestSchema, type RustOwnedPublicInvokeDeploymentRequest, type RustOwnedPublicInvokeVersionRequest } from "../src/index.js";
import { createWorkersGrpcClient } from "../src/grpc.js";

const clientOptions = { endpoint: "http://127.0.0.1:9", token: "fixture-token" };

describe("Workers gRPC Rust-owned admission", () => {
  test("rejects invalid pinned-version invocation before opening the application RPC", () => {
    const client = createWorkersGrpcClient(clientOptions);
    const invalidVersion = create(InvokeVersionRequestSchema, {
      versionSha256: new Uint8Array(31), method: "GET", url: "https://example.test/",
    }) as unknown as RustOwnedPublicInvokeVersionRequest;

    expect(() => client.invokeVersion(invalidVersion)).toThrow(TypeError);
  });

  test("rejects path-like deployment aliases before opening the application RPC", () => {
    const client = createWorkersGrpcClient(clientOptions);
    const invalidDeployment = create(InvokeDeploymentRequestSchema, {
      alias: "..", method: "GET", url: "https://example.test/",
    }) as unknown as RustOwnedPublicInvokeDeploymentRequest;

    expect(() => client.invokeDeployment(invalidDeployment)).toThrow(TypeError);
  });
});
