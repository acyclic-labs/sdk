import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { InvokeDeploymentRequestSchema, InvokeVersionRequestSchema, type RustOwnedPublicInvokeDeploymentRequest, type RustOwnedPublicInvokeVersionRequest } from "../src/index.js";
import { createWorkersGrpcClient } from "../src/grpc.js";

const clientOptions = { endpoint: "https://127.0.0.1:9", token: "fixture-token" };

describe("Workers gRPC Rust-owned admission", () => {
  test("rejects invalid pinned-version invocation before opening the application RPC", async () => {
    const client = createWorkersGrpcClient(clientOptions);
    const invalidVersion = create(InvokeVersionRequestSchema, {
      versionSha256: new Uint8Array(31), method: "GET", url: "https://example.test/",
    }) as unknown as RustOwnedPublicInvokeVersionRequest;

    await expect(client.invokeVersion(invalidVersion)).rejects.toThrow(TypeError);
  });

  test("rejects path-like deployment aliases before opening the application RPC", async () => {
    const client = createWorkersGrpcClient(clientOptions);
    const invalidDeployment = create(InvokeDeploymentRequestSchema, {
      alias: "..", method: "GET", url: "https://example.test/",
    }) as unknown as RustOwnedPublicInvokeDeploymentRequest;

    await expect(client.invokeDeployment(invalidDeployment)).rejects.toThrow(TypeError);
  });

  test("defers endpoint validation until the first RPC", async () => {
    const client = createWorkersGrpcClient({ ...clientOptions, endpoint: "http://127.0.0.1:9" });
    const request = create(InvokeDeploymentRequestSchema, { alias: "stable", method: "GET", url: "https://example.test/" }) as unknown as RustOwnedPublicInvokeDeploymentRequest;

    await expect(client.invokeDeployment(request)).rejects.toThrow(TypeError);
  });

  test("defers credential validation until the first RPC", async () => {
    const client = createWorkersGrpcClient({ ...clientOptions, token: "fixture\r\nforged" });
    const request = create(InvokeDeploymentRequestSchema, { alias: "stable", method: "GET", url: "https://example.test/" }) as unknown as RustOwnedPublicInvokeDeploymentRequest;

    await expect(client.invokeDeployment(request)).rejects.toThrow(TypeError);
  });
});
