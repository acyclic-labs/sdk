import { expect, test } from "bun:test";
import { GrpcStreamProvider } from "../src/grpc.js";
import { HttpStreamProvider } from "../src/http.js";
import { StreamClient } from "../src/client.js";

const environment = { endpoint: "https://stream.example", token: "fixture" };

test("native default selects the Rust-qualified gRPC transport", async () => {
  const client = await StreamClient.fromEnv(environment);
  expect(client.provider).toBeInstanceOf(GrpcStreamProvider);
});

test("native callers may explicitly override the Rust-qualified default with HTTP", async () => {
  const client = await StreamClient.fromEnv({ ...environment, transport: "http" });
  expect(client.provider).toBeInstanceOf(HttpStreamProvider);
});

test("browser default selects HTTP and rejects native gRPC overrides", async () => {
  const runtime = globalThis as typeof globalThis & { process?: unknown };
  const descriptor = Object.getOwnPropertyDescriptor(runtime, "process");
  Object.defineProperty(runtime, "process", { value: undefined, configurable: true, writable: true });
  try {
    const client = await StreamClient.fromEnv(environment);
    expect(client.provider).toBeInstanceOf(HttpStreamProvider);
    await expect(StreamClient.fromEnv({ ...environment, transport: "grpc" })).rejects.toMatchObject({ code: "unsupported" });
  } finally {
    if (descriptor === undefined) delete runtime.process;
    else Object.defineProperty(runtime, "process", descriptor);
  }
});
