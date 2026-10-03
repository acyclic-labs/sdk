import { expect, test } from "bun:test";
import {
  ACTORS_METHODS,
  ACTORS_OPERATIONS,
  ACTORS_REMOTE_POLICY,
} from "../../../typescript/packages/actors/src/generated-client.js";
import {
  WORKERS_METHODS,
  WORKERS_OPERATIONS,
  WORKERS_REMOTE_POLICY,
} from "../../../typescript/packages/workers/src/generated-client.js";
import {
  OBJECTS_METHODS,
  OBJECTS_OPERATIONS,
  OBJECTS_REMOTE_POLICY,
} from "../../../typescript/packages/objects/src/generated-client.js";

const read = (path: string) => Bun.file(path).text();

test("factory selectors keep native gRPC behind a browser-safe dynamic boundary", async () => {
  const selectors = await Promise.all([
    read("typescript/packages/actors/src/client.ts"),
    read("typescript/packages/workers/src/client.ts"),
    read("typescript/packages/objects/src/v2-client.ts"),
  ]);
  for (const source of selectors) {
    expect(source).toContain("globalThis");
    expect(source).toContain("process?.versions");
    expect(source).toContain("await import(");
    expect(source).not.toContain("node:");
  }
  const nativeGrpc = await Promise.all([
    read("typescript/packages/actors/src/grpc.ts"),
    read("typescript/packages/workers/src/grpc.ts"),
    read("typescript/packages/objects/src/v2-grpc.ts"),
  ]);
  for (const source of nativeGrpc) expect(source).toContain("node:tls");
  expect(ACTORS_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  expect(WORKERS_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  expect(OBJECTS_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
});

test("generated operation metadata covers response bounds, idempotency, and streaming", () => {
  const families = [
    [ACTORS_METHODS, ACTORS_OPERATIONS],
    [WORKERS_METHODS, WORKERS_OPERATIONS],
    [OBJECTS_METHODS, OBJECTS_OPERATIONS],
  ] as const;
  for (const [methods, operations] of families) {
    for (const method of Object.values(methods)) {
      expect(method.responseLimitPolicy).toBe("bounded-cumulative-utf8");
      expect(method.credentialPolicy).toBe("bearer-no-crlf");
      expect(method.requestEncoding).toBe("protobuf-json");
      expect(method.responseEncoding).toBe("protobuf-json");
    }
    for (const operation of Object.values(operations)) {
      expect(operation.rpc).toContain("/");
      expect(operation.errors.length).toBeGreaterThan(0);
      expect(operation.validations).toBeArray();
    }
  }
  expect(Object.values(OBJECTS_METHODS).filter(method => method.clientStreaming)).toHaveLength(2);
  expect(Object.values(OBJECTS_METHODS).filter(method => method.serverStreaming)).toHaveLength(1);
  expect(Object.values(OBJECTS_OPERATIONS).filter(operation => operation.capabilities.some(capability => capability.endsWith("idempotent_mutation")))).toHaveLength(8);
  expect(Object.values(ACTORS_OPERATIONS).some(operation => operation.capabilities.includes("actors.idempotent_mutation"))).toBe(true);
  expect(Object.values(WORKERS_OPERATIONS).some(operation => operation.capabilities.includes("workers.idempotent_mutation"))).toBe(true);
});

test("generated transport policies expose all supported family operations without consumer flags", () => {
  expect(ACTORS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(WORKERS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(OBJECTS_REMOTE_POLICY.transport.native[0]?.kind).toBe("grpc");
  expect(ACTORS_REMOTE_POLICY.transport.native.every(option => option.bearerAuth)).toBe(true);
  expect(WORKERS_REMOTE_POLICY.transport.native.every(option => option.bearerAuth)).toBe(true);
  expect(OBJECTS_REMOTE_POLICY.transport.native.every(option => option.bearerAuth)).toBe(true);
  expect(ACTORS_REMOTE_POLICY.transport.native.every(option => option.streaming === false)).toBe(true);
  expect(WORKERS_REMOTE_POLICY.transport.native.every(option => option.streaming === false)).toBe(true);
  expect(OBJECTS_REMOTE_POLICY.transport.native.every(option => option.streaming)).toBe(true);
});
