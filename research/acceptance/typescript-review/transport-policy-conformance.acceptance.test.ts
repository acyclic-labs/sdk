import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
}

test("HTTP adapters retain Rust bounded-response and caller-retry policy", async () => {
  const [actorsRust, workersRust, actorsTs, workersTs, machinesTs, streamRust, streamTs, rustEmitter, actorsMetadata, workersMetadata] = await Promise.all([
    source("rust/crates/actors/src/http.rs"),
    source("rust/crates/workers/src/http.rs"),
    source("typescript/packages/actors/src/http.ts"),
    source("typescript/packages/workers/src/http.ts"),
    source("typescript/packages/machines/src/http.ts"),
    source("rust/crates/stream/src/http.rs"),
    source("typescript/packages/stream/src/http.ts"),
    source("rust/crates/sdk-typescript/src/main.rs"),
    source("typescript/packages/actors/src/generated-client.ts"),
    source("typescript/packages/workers/src/generated-client.ts"),
  ]);

  for (const rust of [actorsRust, workersRust]) {
    expect(rust).toContain("ResponseTooLarge");
    expect(rust).toContain("maximum");
  }
  expect(streamRust).toContain("Mutations are never automatically retried");
  expect(streamRust).toContain("saturating_sub(body.len())");
  expect(streamRust).toContain("maximum");
  for (const typescript of [actorsTs, workersTs, machinesTs, streamTs]) {
    expect(typescript).toContain("maximumResponseBytes");
    expect(typescript).toContain("response exceeds configured bound");
  }
  // The shared response-limit/auth labels must originate in the Rust emitter
  // and survive into each generated family metadata table. The HTTP adapters
  // consume the bounded-response behavior while the generated table remains
  // the source-bound contract surface for later policy reuse.
  expect(rustEmitter).toContain('response_limit_policy: "bounded-cumulative-utf8".to_owned()');
  expect(rustEmitter).toContain('auth: "bearer".to_owned()');
  for (const metadata of [actorsMetadata, workersMetadata]) {
    expect(metadata).toContain('auth: "bearer"');
    expect(metadata).toContain('responseLimitPolicy: "bounded-cumulative-utf8"');
  }
  // HTTP adapters preserve caller-owned retry identity. Stream gRPC has a
  // separate endpoint-failover policy, covered by the next test.
  expect(actorsTs).not.toMatch(/(?:retry|backoff)\s*\(/i);
  expect(workersTs).not.toMatch(/(?:retry|backoff)\s*\(/i);
  expect(streamTs).not.toContain("RETRY_DELAY");
});

test("Stream polling recovery stays HTTP-specific while gRPC owns failover", async () => {
  const [rustHttp, rustGrpc, typescript] = await Promise.all([
    source("rust/crates/stream/src/http.rs"),
    source("rust/crates/stream/src/grpc.rs"),
    source("typescript/packages/stream/src/http.ts"),
  ]);

  expect(rustHttp).toContain("sleep(Duration::from_millis(250))");
  expect(typescript).toContain("delay(250, signal)");
  expect(rustHttp).not.toContain("RETRY_DELAY");
  expect(typescript).not.toContain("retryable");

  expect(rustGrpc).toContain("const RETRY_DELAY");
  expect(rustGrpc).toContain("fn retryable");
  expect(rustGrpc).toContain("OPERATION_DEADLINE");
  expect(rustGrpc).toContain("advance_follow");
});

test("Rust credential policy is projected into TypeScript transport facades", async () => {
  const [actorsRust, workersRust, emitter, actorsTs, workersTs] = await Promise.all([
    source("rust/crates/actors/src/http.rs"),
    source("rust/crates/workers/src/http.rs"),
    source("rust/crates/sdk-typescript/src/main.rs"),
    source("typescript/packages/actors/src/http.ts"),
    source("typescript/packages/workers/src/http.ts"),
  ]);

  for (const rust of [actorsRust, workersRust]) {
    expect(rust).toContain("credential::validate(BEARER_NO_CRLF, token)");
  }
  expect(emitter).toContain("credential_policy");
  for (const typescript of [actorsTs, workersTs]) {
    expect(typescript).toContain("validateRustOwnedCredential");
    expect(typescript).not.toContain("if (!options.token.trim())");
    expect(typescript).not.toContain("token.includes");
    expect(typescript).not.toContain("token.contains");
  }
});

test("Cancellation remains an IO adapter concern, while Rust owns operation policy", async () => {
  const [actors, workers, stream, harness] = await Promise.all([
    source("typescript/packages/actors/src/http.ts"),
    source("typescript/packages/workers/src/http.ts"),
    source("typescript/packages/stream/src/http.ts"),
    source("typescript/packages/harness/src/wire-transport.ts"),
  ]);

  expect(actors).toContain("signal,");
  expect(workers).toContain("signal,");
  expect(stream).toContain("signal === undefined ? {} : { signal }");
  expect(harness).toContain("signal?: AbortSignal");
  expect(harness).toContain("ErrorCode.INDETERMINATE");
  // No TypeScript adapter may turn transport cancellation into an implicit
  // mutation retry or a terminal success claim.
  expect(harness).not.toContain("retry(command)");
});
