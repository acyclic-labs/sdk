import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { OBJECTS_REMOTE_POLICY } from "../../../typescript/packages/objects/src/generated-client.js";
import { STREAM_REMOTE_POLICY } from "../../../typescript/packages/stream/src/generated-client.js";
import { INFERENCE_REMOTE_POLICY } from "../../../typescript/packages/inference/src/generated-client.js";

const root = (path: string) => fileURLToPath(new URL(`../../../${path}`, import.meta.url));

test("Rust transport defaults preserve runtime and streaming capability order", async () => {
  const rust = await readFile(root("rust/crates/sdk-contract-wire/src/transport.rs"), "utf8");
  expect(rust).toContain("const OBJECTS_NATIVE: &[TransportOption] = &[GRPC, HTTP_JSON];");
  expect(rust).toContain("const OBJECTS_BROWSER: &[TransportOption] = &[HTTP_JSON];");
  expect(rust).toContain("const STREAM_NATIVE: &[TransportOption] = &[GRPC, HTTP_JSON];");
  expect(rust).toContain("const STREAM_BROWSER: &[TransportOption] = &[HTTP_JSON];");
  expect(OBJECTS_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["grpc", "http"]);
  expect(OBJECTS_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  expect(STREAM_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["grpc", "http"]);
  expect(STREAM_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
});

test("explicit overrides and fail-closed auth/replay rules are present in the Rust selector", async () => {
  const rust = await readFile(root("rust/crates/sdk-contract-wire/src/transport.rs"), "utf8");
  const normalized = rust.replace(/\/{2,}/g, " ").replace(/\s+/g, " ");
  expect(rust).toContain("pub override_kind: Option<TransportKind>");
  expect(rust).toContain("TransportSelectionError::UnsupportedOverride");
  expect(normalized).toContain("Selection does not retry, downgrade after an authorization or application error, or replay a non-idempotent operation.");
  expect(rust).toContain("NoCompatibleTransport");
});

test("generated family policies preserve Rust transport provenance and runtime/TLS boundaries", async () => {
  const emitter = await readFile(root("rust/crates/sdk-typescript/src/main.rs"), "utf8");
  const actors = await readFile(root("typescript/packages/actors/src/generated-client.ts"), "utf8");
  const workers = await readFile(root("typescript/packages/workers/src/generated-client.ts"), "utf8");
  const stream = await readFile(root("typescript/packages/stream/src/client.ts"), "utf8");
  const inference = await readFile(root("typescript/packages/inference/src/generated-client.ts"), "utf8");
  const transport = await readFile(root("rust/crates/sdk-contract-wire/src/transport.rs"), "utf8");
  const inferenceHost = await readFile(root("rust/crates/inference/src/host.rs"), "utf8");

  expect(emitter).toContain("transport_policy_metadata(spec.transport)");
  expect(actors).toContain("ACTORS_REMOTE_POLICY");
  expect(stream).toContain("environment?.transport");
  expect(workers).toContain("WORKERS_REMOTE_POLICY");
  expect(emitter).toContain("inference_transport_policy_metadata(spec.transport)");
  expect(inference).toContain("INFERENCE_REMOTE_POLICY");
  expect(inference).toContain('sourceKind: "rust-model"');
  expect(inference).toContain("sourceArtifact: \"acyclic_sdk_contract_wire::inference::inference_descriptor\"");
  expect(INFERENCE_REMOTE_POLICY.protocol).toBe("https");
  expect(INFERENCE_REMOTE_POLICY.behaviorBinding).toBe("generated-client");
  expect(INFERENCE_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["http"]);
  expect(INFERENCE_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  expect(transport).not.toContain("ca_pem");
  expect(inferenceHost).toContain('if endpoint.uri().scheme_str() != Some("https")');
  expect(inferenceHost).toContain("ca_pem.is_empty()");
});
