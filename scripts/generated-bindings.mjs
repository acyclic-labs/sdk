import { existsSync, readFileSync, writeFileSync } from "node:fs";

export const writeChanged = (path, content) => {
  const bytes = typeof content === "string" ? Buffer.from(content) : content;
  if (!existsSync(path) || !readFileSync(path).equals(bytes)) writeFileSync(path, bytes);
};

// Canonical generation inputs and their published copies live here.
// Every family that negotiates a protocol ships the shared handshake schema.
/** @type {[string, string[]][]} */
export const packagedTypeScriptBindings = [
  ["filesystem/v2/filesystem_pb", ["filesystem"]],
  ["protocol/v1/protocol_pb", ["filesystem"]],
  // inference_pb imports the proto2 custom options descriptor transitively.
  ["validation/v1/options_pb", ["inference", "actors", "filesystem", "machines", "objects", "stream"]],
  ["auth/v1/auth_pb", ["auth"]],
  ["inference/v1/inference_pb", ["inference"]],
  ["machines/v1/machines_pb", ["machines"]],
  ["objects/v1/objects_pb", ["objects"]],
  ["stream/v1/stream_pb", ["stream"]],
  ["stream/v1/session_pb", ["stream"]],
  ["actors/v1/actors_pb", ["actors"]],
];

// These packages intentionally own no protobuf outputs after protocol retirement.
export const retiredTypeScriptBindingPackages = ["harness"];

export const compatibilityArtifacts = {
  auth: {
    schemaDigest: "rust/crates/auth/proto/auth/v1/auth.proto",
    descriptorDigest: "compatibility/descriptors/auth-v1.bin",
  },
  harness: {
    schemaDigest: "rust/crates/harness-core/proto/harness/v2/harness.proto",
    conformanceDigest: "conformance/vectors/core.json",
  },
  filesystem: {
    schemaDigest: "rust/crates/filesystem-wire/proto/filesystem/v2/filesystem.proto",
    descriptorDigest: "compatibility/descriptors/filesystem-v2.bin",
    conformanceDigest: "conformance/vectors/filesystem/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v1/stream.proto",
    descriptorDigest: "compatibility/descriptors/stream-v1.bin",
    conformanceDigest: "rust/crates/stream/conformance/stream.json",
  },
  objects: {
    schemaDigest: "rust/crates/objects/proto/objects/v1/objects.proto",
    descriptorDigest: "compatibility/descriptors/objects-v1.bin",
    conformanceDigest: "rust/crates/objects/conformance/objects-v1.json",
  },
  machines: {
    schemaDigest: "rust/crates/machines/proto/machines/v1/machines.proto",
    descriptorDigest: "compatibility/descriptors/machines-v1.bin",
    conformanceDigest: "conformance/vectors/machines.json",
  },
  inference: {
    schemaDigest: "rust/crates/inference/proto/inference/v1/inference.proto",
    descriptorDigest: "compatibility/descriptors/inference-v1.bin",
    conformanceDigest: "conformance/vectors/inference.json",
  },
  actors: {
    schemaDigest: "rust/crates/actors/proto/actors/v1/actors.proto",
    descriptorDigest: "compatibility/descriptors/actors-v1.bin",
  },
};

export const nativeWasmVector = "conformance/vectors/harness/native-wasm-event-v2.json";
export const packagedSourceCopies = [
  [compatibilityArtifacts.actors.descriptorDigest, "rust/crates/actors/tests/fixtures/actors/v1/actors-v1.bin"],
  ...["actors", "filesystem-wire", "inference", "machines", "objects", "stream"].map(crate => [
    "rust/crates/contract-derive/proto/validation/v1/options.proto", `rust/crates/${crate}/proto/validation/v1/options.proto`,
  ]),
  ...[
    "conversation-message", "conversation-kinds", "file-ref", "file-ref-security-cases", "private-directory-page", "task-outcome",
    "extension-record", "extension-state-migration", "extension-dependency", "extension-configuration", "extension-admission", "resource-revision", "fork-request", "fork-seed", "reference-grant",
    "interaction-resolution", "project-merge-receipt",
  ].map(name => [`fixtures/harness/v2/${name}.json`, `rust/crates/harness/fixtures/v2/${name}.json`]),
  ["rust/crates/harness-core/proto/protocol/v1/protocol.proto", "rust/crates/filesystem-wire/proto/protocol/v1/protocol.proto"],
  [compatibilityArtifacts.harness.conformanceDigest, "rust/crates/conformance/vectors/harness.json"],
  [nativeWasmVector, "rust/crates/harness/conformance/native-wasm-event-v2.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  [compatibilityArtifacts.filesystem.conformanceDigest, "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
];
