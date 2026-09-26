// Canonical generation inputs and their published copies live here.
// Filesystem also ships Harness because its generated schema imports Harness messages.
export const packagedTypeScriptBindings = [
  ["filesystem/v2/filesystem_pb", ["filesystem"]],
  ["harness/v2/harness_pb", ["filesystem", "harness"]],
  ["inference/v1/inference_pb", ["inference"]],
  ["machines/v1/machines_pb", ["machines"]],
  ["objects/v1/objects_pb", ["objects"]],
  ["stream/v2/stream_pb", ["stream"]],
];

export const compatibilityArtifacts = {
  harness: {
    schemaDigest: "proto/harness/v2/harness.proto",
    conformanceDigest: "conformance/vectors/core.json",
  },
  filesystem: {
    schemaDigest: "proto/filesystem/v2/filesystem.proto",
    descriptorDigest: "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
    conformanceDigest: "conformance/vectors/filesystem/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v2/stream.proto",
    descriptorDigest: "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
    conformanceDigest: "conformance/vectors/stream.json",
  },
  objects: {
    schemaDigest: "proto/objects/v1/objects.proto",
    descriptorDigest: "rust/crates/objects/src/generated/acyclic-objects-v1.bin",
    conformanceDigest: "conformance/vectors/objects.json",
  },
  machines: {
    schemaDigest: "proto/machines/v1/machines.proto",
    descriptorDigest: "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
    conformanceDigest: "conformance/vectors/machines.json",
  },
  inference: {
    schemaDigest: "proto/inference/v1/inference.proto",
    descriptorDigest: "rust/crates/inference/inference_descriptor.bin",
    conformanceDigest: "conformance/vectors/inference.json",
  },
};

export const generatedDescriptors = [
  ["proto/filesystem", compatibilityArtifacts.filesystem.descriptorDigest],
  ["proto/objects", compatibilityArtifacts.objects.descriptorDigest],
  ["proto/machines", compatibilityArtifacts.machines.descriptorDigest],
  ["proto/inference", compatibilityArtifacts.inference.descriptorDigest],
  ["rust/crates/stream/proto/stream", compatibilityArtifacts.stream.descriptorDigest],
];

export const packagedRustBindings = [
  ["acyclic/filesystem/v2/acyclic.filesystem.v2.rs", "rust/crates/filesystem/src/generated/acyclic/filesystem/v2/acyclic.filesystem.v2.rs"],
  ["acyclic/filesystem/v2/acyclic.filesystem.v2.tonic.rs", "rust/crates/filesystem/src/generated/acyclic/filesystem/v2/acyclic.filesystem.v2.tonic.rs"],
  ["acyclic/objects/v1/acyclic.objects.v1.rs", "rust/crates/objects/src/generated/acyclic.objects.v1.rs"],
  ["acyclic/objects/v1/acyclic.objects.v1.tonic.rs", "rust/crates/objects/src/generated/acyclic.objects.v1.tonic.rs"],
  ["acyclic/machines/v1/acyclic.machines.v1.rs", "rust/crates/machines/src/generated/acyclic.machines.v1.rs"],
  ["acyclic/machines/v1/acyclic.machines.v1.tonic.rs", "rust/crates/machines/src/generated/acyclic.machines.v1.tonic.rs"],
  ["acyclic/harness/v2/acyclic.harness.v2.rs", "rust/crates/filesystem/src/generated/acyclic/harness/v2/acyclic.harness.v2.rs"],
];

export const nativeWasmVector = "conformance/vectors/harness/native-wasm-event-v2.json";
export const packagedSourceCopies = [
  ...[
    "conversation-message", "conversation-kinds", "file-ref", "file-ref-security-cases", "private-directory-page", "task-outcome",
    "extension-record", "extension-state-migration", "extension-dependency", "extension-configuration", "extension-admission", "resource-revision", "fork-request", "fork-seed", "reference-grant", "execution-placement", "task-admission", "workflow-admission",
    "interaction-resolution", "resolution-receipt", "project-merge-receipt",
  ].map(name => [`fixtures/harness/v2/${name}.json`, `rust/crates/harness/fixtures/v2/${name}.json`]),
  [compatibilityArtifacts.harness.schemaDigest, "rust/crates/harness/proto/harness/v2/harness.proto"],
  [compatibilityArtifacts.harness.conformanceDigest, "rust/crates/conformance/vectors/harness.json"],
  [nativeWasmVector, "rust/crates/harness/conformance/native-wasm-event-v2.json"],
  ["conformance/vectors/stream.json", "rust/crates/stream/conformance/stream.json"],
  ["conformance/vectors/stream.json", "rust/crates/conformance/vectors/stream.json"],
  ["conformance/vectors/objects.json", "rust/crates/conformance/vectors/objects.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  [compatibilityArtifacts.filesystem.conformanceDigest, "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
];

export const normalizeGeneratedRust = (relative, source) => {
  let normalized = `${source.trimEnd()}\n`;
  if (relative === "acyclic/objects/v1/acyclic.objects.v1.rs" ||
      relative === "acyclic/machines/v1/acyclic.machines.v1.rs") {
    const service = relative.includes("objects") ? "acyclic.objects.v1" : "acyclic.machines.v1";
    normalized = normalized.replace(
      new RegExp(`(?:#\\[cfg\\(feature = "grpc"\\)\\]\\r?\\n)?include!\\("${service.replaceAll(".", "\\.")}\\.tonic\\.rs"\\);`),
      `#[cfg(feature = "grpc")]\ninclude!("${service}.tonic.rs");`,
    );
  }
  return normalized;
};

export const normalizeGeneratedTypeScript = source => `${source.trimEnd()}\n`;
