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
  ["harness/v2/harness_pb", ["harness"]],
  ["protocol/v1/protocol_pb", ["filesystem", "harness"]],
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

export const compatibilityArtifacts = {
  auth: {
    schemaDigest: "proto/auth/v1/auth.proto",
    descriptorDigest: "rust/crates/auth/src/generated/acyclic-auth-v1.bin",
  },
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
    schemaDigest: "rust/crates/stream/proto/stream/v1/stream.proto",
    descriptorDigest: "rust/crates/stream/proto/stream/v1/stream_descriptor.bin",
    conformanceDigest: "conformance/vectors/stream.json",
  },
  objects: {
    schemaDigest: "proto/objects/v1/objects.proto",
    descriptorDigest: "rust/crates/objects/src/generated/acyclic-objects-v1.bin",
    conformanceDigest: "conformance/vectors/objects-v1.json",
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
  actors: {
    schemaDigest: "proto/actors/v1/actors.proto",
    descriptorDigest: "compatibility/descriptors/actors-v1.bin",
  },
};

export const generatedDescriptors = [
  ["proto/auth", compatibilityArtifacts.auth.descriptorDigest],
  ["proto/filesystem", compatibilityArtifacts.filesystem.descriptorDigest],
  ["proto/objects/v1", "rust/crates/objects/src/generated/acyclic-objects-v1.bin"],
  ["proto/machines", compatibilityArtifacts.machines.descriptorDigest],
  ["proto/inference", compatibilityArtifacts.inference.descriptorDigest],
  ["proto/actors", compatibilityArtifacts.actors.descriptorDigest],
  ["rust/crates/stream/proto/stream", compatibilityArtifacts.stream.descriptorDigest],
];

export const packagedRustBindings = [
  ["acyclic/objects/v1/acyclic.objects.v1.rs", "rust/crates/objects/src/generated/acyclic.objects.v1.rs"],
  ["acyclic/objects/v1/acyclic.objects.v1.tonic.rs", "rust/crates/objects/src/generated/acyclic.objects.v1.tonic.rs"],
  ["acyclic/machines/v1/acyclic.machines.v1.rs", "rust/crates/machines/src/generated/acyclic.machines.v1.rs"],
  ["acyclic/machines/v1/acyclic.machines.v1.tonic.rs", "rust/crates/machines/src/generated/acyclic.machines.v1.tonic.rs"],
];

export const nativeWasmVector = "conformance/vectors/harness/native-wasm-event-v2.json";
export const packagedSourceCopies = [
  ["proto/validation/v1/options.proto", "rust/crates/contract-derive/proto/validation/v1/options.proto"],
  ["proto/validation/v1/options.proto", "rust/crates/stream/proto/validation/v1/options.proto"],
  ...[
    "conversation-message", "conversation-kinds", "file-ref", "file-ref-security-cases", "private-directory-page", "task-outcome",
    "extension-record", "extension-state-migration", "extension-dependency", "extension-configuration", "extension-admission", "resource-revision", "fork-request", "fork-seed", "reference-grant", "execution-placement", "task-admission", "workflow-admission",
    "interaction-resolution", "resolution-receipt", "project-merge-receipt",
  ].map(name => [`fixtures/harness/v2/${name}.json`, `rust/crates/harness/fixtures/v2/${name}.json`]),
  [compatibilityArtifacts.harness.schemaDigest, "rust/crates/harness/proto/harness/v2/harness.proto"],
  ["proto/protocol/v1/protocol.proto", "rust/crates/harness/proto/protocol/v1/protocol.proto"],
  [compatibilityArtifacts.harness.conformanceDigest, "rust/crates/conformance/vectors/harness.json"],
  [nativeWasmVector, "rust/crates/harness/conformance/native-wasm-event-v2.json"],
  ["conformance/vectors/stream.json", "rust/crates/stream/conformance/stream.json"],
  ["conformance/vectors/stream.json", "rust/crates/conformance/vectors/stream.json"],
  ["conformance/vectors/objects-v1.json", "rust/crates/objects/conformance/objects-v1.json"],
  ["conformance/vectors/objects-v1.json", "rust/crates/conformance/vectors/objects-v1.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  [compatibilityArtifacts.filesystem.conformanceDigest, "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
];

// Objects and Machines compile their tonic glue only under the `grpc` feature.
/** @type {Record<string, string | undefined>} */
const grpcGatedRust = {
  "acyclic/objects/v1/acyclic.objects.v1.rs": 'include!("acyclic.objects.v1.tonic.rs");',
  "acyclic/machines/v1/acyclic.machines.v1.rs": 'include!("acyclic.machines.v1.tonic.rs");',
};
export const normalizeGeneratedRust = (relative, source) => {
  const normalized = `${source.trimEnd()}\n`;
  const include = grpcGatedRust[relative];
  const gate = relative.startsWith("acyclic/objects/")
    ? 'all(feature = "grpc", not(target_arch = "wasm32"))' : 'feature = "grpc"';
  return include ? normalized.replace(include, `#[cfg(${gate})]\n${include}`) : normalized;
};

export const normalizeGeneratedTypeScript = source => {
  let normalized = `${source.trimEnd()}\n`;
  // Preserve documentation while keeping large generated projections within
  // the source-file bound. Repeated normalization is byte-idempotent.
  if ((normalized.includes("fileDesc(") || normalized.includes("from file auth/v1/auth.proto") || normalized.includes("from file objects/v1/objects.proto")) && normalized.split("\n").length > 800) {
    normalized = normalized.replaceAll("\n\n", "\n");
    if (normalized.split("\n").length > 800) {
      normalized = normalized.replace(/\/\*\*([\s\S]*?)\*\//g, (_, body) =>
        `/** ${body.split("\n").map(line => line.trim().replace(/^\* ?/, "")).filter(Boolean).join(" ")} */`);
    }
  }
  return normalized;
};

// Keep the canonical tonic modules separate so each packaged source stays bounded.
/** @returns {[string, string][]} */
export const packagedRustProjection = (relative, source) => {
  const binding = packagedRustBindings.find(([name]) => name === relative);
  if (!binding) throw new Error(`unknown packaged Rust binding: ${relative}`);
  const destination = binding[1];
  const normalized = normalizeGeneratedRust(relative, source);
  if (relative !== "acyclic/objects/v1/acyclic.objects.v1.tonic.rs") return [[destination, normalized]];
  const parts = normalized.split(/(?=^\/\/\/ Generated (?:client|server) implementations\.$)/m);
  if (parts.length !== 7) throw new Error("expected three Objects tonic client/server pairs");
  /** @type {[string, string][]} */
  const files = parts.slice(1).map(part => {
    const module = /^\/\/\/ Generated (?:client|server) implementations\.\npub mod (\w+)_service_(client|server) \{/u.exec(part);
    if (!module) throw new Error("unexpected tonic module layout");
    const [, service, side] = module;
    return [`rust/crates/objects/src/generated/services/${service}/${side}.rs`, part];
  });
  return [[destination, parts[0] + files.map(([path]) => `include!("${path.split("/generated/")[1]}");\n`).join("")], ...files];
};
