import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const sourceRoot = fileURLToPath(new URL("../", import.meta.url));
let tracked;

// Pinned outputs are compared. Ignored build outputs are regenerated from the
// current source, so a clean checkout needs no pre-existing generated cache.
export function projectGenerated(path, content, check) {
  if (check) {
    tracked ??= new Set(execFileSync("git", ["ls-files", "-z"], { cwd: sourceRoot, encoding: "utf8" }).split("\0"));
    if (tracked.has(relative(sourceRoot, path).replaceAll("\\", "/"))) return checkGenerated(path, content);
  }
  mkdirSync(dirname(path), { recursive: true });
  writeChanged(path, content);
}

export function checkGenerated(path, content) {
  if (!existsSync(path) || !readFileSync(path).equals(Buffer.from(content))) {
    throw new Error(`STEP_FAILED generated-artifact: ${path} is stale; recovery: bun run generate`);
  }
}

// Directory-producing Rust exporters use the same output in write and check
// modes. Comparing their fresh bytes never rewrites a contributor's checkout.
export function checkGeneratedDirectory(generated, destination) {
  for (const entry of readdirSync(generated, { recursive: true, withFileTypes: true })) {
    if (entry.isDirectory()) continue;
    const source = join(entry.parentPath, entry.name);
    const relative = source.slice(generated.length + 1);
    projectGenerated(join(destination, relative), readFileSync(source), true);
  }
}

export const writeChanged = (path, content) => {
  const bytes = typeof content === "string" ? Buffer.from(content) : content;
  if (!existsSync(path) || !readFileSync(path).equals(bytes)) writeFileSync(path, bytes);
};

// Canonical generation inputs and their published copies live here.
// Every family that negotiates a protocol ships the shared handshake schema.
/** @type {[string, string[], string?][]} */
export const packagedTypeScriptBindings = [
  ["filesystem/v2/filesystem_pb", ["filesystem"]],
  ["protocol/v1/protocol_pb", ["filesystem"]],
  // inference_pb imports the proto2 custom options descriptor transitively.
  ["validation/v1/options_pb", ["inference", "actors", "filesystem", "machines", "objects", "stream"], "compatibility"],
  ["auth/v1/auth_pb", ["auth"]],
  ["inference/v1/inference_pb", ["inference"]],
  ["machines/v1/machines_pb", ["machines"]],
  ["harness/v2/resources_pb", ["machines", "actors"], "compatibility"],
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
    conformanceDigest: "rust/crates/auth/conformance/secrets-v1.json",
  },
  harness: {
    schemaDigest: "rust/crates/harness-core/proto/harness/v2/harness.proto",
    conformanceDigest: "rust/crates/harness/conformance/legacy-harness.json",
  },
  filesystem: {
    schemaDigest: "rust/crates/filesystem-wire/proto/filesystem/v2/filesystem.proto",
    descriptorDigest: "compatibility/descriptors/filesystem-v2.bin",
    conformanceDigest: "rust/crates/filesystem/conformance/dependency-content-range-v1.json",
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
    conformanceDigest: "rust/crates/machines/conformance/machines.json",
  },
  inference: {
    schemaDigest: "rust/crates/inference/proto/inference/v1/inference.proto",
    descriptorDigest: "compatibility/descriptors/inference-v1.bin",
    conformanceDigest: "rust/crates/inference/conformance/inference.json",
  },
  actors: {
    schemaDigest: "rust/crates/actors/proto/actors/v1/actors.proto",
    descriptorDigest: "compatibility/descriptors/actors-v1.bin",
  },
};

export const nativeWasmVector = "rust/crates/harness/conformance/native-wasm-event-v2.json";
export const packagedSourceCopies = [
  [compatibilityArtifacts.auth.conformanceDigest, "typescript/packages/auth/generated/conformance/secrets-v1.json"],
  ...[
    "conversation-message", "conversation-kinds", "file-ref", "file-ref-security-cases", "private-directory-page", "task-outcome",
    "extension-record", "extension-state-migration", "extension-dependency", "extension-configuration", "extension-admission", "resource-revision", "fork-request", "fork-seed", "reference-grant",
    "interaction-resolution", "project-merge-receipt",
  ].map(name => [`fixtures/harness/v2/${name}.json`, `rust/crates/harness/fixtures/v2/${name}.json`]),
  ["rust/crates/harness-core/proto/protocol/v1/protocol.proto", "rust/crates/filesystem-wire/proto/protocol/v1/protocol.proto"],
];
