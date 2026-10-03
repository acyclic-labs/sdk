import { spawnSync } from "node:child_process";
import {
  existsSync,
  cpSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function filesUnder(directory) {
  if (!existsSync(directory)) return [];
  const files = [];
  const visit = current => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile()) files.push(path);
    }
  };
  visit(directory);
  return files;
}

function authorityManifest(rootDirectory) {
  const path = join(rootDirectory, "rust-authority.json");
  if (!existsSync(path)) throw new Error(`Rust authority manifest is missing: ${path}`);
  const manifest = JSON.parse(readFileSync(path, "utf8"));
  if (manifest.schema !== "acyclic.sdk.rust-authority.v1" || manifest.authority !== "rust") {
    throw new Error("Rust authority manifest has an invalid identity");
  }
  if (!Array.isArray(manifest.families) || manifest.families.length === 0) {
    throw new Error("Rust authority manifest has no family registry entries");
  }
  for (const family of manifest.families) {
    for (const field of ["source", "descriptor", "source_sha256", "descriptor_sha256"]) {
      if (typeof family[field] !== "string" || family[field].length === 0) {
        throw new Error(`Rust authority family is missing ${field}`);
      }
    }
  }
  return manifest;
}

/**
 * Export the contract model through the Rust entrypoint. The optional
 * environment override is used by qualification fixtures; normal generation
 * always builds the export from the current Rust source tree.
 */
export function rustAuthorityExport() {
  const configured = process.env.ACYCLIC_RUST_AUTHORITY_DIR;
  const output = configured
    ? resolve(configured)
    : mkdtempSync(join(tmpdir(), "acyclic-rust-authority-"));
  if (!configured) {
    const result = spawnSync(
      "cargo",
      [
        "run",
        "--quiet",
        "--locked",
        "--offline",
        "--manifest-path",
        join(root, "rust/crates/sdk-contract-wire/Cargo.toml"),
        "--",
        "generate",
        "--out",
        output,
      ],
      { cwd: root, encoding: "utf8", env: { ...process.env, CARGO_NET_OFFLINE: "true" } },
    );
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`Rust authority export failed:\n${result.stdout ?? ""}\n${result.stderr ?? ""}`);
    }
  }
  const manifest = authorityManifest(output);
  // Objects v1 is a retired compatibility fixture still covered by Buf's
  // package-output checks. It is copied into a throwaway Buf input overlay;
  // it never participates in the Rust authority manifest.
  const input = configured
    ? mkdtempSync(join(tmpdir(), "acyclic-rust-authority-input-"))
    : output;
  if (configured) cpSync(output, input, { recursive: true });
  const historicalObjects = join(root, "proto/objects/v1");
  if (existsSync(historicalObjects)) {
    cpSync(historicalObjects, join(input, "objects/v1"), { recursive: true });
  }
  // Buf needs a local module root for the generated source tree. The Rust
  // exporter remains the source authority; this file is only module plumbing.
  const bufConfig = join(input, "buf.yaml");
  if (!existsSync(bufConfig)) {
    writeFileSync(bufConfig, "version: v2\nmodules:\n  - path: .\n");
  }
  return { root: output, inputRoot: input, manifest };
}

export function rustAuthorityBufTemplate(authority) {
  void authority;
  // Keep the existing Buf plugin compatibility contract. Only the input
  // module changes: its active files come from Rust and its retired Objects v1
  // file is the explicit historical fixture copied above.
  return join(root, "buf.gen.yaml");
}

function canonicalTypeScriptBindings() {
  const canonical = join(root, "generated/typescript");
  const stems = filesUnder(canonical)
    .filter(path => path.endsWith(".js"))
    .map(path => relative(canonical, path).replaceAll("\\", "/").slice(0, -3))
    .sort();
  return stems.map(stem => {
    const packages = readdirSync(join(root, "typescript/packages"), { withFileTypes: true })
      .filter(entry => entry.isDirectory())
      .map(entry => entry.name)
      .filter(name => existsSync(join(root, "typescript/packages", name, "generated/proto", `${stem}.js`)));
    return [stem, packages];
  });
}

// Canonical generation outputs are discovered from the generated tree and
// package copies. This keeps package inventories in sync with Rust authority
// additions without another authored family list.
export const packagedTypeScriptBindings = canonicalTypeScriptBindings();

// These paths are retained only for release metadata and historical package
// compatibility. Contract generation never reads them; active inputs come
// from rustAuthorityExport above.
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
    schemaDigest: "proto/objects/v2/objects.proto",
    descriptorDigest: "rust/crates/objects/src/generated/acyclic-objects-v2.bin",
    conformanceDigest: "conformance/vectors/objects-v2.json",
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
    descriptorDigest: "rust/crates/actors/src/generated/acyclic-actors-v1.bin",
  },
  workers: {
    schemaDigest: "proto/workers/v1/workers.proto",
    descriptorDigest: "rust/crates/workers/src/generated/acyclic-workers-v1.bin",
  },
};

// Released contracts remain immutable historical evidence after their clients
// retire. They are verified separately from the active family metadata.
export const historicalCompatibilityArtifacts = {
  "compatibility/objects/v1/manifest.json": {
    schemaDigest: "proto/objects/v1/objects.proto",
    descriptorDigest: "compatibility/objects/v1/objects_descriptor.bin",
    conformanceDigest: "conformance/vectors/objects.json",
  },
};

// Buf descriptor destinations are compatibility/package artifacts. Their
// active source is resolved through the Rust authority manifest by
// check-generated.mjs and generate.mjs; the legacy Objects v1 path remains an
// explicit historical fixture.
export const generatedDescriptors = [
  ["proto/filesystem", compatibilityArtifacts.filesystem.descriptorDigest],
  ["proto/objects/v1", "compatibility/objects/v1/objects_descriptor.bin"],
  ["proto/objects/v2", "rust/crates/objects/src/generated/acyclic-objects-v2.bin"],
  ["proto/machines", compatibilityArtifacts.machines.descriptorDigest],
  ["proto/inference", compatibilityArtifacts.inference.descriptorDigest],
  ["proto/inference", "rust/crates/inference-contract/inference_descriptor.bin"],
  ["proto/inference", "rust/crates/inference-wasm/inference_reflection_descriptor.bin"],
  ["proto/actors", compatibilityArtifacts.actors.descriptorDigest],
  ["proto/workers", compatibilityArtifacts.workers.descriptorDigest],
  ["rust/crates/stream/proto/stream", compatibilityArtifacts.stream.descriptorDigest],
];

export const packagedRustBindings = [
  ["acyclic/objects/v2/acyclic.objects.v2.rs", "rust/crates/objects/src/generated/acyclic.objects.v2.rs"],
  ["acyclic/objects/v2/acyclic.objects.v2.tonic.rs", "rust/crates/objects/src/generated/acyclic.objects.v2.tonic.rs"],
  ["acyclic/machines/v1/acyclic.machines.v1.rs", "rust/crates/machines/src/generated/acyclic.machines.v1.rs"],
  ["acyclic/machines/v1/acyclic.machines.v1.tonic.rs", "rust/crates/machines/src/generated/acyclic.machines.v1.tonic.rs"],
  ["acyclic/actors/v1/acyclic.actors.v1.rs", "rust/crates/actors/src/generated/acyclic.actors.v1.rs"],
  ["acyclic/actors/v1/acyclic.actors.v1.tonic.rs", "rust/crates/actors/src/generated/acyclic.actors.v1.tonic.rs"],
  ["acyclic/workers/v1/acyclic.workers.v1.rs", "rust/crates/workers/src/generated/acyclic.workers.v1.rs"],
  ["acyclic/workers/v1/acyclic.workers.v1.tonic.rs", "rust/crates/workers/src/generated/acyclic.workers.v1.tonic.rs"],
];

export const nativeWasmVector = "conformance/vectors/harness/native-wasm-event-v2.json";
export const packagedSourceCopies = [
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
  ["conformance/vectors/objects.json", "rust/crates/conformance/vectors/objects.json"],
  ["conformance/vectors/objects-v2.json", "rust/crates/objects/conformance/objects-v2.json"],
  ["conformance/vectors/objects-v2.json", "rust/crates/conformance/vectors/objects-v2.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  [compatibilityArtifacts.filesystem.conformanceDigest, "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
];

// The local Rust protoc plugin emits feature guards for service includes.
// JavaScript only normalizes terminal whitespace and does not own policy.
export const normalizeGeneratedRust = (_relative, source) => `${source.trimEnd()}\n`;

export const normalizeGeneratedTypeScript = source => `${source.trimEnd()}\n`;
