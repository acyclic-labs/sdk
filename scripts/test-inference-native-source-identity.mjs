import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";

import { runtimeSourceIdentityFromMetadata } from "./inference-native-source-identity.mjs";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "inference-native-identity-"));
  const native = join(root, "rust", "crates", "sdk-inference-native");
  const inference = join(root, "rust", "crates", "inference");
  mkdirSync(join(native, "src"), { recursive: true });
  mkdirSync(join(inference, "src"), { recursive: true });
  mkdirSync(join(root, ".cargo"), { recursive: true });
  writeFileSync(join(root, "Cargo.toml"), "[workspace]\nmembers=[]\n");
  writeFileSync(join(root, "Cargo.lock"), "workspace-lock-v1\n");
  writeFileSync(join(root, "rust-toolchain.toml"), "[toolchain]\nchannel=\"fixture\"\n");
  writeFileSync(join(root, ".cargo", "config.toml"), "[build]\ntarget-dir=\"target\"\n");
  writeFileSync(join(native, "Cargo.toml"), "[package]\nname=\"native\"\n");
  writeFileSync(join(native, "src", "lib.rs"), "native-v1\n");
  writeFileSync(join(inference, "Cargo.toml"), "[package]\nname=\"inference\"\n");
  writeFileSync(join(inference, "src", "client.rs"), "client-v1\n");
  writeFileSync(join(inference, "inference_model_descriptor_docs.bin"), "descriptor-docs-v1\n");
  writeFileSync(join(inference, "inference_model_descriptor.bin"), "descriptor-model-v1\n");
  const nativeId = "path+file:///fixture/native#native@1.0.0";
  const inferenceId = "path+file:///fixture/inference#inference@1.0.0";
  const metadata = {
    packages: [
      {
        id: nativeId, name: "native", version: "1.0.0",
        source: null, manifest_path: join(native, "Cargo.toml"),
        dependencies: [{ name: "inference", source: null, req: "*", kind: "normal", rename: null, optional: false, uses_default_features: true, features: [], target: null, registry: null, path: join(inference, "..", "inference") }],
        targets: [{ name: "native", kind: ["lib"], crate_types: ["cdylib"], required_features: [] }],
      },
      {
        id: inferenceId, name: "inference", version: "1.0.0",
        source: null, manifest_path: join(inference, "Cargo.toml"), dependencies: [],
        targets: [{ name: "inference", kind: ["lib"], crate_types: ["lib"], required_features: [] }],
      },
    ],
    resolve: {
      root: nativeId,
      nodes: [
        { id: nativeId, dependencies: [inferenceId], features: ["default"] },
        { id: inferenceId, dependencies: [], features: ["host"] },
      ],
    },
  };
  const environment = { ...process.env };
  delete environment.ACYCLIC_INFERENCE_MODEL_DESCRIPTOR;
  return { root, native, inference, metadata, environment };
}

test("runtime source identity covers runtime sources, workspace inputs, descriptor, and toolchain", () => {
  const value = fixture();
  try {
    const options = { environment: value.environment };
    const first = runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options);
    const paths = first.files.map((entry) => entry.path);
    assert.ok(paths.includes("Cargo.lock"));
    assert.ok(paths.includes("Cargo.toml"));
    assert.ok(paths.includes("rust-toolchain.toml"));
    assert.ok(paths.includes(".cargo/config.toml"));
    assert.ok(paths.includes("rust/crates/sdk-inference-native/src/lib.rs"));
    assert.ok(paths.includes("rust/crates/inference/src/client.rs"));
    assert.equal(first.descriptor.path, "rust/crates/inference/inference_model_descriptor_docs.bin");
    assert.equal(first.descriptor.environment, null);
    assert.match(first.toolchain.rustc_verbose, /release|commit-hash|host/);
    assert.ok(first.toolchain.executables.some((entry) => entry.label === "rustc" && /^[0-9a-f]{64}$/.test(entry.sha256)));
    assert.ok(first.toolchain.executables.some((entry) => entry.label === "cargo" && /^[0-9a-f]{64}$/.test(entry.sha256)));
    assert.equal(first.toolchain.effective_linker.command, "cc");

    writeFileSync(join(value.native, "src", "lib.rs"), "native-v2\n");
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options).closureSha256, first.closureSha256);
    writeFileSync(join(value.native, "src", "lib.rs"), "native-v1\n");
    writeFileSync(join(value.inference, "src", "client.rs"), "client-v2\n");
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options).closureSha256, first.closureSha256);
    writeFileSync(join(value.inference, "src", "client.rs"), "client-v1\n");
    writeFileSync(join(value.root, "Cargo.lock"), "workspace-lock-v2\n");
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options).closureSha256, first.closureSha256);
    writeFileSync(join(value.root, "Cargo.toml"), "[workspace]\nmembers=[\"rust/crates/inference\"]\n");
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options).closureSha256, first.closureSha256);
    writeFileSync(join(value.root, "rust-toolchain.toml"), "[toolchain]\nchannel=\"fixture-v2\"\n");
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options).closureSha256, first.closureSha256);
    writeFileSync(join(value.root, ".cargo", "config.toml"), "[build]\ntarget-dir=\"other-target\"\n");
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", options).closureSha256, first.closureSha256);
    const flagged = { ...value.environment, RUSTFLAGS: "-C debuginfo=1" };
    assert.notEqual(runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", { environment: flagged }).closureSha256, first.closureSha256);
    const wrapped = { ...value.environment, RUSTC_WRAPPER: "rustc" };
    const wrappedIdentity = runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", { environment: wrapped });
    assert.ok(wrappedIdentity.toolchain.executables.some((entry) => entry.label === "rustc-wrapper"));
    assert.notEqual(wrappedIdentity.closureSha256, first.closureSha256);
    const linked = { ...value.environment, CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER: "rustc" };
    const linkedIdentity = runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", { environment: linked });
    assert.ok(linkedIdentity.toolchain.executables.some((entry) => entry.label === "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER"));
    assert.notEqual(linkedIdentity.closureSha256, first.closureSha256);
    const selected = { ...value.environment, ACYCLIC_INFERENCE_MODEL_DESCRIPTOR: "inference_model_descriptor.bin" };
    const selectedIdentity = runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", { environment: selected });
    assert.equal(selectedIdentity.descriptor.path, "rust/crates/inference/inference_model_descriptor.bin");
    assert.notEqual(selectedIdentity.closureSha256, first.closureSha256);
    const external = join(value.root, "..", "external-descriptor.bin");
    writeFileSync(external, "external\n");
    assert.throws(() => runtimeSourceIdentityFromMetadata(value.root, value.metadata, "x86_64-unknown-linux-gnu", {
      environment: { ...value.environment, ACYCLIC_INFERENCE_MODEL_DESCRIPTOR: external },
    }), /escapes checkout/);
    rmSync(external, { force: true });
  } finally {
    rmSync(value.root, { recursive: true, force: true });
  }
});
