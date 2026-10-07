import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { assertBuildInputs, assertExactInventory, assertMatchingBuildInputs, assertSourceSnapshot, sourceSnapshot } from "./build-stream-native.mjs";

const root = fileURLToPath(new URL("..", import.meta.url));

test("native qualification rejects a compiled path dependency mutation", async () => {
  const snapshot = await sourceSnapshot();
  const dependency = resolve(root, "rust/crates/native-runtime/Cargo.toml");
  const original = await readFile(dependency);
  try {
    await writeFile(dependency, Buffer.concat([original, Buffer.from("\n# qualification mutation\n")]));
    await assert.rejects(assertSourceSnapshot(snapshot), /source closure changed/);
  } finally {
    await writeFile(dependency, original);
  }
  await assertSourceSnapshot(snapshot);
});

test("native qualification rejects a Windows junction in the source closure", { skip: process.platform !== "win32" }, async () => {
  const target = await mkdtemp(resolve(tmpdir(), "stream-native-junction-target-"));
  const junction = resolve(root, "rust/crates/native-runtime/.qualification-junction");
  try {
    await symlink(target, junction, "junction");
    await assert.rejects(sourceSnapshot(), /symlink or reparse point/);
  } finally {
    await rm(junction, { recursive: true, force: true });
    await rm(target, { recursive: true, force: true });
  }
});

test("native staging rejects stale package files", async () => {
  const output = await mkdtemp(resolve(tmpdir(), "stream-native-stale-output-"));
  try {
    await writeFile(resolve(output, "binding.cjs"), "ok\n");
    await writeFile(resolve(output, "stale.node"), "stale\n");
    await assert.rejects(assertExactInventory(output, new Set(["binding.cjs"])), /unstated files: stale\.node/);
  } finally {
    await rm(output, { recursive: true, force: true });
  }
});

function validBuildInputs() {
  return {
    schema: "acyclic.stream.native-build-inputs.v1",
    target: "x86_64-pc-windows-msvc",
    target_dir: "C:/runner/_work/target-stream-native",
    runtime: { node: "v24.0.0", node_path: "C:/Program Files/nodejs/node.exe", platform: "win32", arch: "x64", bun: { maintained: "1.4.2", actual: { command: "bun", args: ["--version"], output: "1.4.2" } } },
    invocation: { script: "scripts/build-stream-native.mjs", runtime: "C:/Program Files/nodejs/node.exe", args: ["build", "--target", "x86_64-pc-windows-msvc"] },
    compiler: {
      rustc: { command: "rustc", args: ["--version", "--verbose"], output: "rustc 1.98.1\nhost: x86_64-pc-windows-msvc" },
      cargo: { command: "cargo", args: ["--version", "--verbose"], output: "cargo 1.98.1\nhost: x86_64-pc-windows-msvc" },
    },
    generator: {
      package: "@napi-rs/cli",
      version: "3.10.5",
      package_sha256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      entry_sha256: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      lock_sha256: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      options: {
        release: true,
        platform: true,
        target: "x86_64-pc-windows-msvc",
        output_dir: "C:/runner/_work/native-bundle",
        target_dir: "C:/runner/_work/target-stream-native",
        js_package_name: "@acyclic-labs/stream",
        js_binding: "binding.cjs",
        dts: "binding.d.ts",
      },
    },
    linker: {
      configured: { target: null, rustc: null },
      environment: { LINK: null, CC: null, AR: null, VCINSTALLDIR: null, VCToolsInstallDir: null, WindowsSdkDir: null, VisualStudioVersion: null },
      command: "link.exe",
      path: { command: "where.exe", args: ["link.exe"], output: "C:/Program Files/MSVC/link.exe" },
      version: { command: "link.exe", args: ["/?"], output: "Microsoft (R) Incremental Linker Version 14.42" },
    },
    profile: {
      name: "release",
      cargo_incremental: "0",
      release_incremental: "false",
      manifest_sha256: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      config_sha256: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    },
    environment: {
      RUSTFLAGS: "-C target-feature=+crt-static",
      CARGO_ENCODED_RUSTFLAGS: null,
      RUSTC_WRAPPER: null,
      CARGO_TARGET_DIR: null,
    },
    cache: {
      wrapper: null,
      wrapper_command: null,
      wrapper_version: null,
      directory: null,
      size: null,
    },
  };
}

test("native qualification rejects build input identity mutations", () => {
  const valid = validBuildInputs();
  assertBuildInputs(valid);

  for (const [label, mutate, expected] of [
    ["compiler", value => { value.compiler.rustc.output = "rustc 1.99.0"; }, /build input attestation differs/],
    ["generator", value => { value.generator.version = "3.10.4"; }, /build input attestation differs/],
    ["generator options", value => { value.generator.options.output_dir = "C:/runner/_work/other-bundle"; }, /build input attestation differs/],
    ["bun", value => { value.runtime.bun.maintained = "1.4.1"; }, /build input attestation differs/],
    ["invocation", value => { value.invocation.script = "scripts/other-build.mjs"; }, /build input attestation differs/],
    ["linker", value => { value.linker.version.output = "Microsoft (R) Incremental Linker Version 14.43"; }, /build input attestation differs/],
    ["environment", value => { value.environment.RUSTFLAGS = "-C opt-level=3"; }, /build input attestation differs/],
    ["profile", value => { value.profile.name = "dev"; }, /native build profile is not release/],
    ["cache", value => { value.cache.wrapper = 42; }, /native build cache input wrapper is invalid/],
  ]) {
    const mutated = structuredClone(valid);
    mutate(mutated);
    if (["compiler", "generator", "generator options", "bun", "invocation", "linker", "environment"].includes(label)) assert.throws(() => assertMatchingBuildInputs(valid, mutated), expected);
    else assert.throws(() => assertBuildInputs(mutated), expected);
  }
});
