import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
/** @typedef {ReturnType<typeof validBuildInputs>} BuildInputs */

import { assertBuildInputs, assertExactInventory, assertMatchingBuildInputs, assertOwnedDirectory, assertSourceSnapshot, buildInputsReceipt, configureDarwinRustLld, createRustcInvocationCapture, darwinRustLldPaths, deterministicRustflags, ensureCargoTargetDirectory, linkerInputs, normalizeBuildInputs, publishBundle, sourceSnapshot, withDeterministicRustflags } from "./build-stream-native.mjs";

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

test("native staging rejects symlinked or non-regular artifacts", async () => {
  const output = await mkdtemp(resolve(tmpdir(), "stream-native-symlink-output-"));
  const target = await mkdtemp(resolve(tmpdir(), "stream-native-symlink-target-"));
  const link = resolve(output, "binding.cjs");
  try {
    await writeFile(resolve(target, "binding.cjs"), "external\n");
    await symlink(resolve(target, "binding.cjs"), link, "file");
    await assert.rejects(assertExactInventory(output, new Set(["binding.cjs"])), /symlink or reparse point/);
  } finally {
    await rm(link, { recursive: true, force: true });
    await rm(output, { recursive: true, force: true });
    await rm(target, { recursive: true, force: true });
  }
});

test("native staging refuses an external output root before touching the prior bundle", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-staging-root-"));
  const external = await mkdtemp(resolve(tmpdir(), "stream-native-staging-external-"));
  const output = resolve(parent, "generated-native");
  try {
    await writeFile(resolve(external, "previous.node"), "previous\n");
    await symlink(external, output, process.platform === "win32" ? "junction" : "dir");
    await assert.rejects(assertOwnedDirectory(output, { allowMissing: true }), /owned directory/);
    assert.equal((await readFile(resolve(external, "previous.node"))).toString(), "previous\n");
  } finally {
    await rm(output, { recursive: true, force: true });
    await rm(parent, { recursive: true, force: true });
    await rm(external, { recursive: true, force: true });
  }
});

test("native publication refuses an unowned output without mutating either directory", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-publication-"));
  const candidate = resolve(parent, "candidate");
  const output = resolve(parent, "output");
  try {
    await mkdir(candidate);
    await writeFile(resolve(candidate, "candidate.node"), "candidate\n");
    await mkdir(output);
    await writeFile(resolve(output, "unowned.txt"), "keep\n");
    await assert.rejects(publishBundle(candidate, output), /refusing to replace unowned native bundle/);
    assert.equal((await readFile(resolve(output, "unowned.txt"))).toString(), "keep\n");
    assert.equal((await readFile(resolve(candidate, "candidate.node"))).toString(), "candidate\n");
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});

test("native publication keeps the committed bundle when backup cleanup fails", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-publication-cleanup-"));
  const candidate = resolve(parent, "candidate");
  const output = resolve(parent, "output");
  const warnings = [];
  const originalWarn = console.warn;
  try {
    await mkdir(candidate);
    await writeFile(resolve(candidate, "candidate.node"), "candidate\n");
    await mkdir(output);
    await writeFile(resolve(output, "previous.node"), "previous\n");
    console.warn = message => warnings.push(message);
    await publishBundle(candidate, output, {
      validateExisting: async () => true,
      cleanup: async () => { throw new Error("injected cleanup failure"); },
    });
    assert.equal((await readFile(resolve(output, "candidate.node"))).toString(), "candidate\n");
    assert.equal((await readFile(resolve(output, "previous.node")).catch(() => Buffer.from(""))).toString(), "");
    assert.equal(warnings.length, 1);
    const backups = (await readdir(parent)).filter(name => name.startsWith(".output.backup-"));
    assert.equal(backups.length, 1);
  } finally {
    console.warn = originalWarn;
    await rm(parent, { recursive: true, force: true });
  }
});

function validBuildInputs() {
  return {
    schema: "acyclic.stream.native-build-inputs.v3",
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
        cargo_options: ["--locked"],
      },
    },
    linker: {
      configured: { target: null },
      environment: { LINK: null, CC: null, AR: null, RUSTC_LINKER: null, DYLD_LIBRARY_PATH: null, SDKROOT: null, VCINSTALLDIR: null, VCToolsInstallDir: null, WindowsSdkDir: null, VisualStudioVersion: null },
      actual: {
        source: "rustc-invocation",
        rustc: "C:/Rust/bin/rustc.exe",
        target: "x86_64-pc-windows-msvc",
        linker: "C:/Program Files/MSVC/link.exe",
        args: ["--crate-name", "acyclic_stream_napi", "--emit", "dep-info,link"],
      },
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
      RUSTC_WORKSPACE_WRAPPER: null,
      CARGO_TARGET_DIR: null,
    },
    cache: {
      wrapper: null,
      wrapper_version: null,
      directory: null,
      size: null,
    },
  };
}

test("native qualification encodes remap and MSVC reproducibility flags without losing prior flags", () => {
  const encoded = deterministicRustflags("C:/agent checkout/sdk", "C:/cargo target", "x86_64-pc-windows-msvc", {
    plain: "-C opt-level=3 -C link-arg=\"C:/tool path/extra.lib\"",
    encoded: "--cfg\x1fprior_flag",
  });
  const flags = encoded.split("\x1f");
  assert.deepEqual(flags.slice(0, 2), ["--cfg", "prior_flag"]);
  assert.ok(!flags.includes("opt-level=3"));
  assert.ok(!flags.includes("link-arg=C:/tool path/extra.lib"));
  assert.ok(flags.includes("prior_flag"));
  assert.ok(flags.includes("-C"));
  assert.equal(flags.at(-3), "target-feature=+crt-static");
  assert.equal(flags.at(-1), "link-arg=/Brepro");
  assert.ok(flags.some(flag => flag.includes("--remap-path-prefix=C:/agent checkout/sdk=/__acyclic_stream_source")));
  assert.ok(flags.some(flag => flag.includes("--remap-path-prefix=C:/cargo target=/__acyclic_stream_target")));

  const windowsPath = deterministicRustflags("C:/src", "C:/target", undefined, {
    plain: "-C link-arg=\"C:\\Program Files\\SDK\\link.exe\"",
  }).split("\x1f");
  assert.deepEqual(windowsPath.slice(0, 2), ["-C", "link-arg=C:\\Program Files\\SDK\\link.exe"]);

  const darwin = deterministicRustflags("/src", "/target", "aarch64-apple-darwin").split("\x1f");
  assert.ok(darwin.includes("-C"));
  assert.ok(darwin.includes("link-arg=-Wl,-install_name,@rpath/libacyclic_stream_napi.dylib"));
  assert.ok(darwin.includes("link-arg=-Wl,-final_output,libacyclic_stream_napi.dylib"));
  const lldPaths = darwinRustLldPaths("aarch64-apple-darwin", "/rust/sysroot");
  assert.deepEqual(lldPaths, {
    linkerEnvironment: "CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER",
    linker: resolve("/rust/sysroot", "lib", "rustlib", "aarch64-apple-darwin", "bin", "gcc-ld", "ld64.lld"),
    driver: resolve("/rust/sysroot", "lib", "rustlib", "aarch64-apple-darwin", "bin", "gcc-ld"),
    loaderPath: resolve("/rust/sysroot", "lib"),
  });
  assert.equal(darwinRustLldPaths("x86_64-pc-windows-msvc", "/rust/sysroot"), null);
});

test("native qualification rejects build input identity mutations", () => {
  const valid = validBuildInputs();
  assertBuildInputs(valid);

  /** @type {Array<[string, (value: BuildInputs) => void, RegExp]>} */
  const mutations = [
    ["compiler", value => { value.compiler.rustc.output = "rustc 1.99.0"; }, /build input attestation differs/],
    ["generator", value => { value.generator.version = "3.10.4"; }, /build input attestation differs/],
    ["generator options", value => { value.generator.options.output_dir = "C:/runner/_work/other-bundle"; }, /build input attestation differs/],
    ["bun", value => { value.runtime.bun.maintained = "1.4.1"; }, /build input attestation differs/],
    ["invocation", value => { value.invocation.script = "scripts/other-build.mjs"; }, /build input attestation differs/],
    ["linker", value => { value.linker.actual.linker = "C:/Program Files/LLVM/lld-link.exe"; }, /build input attestation differs/],
    ["environment", value => { value.environment.RUSTFLAGS = "-C opt-level=3"; }, /build input attestation differs/],
    ["profile", value => { value.profile.name = "dev"; }, /native build profile is not release/],
    ["incremental policy", value => { Object.assign(value.profile, { cargo_incremental: null }); }, /incremental policy is not pinned to zero/],
    ["generator cargo options", value => { value.generator.options.cargo_options = ["--offline"]; }, /generator cargo options are invalid/],
    ["cache", value => { Object.assign(value.cache, { wrapper: 42 }); }, /native build input cache\.wrapper is invalid/],
  ];
  for (const [label, mutate, expected] of mutations) {
    const mutated = structuredClone(valid);
    mutate(mutated);
    if (["compiler", "generator", "generator options", "bun", "invocation", "linker", "environment"].includes(label)) assert.throws(() => assertMatchingBuildInputs(valid, mutated), expected);
    else assert.throws(() => assertBuildInputs(mutated), expected);
  }

  const ambientRustcLinker = linkerInputs("x86_64-pc-windows-msvc", { RUSTC_LINKER: "C:/fake/lld-link.exe" });
  assert.equal(ambientRustcLinker.configured.target, null);
  assert.equal(ambientRustcLinker.environment.RUSTC_LINKER, "C:/fake/lld-link.exe");

  const configuredTargetLinker = linkerInputs("x86_64-pc-windows-msvc", {
    CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER: "C:/configured/link.exe",
    RUSTC_LINKER: "C:/ambient/lld-link.exe",
  });
  assert.equal(configuredTargetLinker.configured.target, "C:/configured/link.exe");

  const darwinLinker = linkerInputs("aarch64-apple-darwin", {
    CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER: "/usr/bin/clang",
    DYLD_LIBRARY_PATH: "/rust/sysroot/lib",
    SDKROOT: "/Applications/Xcode.app/SDKs/MacOSX.sdk",
  });
  assert.equal(darwinLinker.configured.target, "/usr/bin/clang");
  assert.equal(darwinLinker.environment.DYLD_LIBRARY_PATH, "/rust/sysroot/lib");
  assert.equal(darwinLinker.environment.SDKROOT, "/Applications/Xcode.app/SDKs/MacOSX.sdk");
});

test("native qualification normalizes host paths in published build inputs", () => {
  const original = validBuildInputs();
  original.invocation.args.push("--output", "C:/runner/_work/native-bundle", "--target-dir", "C:/runner/_work/target-stream-native");
  original.linker.actual.args.push("--out-dir", "C:/runner/_work/target-stream-native/x86_64-pc-windows-msvc/release/deps");
  original.linker.actual.rustc = "C:/Rust/bin/rustc.exe";
  original.environment.RUSTFLAGS = "-C link-arg=C:/runner/_work/target-stream-native/x86_64-pc-windows-msvc/release/deps";
  const normalized = normalizeBuildInputs(original, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });
  assert.equal(normalized.target_dir, "<target-dir>");
  assert.equal(normalized.runtime.node_path, "<runtime>");
  assert.equal(normalized.invocation.runtime, "bun");
  assert.equal(normalized.generator.options.output_dir, "<output-dir>");
  assert.equal(normalized.generator.options.target_dir, "<target-dir>");
  assert.equal(normalized.linker.actual.rustc, "rustc");
  assert.equal(normalized.invocation.args.at(-1), "<target-dir>");
  assert.match(normalized.linker.actual.args.at(-1), /^<target-dir>\//u);
  assert.equal(normalized.environment.RUSTFLAGS, "-C link-arg=<target-dir>/x86_64-pc-windows-msvc/release/deps");
  assertBuildInputs(normalized);

  const relocated = validBuildInputs();
  relocated.invocation.args.push("--output", "D:/agent/work/native-bundle", "--target-dir", "D:/agent/work/target-stream-native");
  relocated.linker.actual.args.push("--out-dir", "D:/agent/work/target-stream-native/x86_64-pc-windows-msvc/release/deps");
  relocated.linker.actual.rustc = "D:/Rust/bin/rustc.exe";
  relocated.linker.actual.linker = "D:/Program Files/MSVC/link.exe";
  relocated.environment.RUSTFLAGS = "-C link-arg=D:/agent/work/target-stream-native/x86_64-pc-windows-msvc/release/deps";
  const normalizedRelocated = normalizeBuildInputs(relocated, {
    targetDir: "D:/agent/work/target-stream-native",
    outputDir: "D:/agent/work/native-bundle",
  });
  assert.deepEqual(normalizedRelocated, normalized);
  assertMatchingBuildInputs(normalized, normalizedRelocated);

  const llvm = validBuildInputs();
  llvm.linker.actual.linker = "C:/LLVM/bin/lld-link.exe";
  llvm.linker.actual.args = ["--crate-name", "acyclic_stream_napi", "-Clinker=C:/LLVM/bin/lld-link.exe"];
  llvm.environment.RUSTFLAGS = "-C linker=C:/LLVM/bin/lld-link.exe";
  Object.assign(llvm.environment, { CARGO_ENCODED_RUSTFLAGS: "-C\x1flinker=C:/LLVM/bin/lld-link.exe" });
  const normalizedLlvm = normalizeBuildInputs(llvm, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });
  assert.notEqual(normalizedLlvm.linker.actual.linker, normalized.linker.actual.linker);
  assert.equal(normalizedLlvm.linker.actual.linker, "<toolchain-path>/LLVM/bin/lld-link.exe");
  assert.equal(normalizedLlvm.linker.actual.args.at(-1), "-Clinker=<toolchain-path>/LLVM/bin/lld-link.exe");
  assert.equal(normalizedLlvm.environment.RUSTFLAGS, "-C linker=<toolchain-path>/LLVM/bin/lld-link.exe");
  assert.equal(normalizedLlvm.environment.CARGO_ENCODED_RUSTFLAGS, "-C\x1flinker=<toolchain-path>/LLVM/bin/lld-link.exe");
});

test("native qualification keeps raw producer paths in an external receipt", () => {
  const raw = validBuildInputs();
  raw.linker.actual.args.push("--out-dir", "C:/runner/_work/target-stream-native/x86_64-pc-windows-msvc/release");
  raw.linker.actual.rustc = "C:/Rust/bin/rustc.exe";
  const published = normalizeBuildInputs(raw, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });
  const receipt = buildInputsReceipt(raw, published);
  assert.equal(receipt.schema, "acyclic.stream.native-build-inputs-receipt.v1");
  assert.equal(receipt.raw_build_inputs.linker.actual.rustc, "C:/Rust/bin/rustc.exe");
  assert.equal(receipt.raw_build_inputs.linker.actual.args.at(-1), "C:/runner/_work/target-stream-native/x86_64-pc-windows-msvc/release");
  assert.equal(receipt.published_build_inputs_sha256.startsWith("sha256:"), true);
  assert.equal(receipt.raw_build_inputs, raw);
  assertBuildInputs(published);
});

test("native qualification ignores diagnostic-only rustc args in the published recipe", () => {
  const original = validBuildInputs();
  const normalized = normalizeBuildInputs(original, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });

  const diagnostic = structuredClone(original);
  diagnostic.linker.actual.args.push("--diagnostic-width=79");
  const normalizedDiagnostic = normalizeBuildInputs(diagnostic, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });
  assert.deepEqual(normalizedDiagnostic, normalized);
  assert.equal(buildInputsReceipt(diagnostic, normalizedDiagnostic).raw_build_inputs.linker.actual.args.at(-1), "--diagnostic-width=79");
  assertMatchingBuildInputs(normalized, normalizedDiagnostic);

  const meaningful = structuredClone(original);
  meaningful.linker.actual.args.push("--cfg", "feature=stream_recipe_change");
  const normalizedMeaningful = normalizeBuildInputs(meaningful, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });
  assert.throws(() => assertMatchingBuildInputs(normalized, normalizedMeaningful), /build input attestation differs/);
});

test("native qualification adds stable Rust path remapping flags", () => {
  assert.deepEqual(
    deterministicRustflags("C:/agent/one", "C:/agent/one/target", undefined, { plain: "-C target-cpu=native" }).split("\x1f"),
    ["-C", "target-cpu=native", "--remap-path-prefix=C:/agent/one=/__acyclic_stream_source", "--remap-path-prefix=C:/agent/one/target=/__acyclic_stream_target"],
  );
  assert.deepEqual(
    deterministicRustflags("D:/agent/two", "D:/agent/two/target", undefined, { plain: null, encoded: null }).split("\x1f"),
    ["--remap-path-prefix=D:/agent/two=/__acyclic_stream_source", "--remap-path-prefix=D:/agent/two/target=/__acyclic_stream_target"],
  );
  assert.deepEqual(
    deterministicRustflags("C:/agent/three", "C:/agent/three/target", undefined, { plain: "ignored_plain", encoded: "encoded_only" }).split("\x1f"),
    ["encoded_only", "--remap-path-prefix=C:/agent/three=/__acyclic_stream_source", "--remap-path-prefix=C:/agent/three/target=/__acyclic_stream_target"],
  );
  assert.deepEqual(
    deterministicRustflags("C:/agent/four", "C:/agent/four/target", undefined, { plain: "ignored_plain", encoded: "" }).split("\x1f"),
    ["--remap-path-prefix=C:/agent/four=/__acyclic_stream_source", "--remap-path-prefix=C:/agent/four/target=/__acyclic_stream_target"],
  );
});

test("native qualification preserves Windows separators in plain Rustflags", () => {
  const flags = deterministicRustflags("C:/src", "C:/target", undefined, {
    plain: "-C linker=C:\\runner\\_work\\target\\lld-link.exe",
  }).split("\x1f");
  assert.deepEqual(flags.slice(0, 2), ["-C", "linker=C:\\runner\\_work\\target\\lld-link.exe"]);
});

test("native qualification restores Rustflags when setup fails", async () => {
  const priorRustflags = process.env.RUSTFLAGS;
  const priorEncoded = process.env.CARGO_ENCODED_RUSTFLAGS;
  const priorIncremental = process.env.CARGO_INCREMENTAL;
  const priorReleaseIncremental = process.env.CARGO_PROFILE_RELEASE_INCREMENTAL;
  process.env.RUSTFLAGS = "-C opt-level=2";
  delete process.env.CARGO_ENCODED_RUSTFLAGS;
  process.env.CARGO_INCREMENTAL = "1";
  process.env.CARGO_PROFILE_RELEASE_INCREMENTAL = "1";
  await assert.rejects(
    withDeterministicRustflags("C:/src", "C:/target", undefined, async () => {
      assert.equal(process.env.RUSTFLAGS, undefined);
      const encodedRustflags = process.env.CARGO_ENCODED_RUSTFLAGS;
      assert.ok(encodedRustflags);
      assert.match(encodedRustflags, /__acyclic_stream_source/u);
      assert.equal(process.env.CARGO_INCREMENTAL, "0");
      assert.equal(process.env.CARGO_PROFILE_RELEASE_INCREMENTAL, "false");
      throw new Error("capture setup failed");
    }),
    /capture setup failed/,
  );
  assert.equal(process.env.RUSTFLAGS, "-C opt-level=2");
  assert.equal(process.env.CARGO_ENCODED_RUSTFLAGS, undefined);
  assert.equal(process.env.CARGO_INCREMENTAL, "1");
  assert.equal(process.env.CARGO_PROFILE_RELEASE_INCREMENTAL, "1");
  if (priorRustflags === undefined) delete process.env.RUSTFLAGS;
  else process.env.RUSTFLAGS = priorRustflags;
  if (priorEncoded === undefined) delete process.env.CARGO_ENCODED_RUSTFLAGS;
  else process.env.CARGO_ENCODED_RUSTFLAGS = priorEncoded;
  if (priorIncremental === undefined) delete process.env.CARGO_INCREMENTAL;
  else process.env.CARGO_INCREMENTAL = priorIncremental;
  if (priorReleaseIncremental === undefined) delete process.env.CARGO_PROFILE_RELEASE_INCREMENTAL;
  else process.env.CARGO_PROFILE_RELEASE_INCREMENTAL = priorReleaseIncremental;
});

test("native qualification restores Darwin rust-lld linker and loader environment", () => {
  const linkerEnvironment = "CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER";
  const priorLinker = process.env[linkerEnvironment];
  const priorLoader = process.env.DYLD_LIBRARY_PATH;
  const priorSdkRoot = process.env.SDKROOT;
  process.env[linkerEnvironment] = "ambient-linker";
  process.env.DYLD_LIBRARY_PATH = "ambient-loader";
  process.env.SDKROOT = "ambient-sdk";
  let restore;
  try {
    const priorPath = process.env.PATH;
    restore = configureDarwinRustLld("aarch64-apple-darwin", { sysroot: "/rust/sysroot", sdkRoot: "/Apple/SDK", driver: "/usr/bin/clang", linkerExists: () => true });
    assert.equal(process.env[linkerEnvironment], "/usr/bin/clang");
    assert.equal(process.env.PATH, `${resolve("/rust/sysroot", "lib", "rustlib", "aarch64-apple-darwin", "bin", "gcc-ld")}${process.platform === "win32" ? ";" : ":"}${priorPath}`);
    assert.equal(process.env.DYLD_LIBRARY_PATH, `${resolve("/rust/sysroot", "lib")}${process.platform === "win32" ? ";" : ":"}ambient-loader`);
    restore();
    restore = undefined;
    assert.equal(process.env[linkerEnvironment], "ambient-linker");
    assert.equal(process.env.DYLD_LIBRARY_PATH, "ambient-loader");
    assert.equal(process.env.SDKROOT, "ambient-sdk");
    assert.equal(process.env.PATH, priorPath);
  } finally {
    restore?.();
    if (priorLinker === undefined) delete process.env[linkerEnvironment];
    else process.env[linkerEnvironment] = priorLinker;
    if (priorLoader === undefined) delete process.env.DYLD_LIBRARY_PATH;
    else process.env.DYLD_LIBRARY_PATH = priorLoader;
    if (priorSdkRoot === undefined) delete process.env.SDKROOT;
    else process.env.SDKROOT = priorSdkRoot;
  }
});

test("native qualification refuses a Darwin toolchain without rust-lld", () => {
  assert.throws(
    () => configureDarwinRustLld("aarch64-apple-darwin", { sysroot: "/rust/sysroot", sdkRoot: "/Apple/SDK", driver: "/usr/bin/clang", linkerExists: () => false }),
    /Rust toolchain rust-lld is unavailable/u,
  );
});

test("native qualification forwards spaces and shell metacharacters through its Windows wrapper", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "stream-native-wrapper-edge-test-"));
  const node = process.execPath.replaceAll("\\", "/");
  const marker = resolve(directory, "args.json").replaceAll("\\", "/");
  const rustcSource = resolve(directory, "rustc.mjs");
  const rustcCommand = resolve(directory, process.platform === "win32" ? "rustc.cmd" : "rustc");
  const driverCommand = resolve(directory, "driver.cmd");
  const priorWrapper = process.env.RUSTC_WRAPPER;
  delete process.env.RUSTC_WRAPPER;
  const args = [rustcCommand, "--crate-name", "acyclic_stream_napi", "--emit=dep-info,link", "--remap-path-prefix=C:/Program Files/SDK=/__source", "-C", "link-arg=C:/a&b/x.dll", "-C", "link-arg=C:/a!b/y.dll"];
  try {
    await writeFile(rustcSource, `import { appendFileSync } from "node:fs"; appendFileSync(${JSON.stringify(marker)}, JSON.stringify(process.argv.slice(2)));\n`);
    if (process.platform === "win32") await writeFile(rustcCommand, `@echo off\r\n"${node}" "${rustcSource}" %*\r\nexit /b %errorlevel%\r\n`);
    else {
      await writeFile(rustcCommand, `#!/bin/sh\nexec ${JSON.stringify(node)} ${JSON.stringify(rustcSource)} "$@"\n`);
      await chmod(rustcCommand, 0o700);
    }
    const capture = await createRustcInvocationCapture();
    try {
      if (process.platform === "win32") {
        const quote = value => `"${value.replaceAll('"', '""')}"`;
        await writeFile(driverCommand, `@echo off\r\ncall ${quote(capture.wrapper)} ${args.map(quote).join(" ")}\r\nexit /b %errorlevel%\r\n`);
        execFileSync(process.env.ComSpec ?? "cmd.exe", ["/d", "/v:off", "/s", "/c", "call", driverCommand], { stdio: "inherit" });
      } else execFileSync(capture.wrapper, args, { stdio: "inherit" });
      const observed = await capture.read("x86_64-pc-windows-msvc");
      assert.ok(observed.args.includes(args[4]));
      assert.ok(observed.args.includes(args[6]));
      assert.ok(observed.args.includes(args[8]));
    } finally {
      await capture.close();
    }
  } finally {
    if (priorWrapper === undefined) delete process.env.RUSTC_WRAPPER;
    else process.env.RUSTC_WRAPPER = priorWrapper;
    await rm(directory, { recursive: true, force: true });
  }
});

test("native qualification records explicit and implicit rustc linkers through a delegated wrapper", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "stream-native-wrapper-test-"));
  const node = process.execPath.replaceAll("\\", "/");
  const marker = resolve(directory, "delegated.txt").replaceAll("\\", "/");
  const rustcSource = resolve(directory, "rustc.mjs");
  const windows = process.platform === "win32";
  const rustcCommand = resolve(directory, windows ? "rustc.cmd" : "rustc");
  const delegateSource = resolve(directory, "delegate.mjs");
  const delegateCommand = resolve(directory, windows ? "delegate.cmd" : "delegate");
  const priorWrapper = process.env.RUSTC_WORKSPACE_WRAPPER;
  const priorPath = process.env.PATH;
  const invoke = (wrapper, args) => windows
    ? execFileSync(process.env.ComSpec ?? "cmd.exe", ["/d", "/s", "/c", "call", wrapper, ...args], { stdio: "inherit" })
    : execFileSync(wrapper, args, { stdio: "inherit" });
  try {
    await writeFile(rustcSource, `import { appendFileSync } from "node:fs"; appendFileSync(${JSON.stringify(marker)}, "rustc\\n");\n`);
    if (windows) await writeFile(rustcCommand, `@echo off\r\n"${node}" "${rustcSource.replaceAll("\\", "/")}" %*\r\nexit /b %errorlevel%\r\n`);
    else {
      await writeFile(rustcCommand, `#!/bin/sh\nexec ${JSON.stringify(node)} ${JSON.stringify(rustcSource)} "$@"\n`);
      await chmod(rustcCommand, 0o700);
    }
    await writeFile(delegateSource, `import { spawnSync } from "node:child_process"; import { appendFileSync } from "node:fs"; const args = process.argv.slice(2); appendFileSync(${JSON.stringify(marker)}, "delegate\\n"); const command = args.shift(); const batch = process.platform === "win32" && /\\.(?:cmd|bat)$/iu.test(command); const result = batch ? spawnSync(process.env.ComSpec ?? "cmd.exe", ["/d", "/s", "/c", command, ...args], { stdio: "inherit" }) : spawnSync(command, args, { stdio: "inherit" }); process.exit(result.status ?? 1);\n`);
    if (windows) await writeFile(delegateCommand, `@echo off\r\n"${node}" "${delegateSource.replaceAll("\\", "/")}" %*\r\nexit /b %errorlevel%\r\n`);
    else {
      await writeFile(delegateCommand, `#!/bin/sh\nexec ${JSON.stringify(node)} ${JSON.stringify(delegateSource)} "$@"\n`);
      await chmod(delegateCommand, 0o700);
    }
    process.env.RUSTC_WORKSPACE_WRAPPER = delegateCommand;
    const capture = await createRustcInvocationCapture();
    try {
      assert.equal(process.env.RUSTC_WORKSPACE_WRAPPER, windows ? "capture.cmd" : "capture");
      assert.notEqual(process.env.PATH, priorPath);
      const remapFlag = "--remap-path-prefix=C:/checkout=$ROOT";
      invoke(capture.wrapper, [rustcCommand, "--crate-name", "acyclic_stream_napi", "--emit=dep-info,link", remapFlag, "-C", "linker=C:/fake/lld-link.exe", "-Clinker=C:/fake/effective-link.exe"]);
      const explicit = await capture.read("x86_64-pc-windows-msvc");
      assert.equal(explicit.linker, "C:/fake/effective-link.exe");
      assert.ok(explicit.args.includes(remapFlag));
      assert.match((await readFile(marker)).toString("utf8"), /delegate/);
    } finally {
      await capture.close();
    }

    const implicitCapture = await createRustcInvocationCapture();
    try {
      invoke(implicitCapture.wrapper, [rustcCommand, "--crate-name", "acyclic_stream_napi", "--emit", "dep-info,link"]);
      const implicit = await implicitCapture.read("x86_64-pc-windows-msvc");
      assert.equal(implicit.linker, null);
    } finally {
      await implicitCapture.close();
    }
  } finally {
    if (priorWrapper === undefined) delete process.env.RUSTC_WORKSPACE_WRAPPER;
    else process.env.RUSTC_WORKSPACE_WRAPPER = priorWrapper;
    assert.equal(process.env.PATH, priorPath);
    await rm(directory, { recursive: true, force: true });
  }
});

test("native capture invokes the exact rustc executable when no nested wrapper is configured", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "stream-native-direct-rustc-test-"));
  const node = process.execPath.replaceAll("\\", "/");
  const rustcSource = resolve(directory, "rustc.mjs");
  const windows = process.platform === "win32";
  const rustcCommand = resolve(directory, windows ? "rustc.cmd" : "rustc");
  const priorWrapper = process.env.RUSTC_WORKSPACE_WRAPPER;
  const invoke = (wrapper, args) => windows
    ? execFileSync(process.env.ComSpec ?? "cmd.exe", ["/d", "/s", "/c", "call", wrapper, ...args], { stdio: "inherit" })
    : execFileSync(wrapper, args, { stdio: "inherit" });
  try {
    await writeFile(rustcSource, "process.exit(0);\n");
    if (windows) await writeFile(rustcCommand, `@echo off\r\n"${node}" "${rustcSource.replaceAll("\\", "/")}" %*\r\nexit /b %errorlevel%\r\n`);
    else {
      await writeFile(rustcCommand, `#!/bin/sh\nexec ${JSON.stringify(node)} ${JSON.stringify(rustcSource)} "$@"\n`);
      await chmod(rustcCommand, 0o700);
    }
    delete process.env.RUSTC_WORKSPACE_WRAPPER;
    const capture = await createRustcInvocationCapture();
    try {
      invoke(capture.wrapper, [rustcCommand, "--crate-name", "acyclic_stream_napi", "--emit=dep-info,link"]);
      const receipt = await capture.read("x86_64-unknown-linux-gnu");
      assert.equal(receipt.rustc, rustcCommand);
    } finally {
      await capture.close();
    }
  } finally {
    if (priorWrapper === undefined) delete process.env.RUSTC_WORKSPACE_WRAPPER;
    else process.env.RUSTC_WORKSPACE_WRAPPER = priorWrapper;
    await rm(directory, { recursive: true, force: true });
  }
});

test("native capture restores nested wrappers and PATH and receipts retain the invoked workspace wrapper", async () => {
  const prior = {
    RUSTC_WRAPPER: process.env.RUSTC_WRAPPER,
    RUSTC_WORKSPACE_WRAPPER: process.env.RUSTC_WORKSPACE_WRAPPER,
    PATH: process.env.PATH,
  };
  const restore = () => {
    for (const [name, value] of Object.entries(prior)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  };
  process.env.RUSTC_WRAPPER = "outer-global-wrapper";
  process.env.RUSTC_WORKSPACE_WRAPPER = "outer-workspace-wrapper";
  try {
    const capture = await createRustcInvocationCapture();
    const invoked = process.env.RUSTC_WORKSPACE_WRAPPER;
    assert.equal(invoked, process.platform === "win32" ? "capture.cmd" : "capture");
    assert.notEqual(process.env.PATH, prior.PATH);
    await capture.close();
    assert.equal(process.env.RUSTC_WRAPPER, "outer-global-wrapper");
    assert.equal(process.env.RUSTC_WORKSPACE_WRAPPER, "outer-workspace-wrapper");
    assert.equal(process.env.PATH, prior.PATH);

    const raw = validBuildInputs();
    assert.ok(invoked);
    Object.assign(raw.environment, { RUSTC_WORKSPACE_WRAPPER: invoked });
    const receipt = buildInputsReceipt(raw, structuredClone(raw));
    assert.equal(receipt.raw_build_inputs.environment.RUSTC_WORKSPACE_WRAPPER, invoked);

    const failing = await createRustcInvocationCapture();
    assert.equal(process.env.RUSTC_WORKSPACE_WRAPPER, invoked);
    try {
      throw new Error("forced capture failure");
    } catch (error) {
      assert.match(error.message, /forced capture failure/u);
    } finally {
      await failing.close();
    }
    assert.equal(process.env.RUSTC_WRAPPER, "outer-global-wrapper");
    assert.equal(process.env.RUSTC_WORKSPACE_WRAPPER, "outer-workspace-wrapper");
    assert.equal(process.env.PATH, prior.PATH);
  } finally {
    restore();
  }
});

test("native builds create a Cargo cache tag only for an empty producer target directory", async () => {
  const empty = await mkdtemp(resolve(tmpdir(), "stream-native-target-tag-test-"));
  const populated = await mkdtemp(resolve(tmpdir(), "stream-native-target-untagged-test-"));
  try {
    await rm(empty, { recursive: true, force: true });
    await ensureCargoTargetDirectory(empty);
    const tag = (await readFile(resolve(empty, "CACHEDIR.TAG"))).toString("utf8");
    assert.match(tag, /^Signature: 8a477f597d28d172789f06886806bc55\n/u);
    await ensureCargoTargetDirectory(empty);
    await writeFile(resolve(empty, "CACHEDIR.TAG"), `${tag}# harmless producer comment\n`);
    await ensureCargoTargetDirectory(empty);
    await writeFile(resolve(empty, "CACHEDIR.TAG"), `Invalid: 8a477f597d28d172789f06886806bc55\n${tag}`);
    await assert.rejects(ensureCargoTargetDirectory(empty), /invalid .*CACHEDIR\.TAG/u);
    await writeFile(resolve(empty, "CACHEDIR.TAG"), tag);
    await writeFile(resolve(populated, "foreign.txt"), "foreign");
    await assert.rejects(ensureCargoTargetDirectory(populated), /pre-existing without a valid CACHEDIR\.TAG/u);
  } finally {
    await rm(empty, { recursive: true, force: true });
    await rm(populated, { recursive: true, force: true });
  }
});
