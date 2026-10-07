import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmod, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { assertBuildInputs, assertExactInventory, assertMatchingBuildInputs, assertSourceSnapshot, buildInputsReceipt, createRustcInvocationCapture, deterministicRustflags, linkerInputs, normalizeBuildInputs, sourceSnapshot } from "./build-stream-native.mjs";

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
      },
    },
    linker: {
      configured: { target: null },
      environment: { LINK: null, CC: null, AR: null, RUSTC_LINKER: null, VCINSTALLDIR: null, VCToolsInstallDir: null, WindowsSdkDir: null, VisualStudioVersion: null },
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
  assert.deepEqual(flags.slice(0, 5), ["-C", "opt-level=3", "-C", "link-arg=C:/tool path/extra.lib", "--cfg"]);
  assert.ok(flags.includes("prior_flag"));
  assert.ok(flags.includes("-C"));
  assert.equal(flags.at(-3), "target-feature=+crt-static");
  assert.equal(flags.at(-1), "link-arg=/Brepro");
  assert.ok(flags.some(flag => flag.includes("--remap-path-prefix=C:/agent checkout/sdk=/__acyclic_stream_source")));
  assert.ok(flags.some(flag => flag.includes("--remap-path-prefix=C:/cargo target=/__acyclic_stream_target")));
});

test("native qualification rejects build input identity mutations", () => {
  const valid = validBuildInputs();
  assertBuildInputs(valid);

  for (const [label, mutate, expected] of [
    ["compiler", value => { value.compiler.rustc.output = "rustc 1.99.0"; }, /build input attestation differs/],
    ["generator", value => { value.generator.version = "3.10.4"; }, /build input attestation differs/],
    ["generator options", value => { value.generator.options.output_dir = "C:/runner/_work/other-bundle"; }, /build input attestation differs/],
    ["bun", value => { value.runtime.bun.maintained = "1.4.1"; }, /build input attestation differs/],
    ["invocation", value => { value.invocation.script = "scripts/other-build.mjs"; }, /build input attestation differs/],
    ["linker", value => { value.linker.actual.linker = "C:/Program Files/LLVM/lld-link.exe"; }, /build input attestation differs/],
    ["environment", value => { value.environment.RUSTFLAGS = "-C opt-level=3"; }, /build input attestation differs/],
    ["profile", value => { value.profile.name = "dev"; }, /native build profile is not release/],
    ["cache", value => { value.cache.wrapper = 42; }, /native build input cache\.wrapper is invalid/],
  ]) {
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
  const normalizedLlvm = normalizeBuildInputs(llvm, {
    targetDir: "C:/runner/_work/target-stream-native",
    outputDir: "C:/runner/_work/native-bundle",
  });
  assert.notEqual(normalizedLlvm.linker.actual.linker, normalized.linker.actual.linker);
  assert.equal(normalizedLlvm.linker.actual.linker, "<toolchain-path>/LLVM/bin/lld-link.exe");
  assert.equal(normalizedLlvm.linker.actual.args.at(-1), "-Clinker=<toolchain-path>/LLVM/bin/lld-link.exe");
  assert.equal(normalizedLlvm.environment.RUSTFLAGS, "-C linker=<toolchain-path>/LLVM/bin/lld-link.exe");
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

test("native qualification adds stable Rust path remapping flags", () => {
  assert.deepEqual(
    deterministicRustflags("C:/agent/one", "C:/agent/one/target", undefined, { plain: "-C target-cpu=native" }).split("\x1f"),
    ["-C", "target-cpu=native", "--remap-path-prefix=C:/agent/one=/__acyclic_stream_source", "--remap-path-prefix=C:/agent/one/target=/__acyclic_stream_target"],
  );
  assert.deepEqual(
    deterministicRustflags("D:/agent/two", "D:/agent/two/target", undefined, { plain: null, encoded: null }).split("\x1f"),
    ["--remap-path-prefix=D:/agent/two=/__acyclic_stream_source", "--remap-path-prefix=D:/agent/two/target=/__acyclic_stream_target"],
  );
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
  const priorWrapper = process.env.RUSTC_WRAPPER;
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
    process.env.RUSTC_WRAPPER = delegateCommand;
    const capture = await createRustcInvocationCapture();
    try {
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
    if (priorWrapper === undefined) delete process.env.RUSTC_WRAPPER;
    else process.env.RUSTC_WRAPPER = priorWrapper;
    await rm(directory, { recursive: true, force: true });
  }
});
