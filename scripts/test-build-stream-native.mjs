import { validBuildInputs } from "./fixtures/native-build-inputs.mjs";
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import test from "node:test";
/** @typedef {ReturnType<typeof validBuildInputs>} BuildInputs */

import { assertBuildInputs, assertBundle, assertExactInventory, assertMatchingBuildInputs, assertOwnedDirectory, assertSourceSnapshot, buildInputsReceipt, capturedCompilerIdentity, configureDarwinAppleLd, createRustcInvocationCapture, darwinAppleLdPaths, darwinRustObjcopyIdentity, deterministicRustflags, ensureCargoTargetDirectory, linkerInputs, normalizeBuildInputs, prepareBuildOutput, publishBundle, signDarwinAddon, sourceSnapshot, withDeterministicRustflags } from "./build-stream-native.mjs";

const root = fileURLToPath(new URL("..", import.meta.url));

test("generator identity hashes the loaded maintained CommonJS entry", async () => {
  const require = createRequire(import.meta.url);
  const resolved = require.resolve("@napi-rs/cli");
  const loaded = require("@napi-rs/cli");
  const cached = require.cache[resolved];
  assert.ok(cached);
  assert.equal(cached.exports, loaded);
  assert.equal(typeof loaded.NapiCli.prototype, "object");
  assert.equal(typeof new loaded.NapiCli().build, "function");
  const { napiGeneratorIdentity } = await import("./build-stream-native.mjs");
  const identity = await napiGeneratorIdentity();
  assert.equal(identity.entry_sha256, `sha256:${createHash("sha256").update(await readFile(resolved)).digest("hex")}`);
  const esmEntry = fileURLToPath(import.meta.resolve("@napi-rs/cli"));
  assert.notEqual(identity.entry_sha256, `sha256:${createHash("sha256").update(await readFile(esmEntry)).digest("hex")}`);
});

test("native build preparation creates a missing nested parent before Cargo discovery", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-prepare-"));
  const output = resolve(parent, "missing", "nested", "bundle");
  const priorPath = process.env.PATH;
  const priorWindowsPath = process.env.Path;
  try {
    process.env.PATH = ""; process.env.Path = "";
    const candidate = await prepareBuildOutput(output);
    assert.equal((await readdir(candidate)).length, 0);
    assert.ok(candidate.startsWith(resolve(parent, "missing", "nested")));
    assert.deepEqual((await readdir(parent)), ["missing"]);
  } finally {
    if (priorPath === undefined) delete process.env.PATH; else process.env.PATH = priorPath;
    if (priorWindowsPath === undefined) delete process.env.Path; else process.env.Path = priorWindowsPath;
    await rm(parent, { recursive: true, force: true });
  }
});

test("native build preparation refuses an existing unowned output without creating scratch or deleting data", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-prepare-owned-"));
  const output = resolve(parent, "bundle");
  try {
    await mkdir(output);
    await writeFile(resolve(output, "keep.txt"), "keep\n");
    await assert.rejects(prepareBuildOutput(output), /ENOENT/);
    assert.equal(await readFile(resolve(output, "keep.txt"), "utf8"), "keep\n");
    assert.deepEqual(await readdir(parent), ["bundle"]);
  } finally { await rm(parent, { recursive: true, force: true }); }
});

test("native bundle validation rejects self-consistent manifests with missing or incorrectly selected artifacts", async () => {
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"], { cwd: root, encoding: "utf8" }));
  const rustPackage = metadata.packages.find(item => item.name === "acyclic-stream-napi");
  const targets = rustPackage.metadata.napi.targets;
  const digest = bytes => `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  for (const mutation of ["valid", "missing-loader", "missing-declarations", "multiple-nodes", "wrong-selected", "omitted-artifact", "input-target"]) {
    const output = await mkdtemp(resolve(tmpdir(), "stream-native-bundle-contract-"));
    try {
      let names = ["binding.cjs", "binding.d.ts", "addon.node"];
      if (mutation === "missing-loader") names = names.filter(name => name !== "binding.cjs");
      if (mutation === "missing-declarations") names = names.filter(name => name !== "binding.d.ts");
      if (mutation === "multiple-nodes") names.push("second.node");
      const artifacts = [];
      for (const name of names.sort()) {
        const bytes = Buffer.from(`fixture ${name}\n`);
        await writeFile(resolve(output, name), bytes);
        artifacts.push({ path: `generated/native/${name}`, sha256: digest(bytes), bytes: bytes.length });
      }
      if (mutation === "omitted-artifact") artifacts.splice(artifacts.findIndex(item => item.path.endsWith("binding.d.ts")), 1);
      const generation = { schema: "acyclic.stream.native-generation.v1", package: rustPackage.name, version: rustPackage.version, source_path: "rust/crates/stream-napi/Cargo.toml", targets, selected_target: targets[0], build_inputs: validBuildInputs(), artifacts };
      if (mutation === "input-target") {
        generation.build_inputs.target = "x86_64-unknown-linux-gnu";
        generation.build_inputs.generator.options.target = "x86_64-unknown-linux-gnu";
        generation.build_inputs.linker.actual.target = "x86_64-unknown-linux-gnu";
      }
      const generationBytes = Buffer.from(JSON.stringify(generation));
      await writeFile(resolve(output, "generation-manifest.json"), generationBytes);
      const selected = artifacts.find(item => item.path.endsWith(mutation === "wrong-selected" ? "binding.cjs" : ".node"));
      await writeFile(resolve(output, "native-targets.json"), JSON.stringify({ ...generation, schema: "acyclic.stream.native-targets.v1", generation_manifest: "generated/native/generation-manifest.json", generation_sha256: digest(generationBytes), artifact: selected }));
      const operation = assertBundle(output, { verifySource: false });
      if (mutation === "valid") await operation;
      else await assert.rejects(operation, mutation.startsWith("missing") ? /missing generated binding loader or declarations/ : mutation === "multiple-nodes" ? /exactly one .node artifact/ : mutation === "wrong-selected" ? /selected artifact is not the actual .node artifact/ : mutation === "input-target" ? /build input target differs from selected target/ : /artifact attestation differs from actual bundle/);
    } finally { await rm(output, { recursive: true, force: true }); }
  }
});

test("native build requires an output bundle before discovering Cargo", () => {
  const result = spawnSync(process.execPath, [resolve(root, "scripts/build-stream-native.mjs"), "build", "--target", "x86_64-pc-windows-msvc"], {
    env: { ...process.env, PATH: "", Path: "" }, encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /build requires --output <native-bundle>/u);
  assert.doesNotMatch(result.stderr, /ENOENT|requires a clean source closure|spawn.*cargo/u);
});

test("native build rejects a workspace compiler delegate before source checks, tools, or output writes", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-workspace-wrapper-"));
  const output = resolve(parent, "missing", "native-bundle");
  try {
    const result = spawnSync(process.execPath, [resolve(root, "scripts/build-stream-native.mjs"), "build", "--target", "x86_64-pc-windows-msvc", "--output", output], {
      env: { ...process.env, PATH: "", Path: "", RUSTC_WORKSPACE_WRAPPER: resolve(parent, "mutating-delegate") }, encoding: "utf8",
    });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /native build provenance does not support RUSTC_WORKSPACE_WRAPPER/u);
    assert.doesNotMatch(result.stderr, /ENOENT|requires a clean source closure|spawn.*cargo/u);
    assert.deepEqual(await readdir(parent), [], "rejection must precede parent or scratch creation");
  } finally { await rm(parent, { recursive: true, force: true }); }
});

test("native compiler identity binds the captured executable instead of PATH preflight", async () => {
  const bytes = await readFile(process.execPath);
  const sha256 = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  const expected = { command: process.execPath, args: ["--version", "--verbose"], output: "captured compiler version", executable_sha256: sha256 };
  const identity = (command, args, executable) => {
    assert.equal(command, process.execPath);
    assert.deepEqual(args, expected.args);
    assert.equal(executable, process.execPath);
    return expected;
  };
  assert.equal(capturedCompilerIdentity({ rustc: process.execPath }, "x86_64-pc-windows-msvc", { output: "PATH compiler version" }, identity), expected);
  assert.throws(() => capturedCompilerIdentity({ rustc: process.execPath }, "aarch64-apple-darwin", { output: "PATH compiler version" }, identity), /differs from Apple toolchain preflight/u);
  assert.equal(capturedCompilerIdentity({ rustc: process.execPath }, "aarch64-apple-darwin", expected, identity), expected);
});

test("non-Darwin builds do not discover Apple signing tools or read signing artifacts", () => {
  for (const target of ["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"]) {
    assert.equal(signDarwinAddon("nonexistent-signing-output", target), null);
    assert.equal(darwinRustObjcopyIdentity(target, {
      output: () => { throw new Error("unexpected tool discovery"); },
      optional: () => { throw new Error("unexpected version probe"); },
      identity: () => { throw new Error("unexpected executable read"); },
    }), null);
  }
});

test("Darwin strip preflight installs only the matching active toolchain's missing LLVM tools", () => {
  const libdir = resolve(tmpdir(), "pinned-rust/lib/rustlib/aarch64-apple-darwin/lib");
  const executable = resolve(libdir, "../bin/rust-objcopy");
  const calls = [];
  let available = false;
  const identity = { command: executable, args: ["--version"], output: "LLVM 22", executable_sha256: `sha256:${"a".repeat(64)}` };
  const commands = {
    output(command, args) {
      calls.push([command, ...args]);
      if (args[0] === "show") return "1.98.1-aarch64-apple-darwin (overridden by rust-toolchain.toml)";
      if (args[0] === "component") { available = true; return ""; }
      if (args.includes("--target")) return resolve(tmpdir(), "pinned-rust/lib/rustlib/x86_64-apple-darwin/lib");
      return libdir;
    },
    optional() { return available ? identity : null; },
    identity(command, args, path) {
      assert.equal(command, executable); assert.equal(path, executable); assert.deepEqual(args, ["--version"]);
      if (!available) throw new Error("strip helper still unavailable");
      return identity;
    },
  };
  assert.deepEqual(darwinRustObjcopyIdentity("x86_64-apple-darwin", commands), identity);
  assert.deepEqual(calls.at(-1), ["rustup", "component", "add", "llvm-tools-preview", "--toolchain", "1.98.1-aarch64-apple-darwin"]);
  calls.length = 0;
  darwinRustObjcopyIdentity("aarch64-apple-darwin", commands);
  assert.equal(calls.some(([command]) => command === "rustup"), false, "working strip helper must not alter the installed toolchain");
  available = false;
  assert.throws(() => darwinRustObjcopyIdentity("aarch64-apple-darwin", { ...commands,
    output: (command, args) => args[0] === "run" ? "another-sysroot" : commands.output(command, args),
  }), /differs from the Darwin Rust compiler/);
  assert.throws(() => darwinRustObjcopyIdentity("aarch64-apple-darwin", { ...commands,
    identity: () => { throw new Error("strip helper still unavailable"); },
  }), /strip helper still unavailable/);
});

test("Apple strip identity is required, normalized, and protected against mutation", () => {
  const tool = { command: "/pinned/rust/bin/rust-objcopy", args: ["--version"], output: "LLVM 22", executable_sha256: `sha256:${"a".repeat(64)}` };
  const valid = { ...validBuildInputs(), linker: { ...validBuildInputs().linker, apple: {
    sdk: { path: "/SDK", version: "26", build: "test" }, clang: tool, ld: tool, codesign: tool, rust_objcopy: tool,
  } } };
  assertBuildInputs(valid);
  const normalized = normalizeBuildInputs(valid, { targetDir: "C:/runner/_work/target-stream-native", outputDir: "C:/runner/_work/native-bundle" });
  assert.equal(normalized.linker.apple.rust_objcopy.command, "rust-objcopy");
  assert.equal(buildInputsReceipt(valid, normalized).raw_build_inputs.linker.apple.rust_objcopy.command, tool.command);
  const changed = structuredClone(valid);
  changed.linker.apple.rust_objcopy.executable_sha256 = `sha256:${"b".repeat(64)}`;
  assert.throws(() => assertMatchingBuildInputs(valid, changed), /build input attestation differs/);
  const missing = structuredClone(valid);
  Reflect.deleteProperty(missing.linker.apple, "rust_objcopy");
  assert.throws(() => assertBuildInputs(missing), /rust_objcopy/);
});

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
  assert.ok(darwin.includes("link-arg=-Wl,-final_output,/__acyclic_stream_output/libacyclic_stream_napi.dylib"));
  assert.ok(darwin.includes("link-arg=-Wl,-reproducible"));
  const applePaths = darwinAppleLdPaths("aarch64-apple-darwin", "/Apple/SDK");
  assert.deepEqual(applePaths, {
    linkerEnvironment: "CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER",
    linker: "/usr/bin/ld",
    driver: "/usr/bin/clang",
    sdkRoot: "/Apple/SDK",
  });
  assert.equal(darwinAppleLdPaths("x86_64-pc-windows-msvc", "/Apple/SDK"), null);
});

test("native qualification rejects build input identity mutations", () => {
  const valid = validBuildInputs();
  assertBuildInputs(valid);

  /** @type {Array<[string, (value: BuildInputs) => void, RegExp]>} */
  const mutations = [
    ["compiler", value => { value.compiler.rustc.output = "rustc 1.99.0"; }, /build input attestation differs/],
    ["compiler binary", value => { value.compiler.rustc.executable_sha256 = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; }, /build input attestation differs/],
    ["generator", value => { value.generator.version = "3.10.4"; }, /build input attestation differs/],
    ["generator options", value => { value.generator.options.output_dir = "C:/runner/_work/other-bundle"; }, /build input attestation differs/],
    ["bun", value => { value.runtime.bun.maintained = "1.4.1"; }, /build input attestation differs/],
    ["invocation", value => { value.invocation.script = "scripts/other-build.mjs"; }, /build input attestation differs/],
    ["linker", value => { value.linker.actual.linker = "C:/Program Files/LLVM/lld-link.exe"; }, /build input attestation differs/],
    ["linker target", value => { value.linker.actual.target = "x86_64-unknown-linux-gnu"; }, /linker invocation target differs from build input target/],
    ["environment", value => { value.environment.RUSTFLAGS = "-C opt-level=3"; }, /build input attestation differs/],
    ["profile", value => { value.profile.name = "dev"; }, /native build profile is not release/],
    ["incremental policy", value => { Object.assign(value.profile, { cargo_incremental: null }); }, /incremental policy is not pinned to zero/],
    ["generator cargo options", value => { value.generator.options.cargo_options = ["--offline"]; }, /generator cargo options are invalid/],
    ["cache", value => { Object.assign(value.cache, { wrapper: 42 }); }, /native build input cache\.wrapper is invalid/],
  ];
  for (const [label, mutate, expected] of mutations) {
    const mutated = structuredClone(valid);
    mutate(mutated);
    if (["compiler", "compiler binary", "generator", "generator options", "bun", "invocation", "linker", "environment"].includes(label)) assert.throws(() => assertMatchingBuildInputs(valid, mutated), expected);
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

  const missingDarwinIdentity = structuredClone(valid);
  missingDarwinIdentity.target = "aarch64-apple-darwin";
  missingDarwinIdentity.generator.options.target = missingDarwinIdentity.target;
  assert.throws(() => assertBuildInputs(missingDarwinIdentity), /missing Apple tool identity/);

  const darwinLinker = linkerInputs("aarch64-apple-darwin", {
    CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER: "/usr/bin/clang",
    DYLD_LIBRARY_PATH: "/rust/sysroot/lib",
    SDKROOT: "/Applications/Xcode.app/SDKs/MacOSX.sdk",
  }, { attestApple: false });
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
  assert.equal(normalized.invocation.runtime, "node");
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

test("native qualification restores Darwin Apple linker environment", () => {
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
    restore = configureDarwinAppleLd("aarch64-apple-darwin", { sdkRoot: "/Apple/SDK", driver: "/usr/bin/clang", rustSysroot: "/Rust/sysroot", linkerExists: () => true });
    assert.equal(process.env[linkerEnvironment], "/usr/bin/clang");
    assert.equal(process.env.PATH, priorPath);
    assert.equal(process.env.DYLD_LIBRARY_PATH, resolve("/Rust/sysroot", "lib"));
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

test("native qualification selects the supplied Darwin SDK for clang", () => {
  const restore = configureDarwinAppleLd("aarch64-apple-darwin", {
    sdkRoot: "/Applications/Custom SDK.sdk",
    driver: "/Applications/Custom SDK.sdk/usr/bin/clang",
    rustSysroot: "/Rust/sysroot",
    linkerExists: () => true,
  });
  assert.equal(process.env.SDKROOT, "/Applications/Custom SDK.sdk");
  assert.equal(process.env.CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER, "/Applications/Custom SDK.sdk/usr/bin/clang");
  restore();
});

test("native qualification refuses a Darwin host without Apple ld", () => {
  assert.throws(
    () => configureDarwinAppleLd("aarch64-apple-darwin", { sdkRoot: "/Apple/SDK", driver: "/usr/bin/clang", linkerExists: () => false }),
    /Apple ld is unavailable/u,
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

// This exercises forwarding/restoration only; native build rejects an original
// workspace delegate because forwarding cannot attest its final compiler argv.
test("capture helper forwards and restores nested wrappers and PATH", async () => {
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

test("native builds reject a target directory symlink before creating cache state", async () => {
  const parent = await mkdtemp(resolve(tmpdir(), "stream-native-target-link-test-"));
  const target = resolve(parent, "target");
  const destination = resolve(parent, "destination");
  try {
    await symlink(destination, target, process.platform === "win32" ? "junction" : "dir");
    await assert.rejects(ensureCargoTargetDirectory(target), /symlink or junction/u);
    assert.equal((await readdir(parent)).includes("target"), true);
    await assert.rejects(readFile(resolve(target, "CACHEDIR.TAG")));
  } finally {
    await rm(parent, { recursive: true, force: true });
  }
});
