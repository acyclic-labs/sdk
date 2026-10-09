// Synthetic recipes for negative receipt controls, never compiler evidence.
export function validBuildInputs(family = "stream") {
  return {
    schema: `acyclic.${family}.native-build-inputs.v3`,
    target: "x86_64-pc-windows-msvc",
    target_dir: "C:/runner/_work/target-stream-native",
    runtime: { node: "v24.0.0", node_path: "C:/Program Files/nodejs/node.exe", platform: "win32", arch: "x64", bun: { maintained: "1.4.2", actual: null } },
    invocation: { script: `scripts/build-${family}-native.mjs`, runtime: "node", args: ["build", "--target", "x86_64-pc-windows-msvc"] },
    compiler: {
      rustc: { command: "rustc", args: ["--version", "--verbose"], output: "rustc 1.98.1\nhost: x86_64-pc-windows-msvc", executable_sha256: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" },
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
        js_package_name: `@acyclic-labs/${family}`,
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
        args: ["--crate-name", `acyclic_${family}_napi`, "--emit", "dep-info,link"],
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

