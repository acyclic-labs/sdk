import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { generateTypeDef } from "@napi-rs/cli";

export async function render(_, root) {
  // acyclic-stream is a cdylib, whose library Cargo names without a
  // per-configuration hash. Building the binding beside the feature-less
  // contract build would evict its stream library and recompile stream, fs
  // and the binding on every run, so share the native binding build's target
  // directory instead. napi-derive rewrites the stable declaration folder
  // whenever each crate recompiles. Both the binding and its filesystem
  // dependency emit declarations; force either crate whose output is missing.
  const target = `${resolve(root, process.env.CARGO_TARGET_DIR ?? "target")}-napi`;
  const typeDefDir = join(target, "napi-type-def");
  mkdirSync(typeDefDir, { recursive: true });
  const crates = ["acyclic-fs", "acyclic-fs-napi"];
  const hasDeclarations = name => existsSync(join(typeDefDir, name)) && statSync(join(typeDefDir, name)).size > 0;
  const rebuild = Object.fromEntries(crates.filter(name => !hasDeclarations(name))
    .map(name => [`NAPI_FORCE_BUILD_${name.toUpperCase().replaceAll("-", "_")}`, `${Date.now()}`]));
  // Generate only declarations from fresh Rust macro output. The CLI's build
  // command also publishes native binaries/loaders through filesystem locks;
  // those artifacts are qualified separately and are not inputs to this check.
  const generated = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
    "check", "--locked", "--manifest-path", "rust/crates/filesystem-napi/Cargo.toml", "--target-dir", target,
  ], { cwd: root, stdio: "inherit", env: {
    ...process.env,
    NAPI_TYPE_DEF_TMP_FOLDER: typeDefDir,
    // Compiler caches retain Rust artifacts, not napi-derive's external files.
    // Cargo still reuses the target directory's up-to-date dependencies.
    RUSTC_WRAPPER: "",
    RUSTC_WORKSPACE_WRAPPER: "",
    ...rebuild,
  } });
  if (generated.status !== 0) {
    throw new Error(`filesystem N-API declaration generation failed: ${generated.status ?? "unknown"}`);
  }
  for (const name of crates) {
    if (!hasDeclarations(name)) throw new Error(`filesystem Rust macros emitted no N-API declarations for ${name}`);
  }
  const { dts, exports } = await generateTypeDef({ typeDefDir, cwd: root });
  if (exports.length === 0 || dts.length === 0) {
    throw new Error("filesystem Rust macros emitted no N-API declarations");
  }
  return { "typescript/packages/filesystem/generated/native/binding.d.ts": dts };
}
