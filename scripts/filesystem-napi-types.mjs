import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { generateTypeDef } from "@napi-rs/cli";

export async function render(_, root) {
  // acyclic-stream is a cdylib, whose library Cargo names without a
  // per-configuration hash. Building the binding beside the feature-less
  // contract build would evict its stream library and recompile stream, fs
  // and the binding on every run, so share the native binding build's target
  // directory instead. napi-derive rewrites the stable declaration folder
  // whenever the binding recompiles, so an up-to-date build leaves it current;
  // force a rebuild only when the declarations are missing.
  const target = `${resolve(root, process.env.CARGO_TARGET_DIR ?? "target")}-napi`;
  const typeDefDir = join(target, "napi-type-def");
  mkdirSync(typeDefDir, { recursive: true });
  // Generate only declarations from fresh Rust macro output. The CLI's build
  // command also publishes native binaries/loaders through filesystem locks;
  // those artifacts are qualified separately and are not inputs to this check.
  const generated = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
    "build", "--locked", "--manifest-path", "rust/crates/filesystem-napi/Cargo.toml", "--target-dir", target,
  ], { cwd: root, stdio: "inherit", env: {
    ...process.env,
    NAPI_TYPE_DEF_TMP_FOLDER: typeDefDir,
    ...existsSync(join(typeDefDir, "acyclic-fs-napi")) ? {} : { NAPI_FORCE_BUILD_ACYCLIC_FS_NAPI: `${Date.now()}` },
  } });
  if (generated.status !== 0) {
    throw new Error(`filesystem N-API declaration generation failed: ${generated.status ?? "unknown"}`);
  }
  const { dts, exports } = await generateTypeDef({ typeDefDir, cwd: root });
  if (exports.length === 0 || dts.length === 0) {
    throw new Error("filesystem Rust macros emitted no N-API declarations");
  }
  return { "typescript/packages/filesystem/generated/native/binding.d.ts": dts };
}
