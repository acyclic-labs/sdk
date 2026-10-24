import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { generateTypeDef } from "@napi-rs/cli";

// Every binding family shares the enclosing job's target custody and device.
// A sibling of a private bind mount is outside that ownership boundary.
export function declarationTarget(root, environment = process.env) {
  return join(resolve(root, environment.CARGO_TARGET_DIR ?? "target"), "napi");
}

export async function renderTypes(root, family, crates) {
  // Typechecking runs napi-derive without linking native libraries. Keep its
  // Cargo state beside the native binding build. Both the binding and its
  // filesystem dependency emit declarations; force either crate whose output
  // is missing.
  const target = declarationTarget(root);
  const typeDefDir = join(target, "napi-type-def", family);
  mkdirSync(typeDefDir, { recursive: true });
  const hasDeclarations = name => existsSync(join(typeDefDir, name)) && statSync(join(typeDefDir, name)).size > 0;
  const rebuild = Object.fromEntries(crates.filter(name => !hasDeclarations(name))
    .map(name => [`NAPI_FORCE_BUILD_${name.toUpperCase().replaceAll("-", "_")}`, `${Date.now()}`]));
  // Generate only declarations from fresh Rust macro output. The CLI's build
  // command also publishes native binaries/loaders through filesystem locks;
  // those artifacts are qualified separately and are not inputs to this check.
  const generated = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
    "check", "--locked", "--manifest-path", `rust/crates/${family === "filesystem" ? "filesystem" : family}-napi/Cargo.toml`, "--target-dir", target,
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
    throw new Error(`${family} N-API declaration generation failed: ${generated.status ?? "unknown"}`);
  }
  for (const name of crates) {
    if (!hasDeclarations(name)) throw new Error(`${family} Rust macros emitted no N-API declarations for ${name}`);
  }
  const { dts, exports } = await generateTypeDef({ typeDefDir, cwd: root });
  if (exports.length === 0 || dts.length === 0) {
    throw new Error(`${family} Rust macros emitted no N-API declarations`);
  }
  return { [`typescript/packages/${family}/generated/native/binding.d.ts`]: dts };
}

export const render = (_, root) => renderTypes(root, "filesystem", ["acyclic-fs", "acyclic-fs-napi"]);
