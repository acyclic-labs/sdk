import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error("usage: build-harness-wasm.mjs [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]");
}
const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
const version = spawnSync(wasmBindgen, ["--version"], { cwd: root, encoding: "utf8" });
if (version.error || version.status !== 0 || version.stdout.trim() !== "wasm-bindgen 0.2.117") {
  throw new Error("wasm-bindgen 0.2.117 is required to build harness WASM");
}
const run = (executable, args, options = {}) => {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  return result;
};

run(cargo, [
  "build",
  "--manifest-path", "Cargo.toml",
  "-p", "acyclic-harness",
  "--features", "wasm",
  "--target", "wasm32-unknown-unknown",
  "--profile", "wasm-release",
  "--locked",
]);
const metadata = spawnSync(cargo, ["metadata", "--locked", "--no-deps", "--format-version", "1"], {
  cwd: root,
  encoding: "utf8",
});
if (metadata.error) throw metadata.error;
if (metadata.status !== 0) {
  process.stderr.write(metadata.stderr);
  process.exit(metadata.status ?? 1);
}
const targetDirectory = JSON.parse(metadata.stdout).target_directory;
const outputDirectory = outputArgument || process.env.ACYCLIC_HARNESS_WASM_OUT_DIR
  ? resolve(outputArgument || process.env.ACYCLIC_HARNESS_WASM_OUT_DIR)
  : resolve(root, "typescript/packages/harness/generated/wasm");
run(wasmBindgen, [
  resolve(targetDirectory, "wasm32-unknown-unknown", "wasm-release", "acyclic_harness.wasm"),
  "--target", "web",
  "--out-dir", outputDirectory,
  "--out-name", "acyclic_harness_wasm",
]);

// wasm-bindgen exposes the internal closure invoke shim in its generated
// `InitOutput` declarations.  Its short hash is derived from linker details
// and therefore changes between otherwise equivalent host builds (for
// example, Windows and Linux).  The shim is an implementation detail: it is
// never a supported JS entry point and is not used by the public Harness
// facade.  Omit it from the tracked declaration surface so a clean build is
// byte-for-byte stable across platforms while leaving the generated runtime
// JS/WASM pair untouched.
const generatedDeclarationFiles = [
  resolve(outputDirectory, "acyclic_harness_wasm.d.ts"),
  resolve(outputDirectory, "acyclic_harness_wasm_bg.wasm.d.ts"),
];
const closureInvokeShim = /^\s*(?:(?:readonly|export const)\s+)?wasm_bindgen__convert__closures_____invoke__h[0-9a-f]+(?:_[0-9]+)?:.*\r?\n/gm;
for (const declaration of generatedDeclarationFiles) {
  const source = readFileSync(declaration, "utf8");
  const normalized = source.replace(closureInvokeShim, "");
  if (normalized !== source) writeFileSync(declaration, normalized);
}
