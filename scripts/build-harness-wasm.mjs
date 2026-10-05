import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { harnessWasmSourceClosure } from "./harness-wasm-source-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error("usage: build-harness-wasm.mjs [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]");
}
const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
const version = spawnSync(wasmBindgen, ["--version"], { cwd: root, encoding: "utf8", windowsHide: true });
if (version.error || version.status !== 0 || version.stdout.trim() !== "wasm-bindgen 0.2.117") {
  throw new Error("wasm-bindgen 0.2.117 is required to build harness WASM");
}
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const revision = spawnSync("git", ["rev-parse", "HEAD"], {
  cwd: root,
  encoding: "utf8",
  windowsHide: true,
});
const run = (executable, args, options = {}) => {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit", windowsHide: true, ...options });
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
  windowsHide: true,
});
if (metadata.error) throw metadata.error;
if (metadata.status !== 0) {
  process.stderr.write(metadata.stderr);
  process.exit(metadata.status ?? 1);
}
const targetDirectory = JSON.parse(metadata.stdout).target_directory;
const generationInputs = harnessWasmSourceClosure(root);
const sourceSnapshot = generationInputs.map(path => ({
  path,
  sha256: sha256(readFileSync(resolve(root, path))),
}));
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
// Older toolchains name it `wasm_bindgen__convert__closures_____invoke__h<hash>`;
// newer ones spell out the closure type with each crate's build hash, which
// Cargo derives from the host triple among other things.
const closureInvokeShim = /^\s*(?:(?:readonly|export const)\s+)?wasm_bindgen_(?:[0-9a-f]+)?_+convert__closures_+invoke_[^:\s]*:.*\r?\n/gm;
for (const declaration of generatedDeclarationFiles) {
  const source = readFileSync(declaration, "utf8");
  const normalized = source.replace(closureInvokeShim, "");
  if (normalized !== source) writeFileSync(declaration, normalized);
}

const generatedArtifacts = [
  "acyclic_harness_wasm.js",
  "acyclic_harness_wasm.d.ts",
  "acyclic_harness_wasm_bg.wasm",
  "acyclic_harness_wasm_bg.wasm.d.ts",
].map(path => ({
  path,
  sha256: sha256(readFileSync(resolve(outputDirectory, path))),
}));
writeFileSync(resolve(outputDirectory, "acyclic_harness_wasm.manifest.json"), `${JSON.stringify({
  version: 1,
  generator: "scripts/build-harness-wasm.mjs",
  wasmBindgen: "0.2.117",
  target: "web",
  cargoProfile: "wasm-release",
  sourceCommit: revision.status === 0 ? revision.stdout.trim() : null,
  sourceSnapshot,
  artifacts: generatedArtifacts,
}, null, 2)}\n`);
