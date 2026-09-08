import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error("usage: build-harness-wasm.mjs [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]");
}
const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
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
  "--no-default-features",
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
