import { mkdirSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error("usage: build-stream-wasm.mjs [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]");
}
const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
const version = spawnSync(wasmBindgen, ["--version"], { cwd: root, encoding: "utf8" });
if (version.error || version.status !== 0 || version.stdout.trim() !== "wasm-bindgen 0.2.117") {
  throw new Error("wasm-bindgen 0.2.117 is required to build stream WASM");
}
const run = (executable, args) => {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

run(cargo, [
  "build", "--manifest-path", "Cargo.toml", "-p", "acyclic-stream",
  "--no-default-features", "--features", "wasm", "--target", "wasm32-unknown-unknown",
  "--profile", "wasm-release", "--locked",
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
const outputDirectory = resolve(root,
  outputArgument || process.env.ACYCLIC_STREAM_WASM_OUT_DIR || "typescript/packages/stream/generated/wasm",
);
mkdirSync(outputDirectory, { recursive: true });
run(wasmBindgen, [
  resolve(targetDirectory, "wasm32-unknown-unknown", "wasm-release", "acyclic_stream.wasm"),
  "--target", "web", "--out-dir", outputDirectory, "--out-name", "acyclic_stream_wasm",
]);
