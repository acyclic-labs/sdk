import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) throw new Error("usage: build-objects-wasm.mjs [OUTPUT_DIR]");
const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
const version = spawnSync(wasmBindgen, ["--version"], { cwd: root, encoding: "utf8" });
if (version.error || version.status !== 0 || version.stdout.trim() !== "wasm-bindgen 0.2.117") {
  throw new Error("wasm-bindgen 0.2.117 is required to build objects WASM");
}
const run = (command, args) => {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

run(cargo, ["build", "-p", "acyclic-objects-wasm", "--target", "wasm32-unknown-unknown", "--profile", "wasm-release", "--locked"]);
const metadata = spawnSync(cargo, ["metadata", "--locked", "--no-deps", "--format-version", "1"], { cwd: root, encoding: "utf8" });
if (metadata.error) throw metadata.error;
if (metadata.status !== 0) throw new Error(metadata.stderr);
const target = JSON.parse(metadata.stdout).target_directory;
run(wasmBindgen, [
  resolve(target, "wasm32-unknown-unknown", "wasm-release", "acyclic_objects_wasm.wasm"),
  "--target", "web",
  "--out-dir", outputArgument ? resolve(outputArgument) : resolve(root, "typescript/packages/objects/generated/wasm"),
  "--out-name", "acyclic_objects_wasm",
]);
