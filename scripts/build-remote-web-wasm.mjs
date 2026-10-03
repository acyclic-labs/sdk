import { mkdirSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) {
  throw new Error("usage: build-remote-web-wasm.mjs [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]");
}
const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
const version = spawnSync(wasmBindgen, ["--version"], { cwd: root, encoding: "utf8" });
if (version.error || version.status !== 0 || version.stdout.trim() !== "wasm-bindgen 0.2.117") {
  throw new Error("wasm-bindgen 0.2.117 is required for the remote-web WASM boundary");
}
const run = (executable, args) => {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};
const manifest = resolve(root, "rust/crates/sdk-remote-web/Cargo.toml");
run(cargo, ["build", "--manifest-path", manifest, "--target", "wasm32-unknown-unknown", "--profile", "wasm-release", "--locked"]);
const metadata = spawnSync(cargo, ["metadata", "--manifest-path", manifest, "--locked", "--no-deps", "--format-version", "1"], {
  cwd: root,
  encoding: "utf8",
});
if (metadata.error) throw metadata.error;
if (metadata.status !== 0) {
  process.stderr.write(metadata.stderr);
  process.exit(metadata.status ?? 1);
}
const targetDirectory = JSON.parse(metadata.stdout).target_directory;
const outputs = outputArgument
  ? [resolve(outputArgument)]
  : [
      resolve(root, "typescript/packages/actors/generated/wasm"),
      resolve(root, "typescript/packages/workers/generated/wasm"),
    ];
for (const output of outputs) {
  mkdirSync(output, { recursive: true });
  run(wasmBindgen, [
    resolve(targetDirectory, "wasm32-unknown-unknown", "wasm-release", "acyclic_sdk_remote_web.wasm"),
    "--target", "web", "--out-dir", output, "--out-name", "acyclic_remote_web_wasm",
  ]);
}

