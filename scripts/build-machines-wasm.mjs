import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [outputArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) throw new Error("usage: build-machines-wasm.mjs [OUTPUT_DIR]");
const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
const wasmBindgen = process.env.ACYCLIC_WASM_BINDGEN_BIN || "wasm-bindgen";
const version = spawnSync(wasmBindgen, ["--version"], { cwd: root, encoding: "utf8" });
if (version.error || version.status !== 0 || version.stdout.trim() !== "wasm-bindgen 0.2.117") {
  throw new Error("wasm-bindgen 0.2.117 is required to build machines WASM");
}
const run = (command, args) => {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

run(cargo, ["build", "-p", "acyclic-machines-wasm", "--target", "wasm32-unknown-unknown", "--profile", "wasm-release", "--locked"]);
const metadata = spawnSync(cargo, ["metadata", "--locked", "--no-deps", "--format-version", "1"], { cwd: root, encoding: "utf8" });
if (metadata.error) throw metadata.error;
if (metadata.status !== 0) throw new Error(metadata.stderr);
const target = JSON.parse(metadata.stdout).target_directory;
run(wasmBindgen, [
  resolve(target, "wasm32-unknown-unknown", "wasm-release", "acyclic_machines_wasm.wasm"),
  "--target", "web",
  "--out-dir", outputArgument ? resolve(outputArgument) : resolve(root, "typescript/packages/machines/generated/wasm"),
  "--out-name", "acyclic_machines_wasm",
]);

// wasm-bindgen cannot express the relationship between the route argument and
// the projected response because the Rust ABI quite correctly exposes JsValue.
// The route/response map itself is emitted by the Rust route macro above; this
// narrow declaration rewrite preserves that generated relationship at the
// TypeScript boundary without adding a handwritten runtime assertion.
const output = outputArgument
  ? resolve(outputArgument)
  : resolve(root, "typescript/packages/machines/generated/wasm");
const declarationPath = resolve(output, "acyclic_machines_wasm.d.ts");
const declaration = readFileSync(declarationPath, "utf8");
const decoder = "static decodeHttpResponse(route: MachinesHttpRoute, response_json: string, expected_json: string): MachinesHttpResponseUnion;";
if (!declaration.includes(decoder)) {
  throw new Error("Machines WASM declaration no longer contains the expected HTTP decoder signature");
}
writeFileSync(
  declarationPath,
  declaration.replace(
    decoder,
    "static decodeHttpResponse<Route extends MachinesHttpRoute>(route: Route, response_json: string, expected_json: string): MachinesHttpResponse<Route>;",
  ),
);
