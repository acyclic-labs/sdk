import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const usage = "usage: build-wasm.mjs [PACKAGE [OUTPUT_DIR [CARGO [WASM_BINDGEN]]]]";

// wasm-bindgen exposes the internal closure invoke shim in its generated
// `InitOutput` declarations. Its short hash is derived from linker details
// and therefore changes between otherwise equivalent host builds (for
// example, Windows and Linux). The shim is an implementation detail: it is
// never a supported JS entry point and is not used by the public Harness
// facade. Omit it from the tracked declaration surface so a clean build is
// byte-for-byte stable across platforms while leaving the generated runtime
// JS/WASM pair untouched. Older toolchains name it
// `wasm_bindgen__convert__closures_____invoke__h<hash>`; newer ones spell out
// the closure type with each crate's build hash, which Cargo derives from the
// host triple among other things.
const closureInvokeShim = /^\s*(?:(?:readonly|export const)\s+)?wasm_bindgen_(?:[0-9a-f]+)?_+convert__closures_+invoke_[^:\s]*:.*\r?\n/gm;
const stripHarnessClosureShims = output => {
  for (const name of ["acyclic_harness_wasm.d.ts", "acyclic_harness_wasm_bg.wasm.d.ts"]) {
    const declaration = resolve(output, name);
    const source = readFileSync(declaration, "utf8");
    const normalized = source.replace(closureInvokeShim, "");
    if (normalized !== source) writeFileSync(declaration, normalized);
  }
};

// wasm-bindgen cannot express the relationship between the route argument and
// the projected response because the Rust ABI quite correctly exposes JsValue.
// The route/response map itself is emitted by the Rust route macro; this
// narrow declaration rewrite preserves that generated relationship at the
// TypeScript boundary without adding a handwritten runtime assertion.
const narrowMachinesDecoder = output => {
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
};

// TypeScript package -> cargo package selection, built artifact, bindgen
// output name, and optional post-processing of the generated directory.
const packages = {
  filesystem: { cargo: ["-p", "acyclic-fs-wasm"], artifact: "acyclic_fs_wasm", outName: "acyclic_fs_wasm" },
  stream: {
    cargo: ["-p", "acyclic-stream", "--no-default-features", "--features", "wasm"],
    artifact: "acyclic_stream",
    outName: "acyclic_stream_wasm",
  },
  objects: { cargo: ["-p", "acyclic-objects-wasm"], artifact: "acyclic_objects_wasm", outName: "acyclic_objects_wasm" },
  machines: {
    cargo: ["-p", "acyclic-machines-wasm"],
    artifact: "acyclic_machines_wasm",
    outName: "acyclic_machines_wasm",
    postprocess: narrowMachinesDecoder,
  },
  inference: { cargo: ["-p", "acyclic-inference-wasm"], artifact: "acyclic_inference_wasm", outName: "acyclic_inference_wasm" },
  harness: {
    cargo: ["-p", "acyclic-harness", "--features", "wasm"],
    artifact: "acyclic_harness",
    outName: "acyclic_harness_wasm",
    postprocess: stripHarnessClosureShims,
  },
};

const [packageArgument, outputArgument, cargoArgument, wasmBindgenArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0 || (packageArgument && !Object.hasOwn(packages, packageArgument))) {
  throw new Error(`${usage}\nPACKAGE is one of: ${Object.keys(packages).join(", ")}`);
}

const cargo = cargoArgument || process.env.ACYCLIC_CARGO_BIN || "cargo";
const capture = (executable, args) => {
  const result = spawnSync(executable, args, { cwd: root, encoding: "utf8" });
  return result.error || result.status !== 0 ? undefined : result.stdout.trim();
};
const run = (executable, args) => {
  const result = spawnSync(executable, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

// The CLI must match the wasm-bindgen crate pinned in the workspace manifest.
const pinned = readFileSync(resolve(root, "Cargo.toml"), "utf8").match(/^wasm-bindgen = "=([^"]+)"/m)?.[1];
if (!pinned) throw new Error("Cargo.toml no longer pins wasm-bindgen to an exact version");
const expectedVersion = `wasm-bindgen ${pinned}`;
const resolveWasmBindgen = () => {
  const explicit = wasmBindgenArgument || process.env.ACYCLIC_WASM_BINDGEN_BIN;
  if (explicit) return explicit;
  if (capture("wasm-bindgen", ["--version"]) === expectedVersion) return "wasm-bindgen";
  // Fall back to the pinned resolver, which installs the CLI when missing.
  const script = resolve(root, "scripts", "ensure-wasm-bindgen.sh");
  const gitBash = process.platform === "win32" ? capture("cygpath", ["-w", "/bin/bash.exe"]) : undefined;
  return gitBash
    ? capture(gitBash, [capture("cygpath", ["-u", script]) ?? script])
    : capture("bash", [script.replaceAll("\\", "/")]);
};
const wasmBindgen = resolveWasmBindgen();
if (!wasmBindgen || capture(wasmBindgen, ["--version"]) !== expectedVersion) {
  throw new Error(`${expectedVersion} is required to build WASM packages`);
}

const metadata = capture(cargo, ["metadata", "--locked", "--no-deps", "--format-version", "1"]);
if (!metadata) throw new Error("cargo metadata failed");
const releaseDirectory = resolve(JSON.parse(metadata).target_directory, "wasm32-unknown-unknown", "wasm-release");

for (const name of packageArgument ? [packageArgument] : Object.keys(packages)) {
  const { cargo: selection, artifact, outName, postprocess } = packages[name];
  const output = outputArgument ? resolve(outputArgument) : resolve(root, "typescript/packages", name, "generated/wasm");
  run(cargo, ["build", ...selection, "--target", "wasm32-unknown-unknown", "--profile", "wasm-release", "--locked"]);
  mkdirSync(output, { recursive: true });
  run(wasmBindgen, [
    resolve(releaseDirectory, `${artifact}.wasm`),
    "--target", "web", "--out-dir", output, "--out-name", outName,
  ]);
  postprocess?.(output);
}
