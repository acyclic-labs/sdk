import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { nativeFamily } from "./native-family-metadata.mjs";

const closureInvokeShim = /^\s*(?:(?:readonly|export const)\s+)?wasm_bindgen_(?:[0-9a-f]+)?_+convert__closures_+invoke_[^:\s]*:.*\r?\n/gm;
const stripHarnessClosureShims = output => {
  for (const name of ["acyclic_harness_wasm.d.ts", "acyclic_harness_wasm_bg.wasm.d.ts"]) {
    const declaration = resolve(output, name);
    const source = readFileSync(declaration, "utf8");
    const normalized = source.replace(closureInvokeShim, "");
    if (normalized !== source) writeFileSync(declaration, normalized);
  }
};

/** Every published WASM package. Building and checking both read this one table. */
export const wasmPackages = {
  auth: { cargo: ["-p", "acyclic-auth-wasm"], artifact: "acyclic_auth_wasm", runtime: true, build: true },
  actors: { family: true, runtime: true, build: true },
  filesystem: { cargo: ["-p", "acyclic-fs-wasm"], artifact: "acyclic_fs_wasm" },
  stream: { family: true, runtime: true, runtimeDirectory: "node_modules" },
  objects: { cargo: ["-p", "acyclic-objects-wasm"], artifact: "acyclic_objects_wasm" },
  manifest: { cargo: ["-p", "acyclic-manifest"], artifact: "acyclic_manifest", outputPackage: "workers" },
  machines: {
    cargo: ["-p", "acyclic-machines-wasm"],
    artifact: "acyclic_machines_wasm",
    runtime: true,
    build: true,
  },
  inference: { cargo: ["-p", "acyclic-inference-wasm"], artifact: "acyclic_inference_wasm", runtime: true, build: true },
  harness: {
    cargo: ["-p", "acyclic-harness-providers", "--features", "wasm-journal"],
    artifact: "acyclic_harness_providers",
    outName: "acyclic_harness_wasm",
    postprocess: stripHarnessClosureShims,
    targetDirectory: "harness-wasm",
  },
};

/** The Cargo selection and outputs of one package; a native family reads its committed provenance. */
export function wasmSelection(name) {
  const entry = wasmPackages[name];
  if (!entry.family) {
    const { cargo: selection, artifact, outName = artifact, postprocess, targetDirectory } = entry;
    return { selection, artifact, outName, postprocess, targetDirectory };
  }
  const family = nativeFamily(name);
  const { artifact, outName, targetDirectory } = family.wasm;
  const selection = ["-p", family.wasmPackageName,
    ...(family.wasm.noDefaultFeatures ? ["--no-default-features"] : []),
    ...(family.wasm.features.length ? ["--features", family.wasm.features.join(",")] : [])];
  return { selection, artifact, outName, targetDirectory, family };
}

// Tests that consume the assembled runtime rather than a checkout's generated tree.
export const wasmTestConsumers = { "acyclic-stream-typescript-models": "stream" };
