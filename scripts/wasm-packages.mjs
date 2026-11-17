import { readFileSync } from "node:fs";

/** Every published WASM package, with the Cargo selection Bazel renders as its wasm targets. */
const streamFamily = JSON.parse(readFileSync(new URL("../compatibility/native-families/stream.json", import.meta.url), "utf8"));
const streamWasm = streamFamily.wasm;
export const wasmPackages = {
  auth: { selection: ["-p", "acyclic-auth-wasm"], artifact: "acyclic_auth_wasm" },
  stream: {
    selection: [
      "-p", streamFamily.wasmPackageName,
      ...(streamWasm.noDefaultFeatures ? ["--no-default-features"] : []),
      ...(streamWasm.features.length ? ["--features", streamWasm.features.join(",")] : []),
    ],
    artifact: streamWasm.artifact,
    outName: streamWasm.outName,
    wasmPackage: "@acyclic/stream-wasm",
  },
};

/** The Cargo selection of one package. */
export function wasmSelection(name) {
  return wasmPackages[name];
}

/** The wasm roots whose Cargo test path ran under wasm-bindgen-test. */
export const wasmTestPackages = new Set();

/** Cargo packages whose Bazel tests run TypeScript that imports a wasm package's generated output. */
export const wasmTestConsumers = {
  "acyclic-stream-typescript-models": "stream",
};
