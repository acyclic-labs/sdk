/** Every published WASM package, with the Cargo selection Bazel renders as its wasm targets. */
export const wasmPackages = {
  stream: { selection: ["-p", "acyclic-stream", "--no-default-features", "--features", "wasm"], artifact: "acyclic_stream", outName: "acyclic_stream_wasm", wasmPackage: "@acyclic/stream-wasm" },
};

/** The Cargo selection of one package. */
export function wasmSelection(name) {
  return wasmPackages[name];
}

/** The wasm roots whose cargo test path ran under wasm-bindgen-test. */
export const wasmTestPackages = new Set();

/** Cargo packages whose Bazel tests run TypeScript that imports a wasm package's generated output. */
export const wasmTestConsumers = {
  "acyclic-stream-typescript-models": "stream",
};
