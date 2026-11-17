/** Every published WASM package, with the Cargo selection Bazel renders as its wasm targets. */
export const wasmPackages = {
  auth: { selection: ["-p", "acyclic-auth-wasm"], artifact: "acyclic_auth_wasm" },
  actors: { selection: ["-p", "acyclic-actors"], artifact: "acyclic_actors_wasm", outName: "acyclic_actors_wasm" },
  filesystem: { selection: ["-p", "acyclic-fs-wasm"], artifact: "acyclic_fs_wasm" },
  stream: { selection: ["-p", "acyclic-stream", "--no-default-features", "--features", "wasm"], artifact: "acyclic_stream", outName: "acyclic_stream_wasm", wasmPackage: "@acyclic/stream-wasm" },
  objects: { selection: ["-p", "acyclic-objects-wasm"], artifact: "acyclic_objects_wasm" },
  manifest: { selection: ["-p", "acyclic-manifest"], artifact: "acyclic_manifest", outputPackage: "workers" },
  machines: { selection: ["-p", "acyclic-machines-wasm"], artifact: "acyclic_machines_wasm" },
  inference: { selection: ["-p", "acyclic-inference-wasm"], artifact: "acyclic_inference_wasm" },
  harness: {
    selection: ["-p", "acyclic-harness-providers", "--features", "wasm-journal"],
    artifact: "acyclic_harness_providers",
    outName: "acyclic_harness_wasm",
  },
};

/** The Cargo selection of one package. */
export function wasmSelection(name) {
  return wasmPackages[name];
}

/** The wasm roots whose test path ran under wasm-bindgen-test. */
export const wasmTestPackages = new Set(["acyclic-fs-wasm"]);

/** Cargo packages whose Bazel tests run TypeScript that imports a wasm package's generated output. */
export const wasmTestConsumers = {
  "acyclic-stream-typescript-models": "stream",
};
