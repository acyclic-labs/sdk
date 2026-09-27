import { copyFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const source = resolve(root, "rust/crates/inference/inference_descriptor.bin");
copyFileSync(source, resolve(root, "rust/crates/inference-contract/inference_descriptor.bin"));
copyFileSync(source, resolve(root, "rust/crates/inference-wasm/inference_reflection_descriptor.bin"));
