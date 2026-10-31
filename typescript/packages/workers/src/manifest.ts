/** Initialize with WASM bytes in Node, then call the Rust-owned validator. */
export { default as initializeManifest, validate, parseSecretReference } from "../generated/wasm/acyclic_manifest.js";
export * from "./generated/manifest.js";
