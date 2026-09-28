import { readFileSync } from "node:fs";
import { join } from "node:path";
import { blake3 } from "@noble/hashes/blake3.js";

/** Browser-safe digest constant derived from the exact descriptor embedded by Rust. */
export function filesystemDescriptorDigestSource(root, declaration = false) {
  const descriptor = readFileSync(join(root, "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin"));
  const digest = Buffer.from(blake3(descriptor)).toString("hex");
  return `// Generated from the canonical Rust filesystem descriptor; do not edit.\n` +
    (declaration
      ? `export declare const FILESYSTEM_DESCRIPTOR_DIGEST: "${digest}";\n`
      : `export const FILESYSTEM_DESCRIPTOR_DIGEST = "${digest}";\n`);
}
