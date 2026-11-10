import {readFileSync, readdirSync} from "node:fs";
import {join} from "node:path";

// Manual caller-array copies must stay behind the admitted byte snapshot.
export const assertByteSnapshotBoundary = (file, source) => {
  // Rust slice accessors return Rust-owned bytes, never a caller's JS array.
  // Their spelling/formatting must not turn an output copy into an input refusal.
  const rustOwned = source
    .replaceAll("cursor.commit.to_vec()", "")
    .replaceAll("prepared.bytes().to_vec()", "")
    .replace(/\.(?:as_bytes|as_slice)\(\)\s*\.to_vec\(\)/g, "")
    .replaceAll(".map(|digest| digest.to_vec())", "")
    .replace(/(canonical_json_digest\([^;]+?)\.to_vec\(\)/g, "$1");
  if (/\.\s*(?:to_vec|copy_to|copy_to_uninit|raw_copy_to_ptr)\s*\(|Uint8Array::new\s*\(/.test(rustOwned)) {
    throw new Error(`${file}: copy caller arrays through acyclic_stream::javascript_bytes::snapshot`);
  }
};
export const checkByteSnapshotBoundary = directory => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const file = join(directory, entry.name);
    if (entry.isDirectory()) checkByteSnapshotBoundary(file);
    else if (entry.name.endsWith(".rs")) assertByteSnapshotBoundary(file, readFileSync(file, "utf8").replaceAll("\r\n", "\n"));
  }
};
