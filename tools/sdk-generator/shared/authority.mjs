import { createHash } from "node:crypto";
import { readFileSync, realpathSync } from "node:fs";
import { isAbsolute, join, posix, relative, sep } from "node:path";

export const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
export const within = (root, path) => {
  const child = relative(root, path);
  return child === "" || (!isAbsolute(child) && child !== ".." && !child.startsWith(`..${sep}`));
};
export const canonical = name => typeof name === "string" && name !== "" && name !== "."
  && !posix.isAbsolute(name) && !name.includes("\\") && !name.includes(":")
  && !name.includes("\0") && !name.split("/").includes("..") && posix.normalize(name) === name;

export function readInput(root, name, digest) {
  if (!canonical(name)) throw new Error("unsafe authority path");
  const path = realpathSync(join(root, name));
  if (!within(root, path)) throw new Error("authority input escapes root");
  const bytes = readFileSync(path);
  if (!/^[a-f0-9]{64}$/.test(digest) || sha256(bytes) !== digest) throw new Error("authority input digest mismatch");
  return bytes;
}

export function loadAuthority(root) {
  const bytes = readFileSync(join(root, "rust-authority.json"));
  const manifest = JSON.parse(bytes);
  if (manifest.schema !== "acyclic.sdk.rust-authority.v1" || manifest.authority !== "rust"
    || !/^[a-f0-9]{40}$/.test(manifest.source_revision) || !Array.isArray(manifest.families)
    || manifest.families.length === 0) throw new Error("not an immutable Rust authority export");
  const sources = new Set();
  const inputs = new Map();
  const descriptors = new Map();
  for (const family of manifest.families) {
    const key = process.platform === "win32" ? family.source?.toLowerCase() : family.source;
    if (sources.has(key)) throw new Error("duplicate authority source");
    sources.add(key);
    for (const field of ["source", ...(family.descriptor ? ["descriptor"] : [])]) {
      inputs.set(family[field], readInput(root, family[field], family[`${field}_sha256`]));
    }
    if (!family.source.endsWith(".proto")) throw new Error("authority source is not a proto file");
    if (family.descriptor) descriptors.set(family.source, family.descriptor);
  }
  if (descriptors.size === 0) throw new Error("authority contains no descriptor sets");
  return { bytes, manifest, inputs, descriptors };
}
