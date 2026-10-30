import { createHash } from "node:crypto";
import { lstatSync, readFileSync, readdirSync, statSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { join } from "node:path";

export function sha256(payload) {
  return createHash("sha256").update(payload).digest("hex");
}

export function readBoundedGzip(path, maxArchiveBytes, maxExpandedBytes) {
  const size = statSync(path).size;
  if (size <= 0 || size > maxArchiveBytes) throw new Error("archive exceeds its size bound");
  const compressed = readFileSync(path);
  if (compressed.length !== size) throw new Error("archive changed while it was being read");
  const expanded = gunzipSync(compressed, { maxOutputLength: maxExpandedBytes + 1 });
  if (expanded.length > maxExpandedBytes) throw new Error("archive expands beyond its size bound");
  return { compressed, expanded };
}

// Package readers admit canonical paths and exact digest inventories before loading archive bytes.
export function readDeclaredTarGzip(path, expectedDigest, files, maxExpandedBytes) {
  const { compressed, expanded } = readBoundedGzip(path, 16 * 1024 * 1024, maxExpandedBytes);
  if (sha256(compressed) !== expectedDigest) throw new Error("archive digest differs");
  const payload = new Map();
  for (const entry of tarEntries(expanded)) {
    if (entry.type !== "0" || payload.has(entry.path) || !Object.hasOwn(files, entry.path)) {
      throw new Error("unsafe, duplicate or unexpected archive member");
    }
    if (sha256(entry.body) !== files[entry.path]) throw new Error("archive payload digest differs");
    payload.set(entry.path, entry.body);
  }
  if (payload.size !== Object.keys(files).length) throw new Error("archive payload inventory differs");
  return { compressed, payload };
}

function field(block, offset, length) {
  const end = block.indexOf(0, offset);
  return block.subarray(offset, end === -1 || end > offset + length ? offset + length : end).toString("utf8");
}

export function tarPayload(bytes, isCanonical, caseInsensitive = process.platform === "win32") {
  const seen = new Set(), payload = new Map();
  for (const entry of tarEntries(bytes)) {
    const name = entry.type === "5" ? entry.path.replace(/\/$/, "") : entry.path;
    const key = caseInsensitive ? name.toLowerCase() : name;
    if (!isCanonical(name) || seen.has(key) || !["0", "5"].includes(entry.type)) throw new Error("unsafe or duplicate archive member");
    seen.add(key);
    if (entry.type === "0") payload.set(name, entry.body);
  }
  if (!payload.size) throw new Error("empty archive payload");
  return payload;
}

export function tarEntries(tar) {
  const result = [];
  let offset = 0;
  while (offset + 512 <= tar.length) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every(byte => byte === 0)) {
      if (offset + 1024 > tar.length || !tar.subarray(offset).every(byte => byte === 0)) {
        throw new Error("archive has an invalid end marker");
      }
      return result;
    }
    const rawChecksum = field(header, 148, 8).trim();
    if (!/^[0-7]+$/.test(rawChecksum)) throw new Error("archive member has an invalid checksum");
    const checksum = header.reduce(
      (sum, byte, index) => sum + (index >= 148 && index < 156 ? 32 : byte),
      0,
    );
    if (checksum !== Number.parseInt(rawChecksum, 8)) throw new Error("archive member checksum mismatch");
    const name = field(header, 0, 100);
    const prefix = field(header, 345, 155);
    const path = prefix ? `${prefix}/${name}` : name;
    const rawSize = field(header, 124, 12).trim();
    if (rawSize !== "" && !/^[0-7]+$/.test(rawSize)) throw new Error("archive member has an invalid size");
    const size = rawSize === "" ? 0 : Number.parseInt(rawSize, 8);
    const type = String.fromCharCode(header[156] || 48);
    if (!Number.isSafeInteger(size) || size < 0) throw new Error("archive member has an invalid size");
    const bodyStart = offset + 512;
    const bodyEnd = bodyStart + size;
    if (bodyEnd > tar.length) throw new Error("archive member is truncated");
    result.push({ path, type, size, body: tar.subarray(bodyStart, bodyEnd) });
    offset = bodyStart + Math.ceil(size / 512) * 512;
  }
  throw new Error("archive lacks an end marker");
}

// wasm-bindgen emits flat bindings plus inline JavaScript under snippets/.
export function isWasmArtifactPath(path) {
  const parts = path.split("/");
  return parts.every(part => part !== "" && part !== "." && part !== ".." && !part.includes("\\"))
    && (parts.length === 1 || (parts[0] === "snippets" && parts.length > 1));
}

/** Read the complete emitted tree without following symbolic links.
 * @returns {Generator<[string, Buffer]>}
 */
export function* wasmArtifactFiles(directory, { receipt = false } = {}) {
  const root = lstatSync(directory);
  if (!root.isDirectory() || root.isSymbolicLink()) {
    throw new Error(`WASM artifact admission failed at ${directory}: expected a real directory; recovery: bun scripts/build-wasm.mjs`);
  }
  /** @returns {Generator<[string, Buffer]>} */
  function* visit(parent, prefix) {
    for (const entry of readdirSync(parent, { withFileTypes: true })) {
      const path = prefix + entry.name;
      if (entry.isDirectory() && !entry.isSymbolicLink() && (path === "snippets" || path.startsWith("snippets/"))) {
        yield* visit(join(parent, entry.name), `${path}/`);
      } else if (entry.isFile() && !entry.isSymbolicLink() && isWasmArtifactPath(path)) {
        if (receipt || path !== "producer-receipt.json") yield [`generated/wasm/${path}`, readFileSync(join(parent, entry.name))];
      } else {
        throw new Error(`WASM artifact admission failed at ${path}: non-regular artifact; recovery: bun scripts/build-wasm.mjs`);
      }
    }
  }
  yield* visit(directory, "");
}

export function wasmArtifactInventory(files) {
  return Array.from(files, ([path, bytes]) => ({
    path, bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
  })).sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
}
