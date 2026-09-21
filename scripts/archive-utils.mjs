import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { gunzipSync } from "node:zlib";

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

function field(block, offset, length) {
  const end = block.indexOf(0, offset);
  return block.subarray(offset, end === -1 || end > offset + length ? offset + length : end).toString("utf8");
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
