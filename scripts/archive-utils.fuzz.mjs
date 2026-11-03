import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { readBoundedGzip, readDeclaredTarGzip, tarEntries } from "./archive-utils.mjs";

// Keep the declared inventory independent of the returned map: every physical
// member must be delivered once, with the admitted compressed and member bytes.
export function fuzzDeclared(data, path, files, digest, maxExpandedBytes) {
  writeFileSync(path, data);
  let result;
  try { result = readDeclaredTarGzip(path, digest, files, maxExpandedBytes); }
  catch (error) {
    assert.ok(["Z_DATA_ERROR", "Z_BUF_ERROR", "ERR_BUFFER_TOO_LARGE"].includes(error.code)
      || /^archive (exceeds|expands|digest|payload|member|has|lacks) /.test(error.message)
      || error.message === "unsafe, duplicate or unexpected archive member", error);
    return false;
  }
  const hash = bytes => createHash("sha256").update(bytes).digest("hex");
  assert.equal(hash(data), digest, "compressed bytes must match their admitted digest");
  assert.deepEqual(result.compressed, Buffer.from(data));
  const expanded = gunzipSync(data, { maxOutputLength: maxExpandedBytes + 1 });
  assert.ok(expanded.length <= maxExpandedBytes);
  assert.equal(fuzz(expanded), true);
  const entries = tarEntries(expanded);
  assert.equal(entries.length, result.payload.size, "each physical member must be delivered once");
  assert.deepEqual([...result.payload.keys()].sort(), Object.keys(files).sort());
  for (const entry of entries) {
    assert.equal(entry.type, "0", "declared payloads contain only regular files");
    assert.deepEqual(result.payload.get(entry.path), entry.body);
    assert.equal(hash(entry.body), files[entry.path], "member bytes must match their admitted digest");
  }
  return result;
}

// The caller owns an isolated file, matching the package readers' actual path
// boundary. Never hide filesystem failures as invalid compressed inputs.
export function fuzzGzip(data, path, maxExpandedBytes) {
  writeFileSync(path, data);
  let result;
  try { result = readBoundedGzip(path, data.length, maxExpandedBytes); }
  catch (error) {
    assert.ok(["Z_DATA_ERROR", "Z_BUF_ERROR", "ERR_BUFFER_TOO_LARGE"].includes(error.code)
      || /^archive (exceeds its size bound|expands beyond its size bound)$/.test(error.message), error);
    return false;
  }
  assert.deepEqual(result.compressed, Buffer.from(data));
  assert.ok(result.expanded.length <= maxExpandedBytes);
  assert.throws(() => readBoundedGzip(path, data.length - 1, maxExpandedBytes), /archive exceeds its size bound/);
  if (result.expanded.length) {
    assert.throws(() => readBoundedGzip(path, data.length, result.expanded.length - 1),
      /archive expands beyond its size bound/);
  }
  return result;
}

// Exercise the production decoder; accepted bodies must retain their original
// bytes, each header must advance, and the complete archive must authenticate.
export function fuzz(data) {
  const bytes = Buffer.from(data);
  let entries;
  try { entries = tarEntries(bytes)[Symbol.iterator](); }
  catch (error) { return refused(error); }
  let offset = 0;
  for (let visited = 0; visited <= Math.floor(bytes.length / 512); visited++) {
    let next;
    try { next = entries.next(); }
    catch (error) { return refused(error); }
    if (next.done) {
      assert.ok(offset + 1024 <= bytes.length, "two end blocks are required");
      assert.ok(bytes.subarray(offset).every(byte => byte === 0), "nonzero archive trailer");
      return true;
    }
    const { body, size, path, type } = next.value;
    assert.equal(typeof path, "string");
    assert.equal(type.length, 1);
    assert.ok(Number.isSafeInteger(size) && size >= 0);
    assert.equal(body.length, size);
    assert.equal(body.buffer, bytes.buffer);
    assert.equal(body.byteOffset, bytes.byteOffset + offset + 512);
    assert.ok(offset + 512 + size <= bytes.length);
    const header = Buffer.from(bytes.subarray(offset, offset + 512));
    const sizeText = header.toString("latin1", 124, 136).split("\0", 1)[0].trim();
    assert.match(sizeText, /^[0-7]*$/);
    assert.equal(size, sizeText === "" ? 0 : Number.parseInt(sizeText, 8));
    const checksumText = header.toString("latin1", 148, 156).split("\0", 1)[0].trim();
    assert.match(checksumText, /^[0-7]+$/);
    const expected = Number.parseInt(checksumText, 8);
    header.fill(32, 148, 156);
    assert.equal(header.reduce((sum, byte) => sum + byte, 0), expected, "unauthenticated header");
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  assert.fail("TAR decoder failed to advance within its input's header count");
}

function refused(error) {
  assert.equal(error.constructor, Error);
  assert.match(error.message, /^archive (member |has an invalid end marker|lacks an end marker)/);
  return false;
}
