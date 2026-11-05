import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { isWasmArtifactPath, wasmArtifactFiles, wasmArtifactInventory, visitTar } from "./archive-utils.mjs";

// The caller records its actual file writes independently of enumeration.
// Invalid trees must refuse, including aliases whose target is a valid tree.
export function fuzzArtifactTree(directory, expected, options) {
  if (expected === null) {
    assert.throws(() => [...wasmArtifactFiles(directory, options)], /WASM artifact admission failed .*; recovery:/);
    return;
  }
  const files = [...wasmArtifactFiles(directory, options)];
  const order = (a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
  assert.deepEqual([...files].sort(order), [...expected].sort(order));
  const inventory = wasmArtifactInventory(files);
  assert.deepEqual(inventory, [...expected].sort(order).map(([path, bytes]) => ({
    path, bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
  })));
  assert.deepEqual(wasmArtifactInventory(files.toReversed()), inventory);
  return inventory;
}

// Exercise the production streaming decoder with native TAR captures and their
// mutations. Body chunks must borrow the input and advance within its length.
export async function fuzz(data) {
  const bytes = Buffer.from(data);
  let offset = 0, visited = 0;
  try {
    await visitTar([bytes], async ({ body, size, path, type }) => {
      assert.ok(++visited <= Math.floor(bytes.length / 512));
      assert.equal(typeof path, "string");
      assert.equal(type.length, 1);
      assert.ok(Number.isSafeInteger(size) && size >= 0);
      const header = Buffer.from(bytes.subarray(offset, offset + 512));
      const sizeText = header.toString("latin1", 124, 136).split("\0", 1)[0].trim();
      assert.match(sizeText, /^[0-7]*$/);
      assert.equal(size, sizeText === "" ? 0 : Number.parseInt(sizeText, 8));
      const checksumText = header.toString("latin1", 148, 156).split("\0", 1)[0].trim();
      assert.match(checksumText, /^[0-7]+$/);
      header.fill(32, 148, 156);
      assert.equal(header.reduce((sum, byte) => sum + byte, 0), Number.parseInt(checksumText, 8), "unauthenticated header");
      let consumed = 0;
      for await (const chunk of body) {
        assert.ok(chunk.length > 0);
        assert.equal(chunk.buffer, bytes.buffer);
        assert.equal(chunk.byteOffset, bytes.byteOffset + offset + 512 + consumed);
        consumed += chunk.length;
        assert.ok(consumed <= size);
      }
      assert.equal(consumed, size);
      offset += 512 + Math.ceil(size / 512) * 512;
    });
  } catch (error) {
    assert.equal(error.constructor, Error);
    assert.match(error.message, /^archive (member |has an invalid end marker|is truncated or lacks an end marker|offset is not representable)/);
    return false;
  }
  assert.ok(offset + 1024 <= bytes.length);
  assert.ok(bytes.subarray(offset).every(byte => byte === 0));
  return true;
}
