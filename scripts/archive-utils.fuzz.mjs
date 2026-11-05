import assert from "node:assert/strict";
import { visitTar } from "./archive-utils.mjs";

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
