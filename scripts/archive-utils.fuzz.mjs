import assert from "node:assert/strict";
import { tarEntries } from "./archive-utils.mjs";

// Byte-oriented target for the production decoder. Seed with captured TAR
// output; mutations may refuse admission but must not escape its error contract,
// stop advancing, copy a body, or admit a partial end marker.
export function fuzz(data) {
  const bytes = Buffer.from(data);
  const entries = tarEntries(bytes);
  let offset = 0;
  for (let visited = 0; visited <= Math.floor(bytes.length / 512); visited++) {
    let next;
    try { next = entries.next(); }
    catch (error) {
      assert.equal(error.constructor, Error);
      assert.match(error.message, /^archive (member |has an invalid end marker|lacks an end marker)/);
      return false;
    }
    if (next.done) {
      assert.ok(offset + 1024 <= bytes.length);
      assert.ok(bytes.subarray(offset).every(byte => byte === 0));
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
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  assert.fail("TAR decoder failed to advance within its input's header count");
}
