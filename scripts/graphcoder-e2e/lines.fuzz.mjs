import assert from "node:assert/strict";
import { Lines } from "./lines.mjs";

// Raw bytes are both the payload and the partition schedule. Node's complete
// UTF-8 conversion is the reference; the production decoder owns framing.
export function fuzz(data) {
  const bytes = Buffer.from(data);
  const expected = bytes.toString("utf8").split("\n");
  if (expected.at(-1) === "") expected.pop();
  for (const fragmented of [false, true]) {
    const received = [];
    let refused = false;
    const lines = new Lines(line => {
      received.push(line);
      if (received.length % 2 === 1) { refused = true; return false; }
    });
    for (let offset = 0; offset < bytes.length;) {
      const end = fragmented ? Math.min(bytes.length, offset + 1 + bytes[offset]) : bytes.length;
      refused = false;
      const room = lines.write(bytes.subarray(offset, end));
      assert.equal(room, !refused, "one refused frame must retain chunk backpressure");
      offset = end;
    }
    refused = false;
    assert.equal(lines.end(), !refused);
    assert.deepEqual(received, expected, "framing must be independent of byte partitions");
  }
}
