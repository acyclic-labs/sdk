import assert from "node:assert/strict";
import { Lines } from "./lines.mjs";

// Raw bytes are both the payload and the partition schedule. Node's complete
// UTF-8 conversion is the reference; the production decoder owns framing.
export function fuzz(data) {
  const bytes = Buffer.from(data);
  const expected = bytes.toString("utf8").split("\n");
  if (expected.at(-1) === "") expected.pop();
  for (const requireNewline of [false, true]) for (const partition of ["whole", "bytes", "input"]) {
    const received = [];
    let refused = false;
    const lines = new Lines(line => {
      received.push(line);
      if (received.length % 2 === 1) { refused = true; return false; }
    });
    for (let offset = 0; offset < bytes.length;) {
      const end = partition === "whole" ? bytes.length
        : Math.min(bytes.length, offset + (partition === "bytes" ? 1 : 1 + bytes[offset]));
      refused = false;
      const room = lines.write(bytes.subarray(offset, end));
      assert.equal(room, !refused, "one refused frame must retain chunk backpressure");
      offset = end;
    }
    refused = false;
    const incomplete = bytes.length > 0 && bytes.at(-1) !== 10;
    if (requireNewline && incomplete) assert.throws(() => lines.end({ requireNewline }), { code: "UNTERMINATED_LINE" });
    else assert.equal(lines.end({ requireNewline }), !refused);
    assert.deepEqual(received, requireNewline && incomplete ? expected.slice(0, -1) : expected,
      "framing must be independent of byte partitions");
  }
}
