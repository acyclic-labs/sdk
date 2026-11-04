import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { Lines } from "./lines.mjs";

test("line framing preserves captured UTF-8 across partitions with one visit per decoded character", t => {
  const captured = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url), "utf8")).captures;
  const original = JSON.parse(captured["jsonl-reopen"].stdout);
  const indexOf = String.prototype.indexOf, split = String.prototype.split;
  // Amplify only the retained shipping answer. Chunk boundaries cut inside
  // its actual multibyte characters, CRLF and the final unterminated reply.
  for (const copies of [1, 128]) for (const size of [1, 7, 64]) {
    const first = JSON.stringify({ ...original, text: captured["text-responses"].stdout.repeat(copies) });
    const last = JSON.stringify(original), source = first + "\r\n" + last;
    const bytes = Buffer.from(source), received = [], room = [];
    let visits = 0;
    const lines = new Lines(line => { received.push(line); return received.length !== 1; });
    try {
      t.mock.method(String.prototype, "indexOf", function (separator, start = 0) {
        const end = indexOf.call(this, separator, start);
        if (separator === "\n") visits += (end === -1 ? this.length : end + 1) - start;
        return end;
      });
      t.mock.method(String.prototype, "split", function (separator, limit) {
        if (separator === "\n") visits += this.length;
        return split.call(this, separator, limit);
      });
      for (let offset = 0; offset < bytes.length; offset += size) room.push(lines.write(bytes.subarray(offset, offset + size)));
      room.push(lines.end());
    } finally { t.mock.restoreAll(); }
    assert.deepEqual(received, [first + "\r", last]);
    assert.ok(room.includes(false), "backpressure from any completed line must survive later lines in that chunk");
    assert.equal(visits, source.length, "a chunk must not rescan the prefix retained from earlier chunks");
  }
});
