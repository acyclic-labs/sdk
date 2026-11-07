import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { OutputWatch } from "./process.mjs";

const original = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url)))
  .captures["text-responses"].stdout;
const terminal = JSON.parse(readFileSync(new URL("./fixtures/live-terminal-query-refusal.json", import.meta.url))).stdout;

// Whole-buffer native UTF-8 conversion and substring positions independently
// check the original incremental watch. The final ASCII delimiter releases an
// incomplete UTF-8 prefix: OutputWatch has no separate end-of-stream method.
export function fuzz(data) {
  const generated = Buffer.from(data);
  for (const bytes of [Buffer.concat([generated, Buffer.from("\n")]),
    ...[original, terminal].map(capture => Buffer.concat([Buffer.from(capture), generated, Buffer.from(capture)]))]) {
    const text = bytes.toString("utf8");
    const needles = [...new Set([original.trim(), "λ", "🙂", "streamed answer", "\x1b[?25h", "\x1b[?25l", "overflowed",
      text.slice(0, 8), text.slice(-8)])].filter(Boolean);
    for (const partition of ["whole", "bytes", "input"]) {
      const watch = new OutputWatch(needles);
      for (let offset = 0; offset < bytes.length;) {
        const end = partition === "whole" ? bytes.length
          : Math.min(bytes.length, offset + (partition === "bytes" ? 1 : 1 + bytes[offset]));
        watch.write(bytes.subarray(offset, end)); offset = end;
      }
      for (const needle of needles) {
        assert.equal(watch.has(needle), text.includes(needle), "raw presence must survive byte/UTF-8 partitions");
        for (const other of needles) assert.equal(watch.later(needle, other),
          text.lastIndexOf(needle) > text.lastIndexOf(other), "raw ordering must retain the final occurrence");
      }
    }
  }
}
