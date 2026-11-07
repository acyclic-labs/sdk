import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import { terminalChild } from "./process.mjs";

// Partition raw native stderr, including truncated and mutated announcements.
// The production listener must never select a PID before its complete line.
export async function fuzz(data) {
  const bytes = Buffer.from(data);
  const lines = bytes.toString("utf8").split("\n");
  lines.pop();
  const match = lines.map(line => /^conpty-host primitive=CreatePseudoConsole process-attribute=PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE child-process-id=([1-9]\d*)\r?$/.exec(line))
    .find(value => value && Number(value[1]) <= 0xffff_ffff);
  const expected = match ? Number(match[1]) : "CONPTY_CHILD_UNANNOUNCED";
  for (const partition of ["whole", "bytes", "input"]) {
    const stderr = new PassThrough();
    let selected;
    const child = terminalChild(stderr).then(pid => { selected = pid; return pid; }, error => error.code);
    for (let offset = 0; offset < bytes.length;) {
      const end = partition === "whole" ? bytes.length
        : Math.min(bytes.length, offset + (partition === "bytes" ? 1 : 1 + bytes[offset]));
      stderr.write(bytes.subarray(offset, end));
      await Promise.resolve();
      if (!bytes.subarray(0, end).includes(10)) assert.equal(selected, undefined, "incomplete announcement selected a process");
      offset = end;
    }
    stderr.end();
    assert.equal(await child, expected, "PID selection must be independent of byte partitions and reject incomplete producers");
    assert.equal(stderr.listenerCount("data"), 0, "terminal selection must release its source listener");
  }
}
