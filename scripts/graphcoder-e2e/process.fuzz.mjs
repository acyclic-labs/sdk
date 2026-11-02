import assert from "node:assert/strict";
import { once } from "node:events";
import { createWriteStream, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { Spool, SpoolEnded } from "./process.mjs";

// Fuzz the deployed Spool ingestion and completed-prefix reader together.
// The stream is a real Node byte stream; process-tree lifecycle and provider
// acceptance remain separate controls. Seed from retained native stdout.
export async function fuzz(data, termination = { code: 0, signal: null }) {
  const root = mkdtempSync(join(tmpdir(), "spool-byte-fuzz-"));
  const bytes = Buffer.from(data), width = 1 + (bytes[0] ?? 0);
  const transcript = createWriteStream(join(root, "events.jsonl"));
  const closed = once(transcript, "close");
  closed.catch(() => {});
  function* chunks() {
    for (let offset = 0; offset < bytes.length; offset += width) yield bytes.subarray(offset, offset + width);
  }
  try {
    const events = new Spool(Readable.from(chunks()), Promise.resolve(termination), { transcript });
    const delivered = [];
    events.subscribe(event => delivered.push(event));
    const terminal = await events.next(() => false).catch(error => error);
    assert.ok(terminal instanceof SpoolEnded);
    assert.ok(["eof", "malformed"].includes(terminal.kind));
    if (terminal.kind === "eof") assert.equal(terminal.detail, termination);
    else assert.ok(terminal.cause instanceof SyntaxError);
    const read = [];
    for await (const event of events.records()) read.push(event);
    assert.deepEqual(read, delivered);
    // Every late wait must observe the very same terminal, including its cause.
    await assert.rejects(events.next(() => true), error => error === terminal);
    await assert.rejects(events.keyed("unpublished"), error => error === terminal);
  } finally {
    transcript.end();
    try { await closed; }
    finally { rmSync(root, { recursive: true, force: true }); }
  }
}
