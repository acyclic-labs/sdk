import assert from "node:assert/strict";
import { test } from "node:test";
import { create, toBinary } from "@bufbuild/protobuf";
import { MemoryObjectsV1, blobDigest, ErrorCode } from "@acyclic-labs/objects";
import { HttpObjectsV1 } from "@acyclic-labs/objects/http";
import { ObjectsV1Memory } from "../generated/wasm/acyclic_objects_wasm.js";

const failed = code => error => error.code === code;

// The response body and all frames come from the actual Rust account, not a
// reimplementation of its event source, protobuf encoder or cursor rules.
function endpoint(memory, hooks = {}, credentials = { token: "fixture" }) {
  return new HttpObjectsV1({ endpoint: "http://localhost", ...credentials, fetch: async (_url, init) => {
    hooks.opened?.resolve();
    if (hooks.open) await hooks.open;
    const { decode_objects_v1_json } = await import("../generated/wasm/acyclic_objects_wasm.js");
    const input = decode_objects_v1_json("AvailabilityRequest", init.body, 1 << 20);
    const source = memory.open("blobs/availability", input);
    let pending = false, closed = false;
    const close = () => { if (!closed) { closed = true; source.close(); if (!pending) source.free(); } };
    init.signal.addEventListener("abort", close, { once: true });
    return new Response(new ReadableStream({
      async pull(controller) {
        pending = true;
        try {
          const reply = await source.next();
          if (closed || reply === null) { controller.close(); return; }
          const body = hooks.frame?.(reply.body) ?? reply.body;
          if (hooks.fragment) for (const byte of body) controller.enqueue(new Uint8Array([byte]));
          else controller.enqueue(body);
          hooks.delivered?.resolve();
          if (hooks.end) { close(); controller.close(); }
        } catch (error) { controller.error(error); }
        finally { pending = false; if (closed) source.free(); }
      },
      cancel() { init.signal.removeEventListener("abort", close); close(); },
    }), { headers: { "content-type": "application/x-protobuf" } });
  } });
}

test("Objects availability owns opening, catch-up, cursor validation and cancellation", async t => {
  const memory = await MemoryObjectsV1.create();
  const body = new Uint8Array([19, 23, 29]);
  const digest = await blobDigest(body);
  await t.test("memory retains a change before the first wait", async () => {
    const selected = await memory.availability(digest);
    const initial = selected.cursor();
    try {
      await memory.put(body);
      const changed = await selected.changed();
      assert.deepEqual(changed.incarnation, initial.incarnation);
      assert.ok(changed.sequence > initial.sequence);
      // A caller cannot mutate the canonical validator's retained cursor.
      selected.cursor().incarnation.fill(0);
      assert.deepEqual(selected.cursor(), changed);
      const waiting = selected.changed();
      selected.close();
      await assert.rejects(waiting, failed(ErrorCode.UNAVAILABLE));
    } finally { selected.close(); }
  });
  await t.test("already cancelled means no credential resolution or open", async () => {
    let credentials = 0, opens = 0;
    const stop = new AbortController();
    const reason = new Error("caller stopped");
    stop.abort(reason);
    const client = new HttpObjectsV1({ endpoint: "http://localhost", credential: () => { credentials++; return "fixture"; }, fetch: async () => { opens++; throw new Error("unexpected open"); } });
    await assert.rejects(client.availability(digest, stop.signal), error => error === reason);
    assert.equal(credentials, 0);
    assert.equal(opens, 0);
  });
  await t.test("cancelling an original credential wait returns the caller reason", async () => {
    const reached = Promise.withResolvers(), release = Promise.withResolvers();
    const stop = new AbortController(), reason = new Error("credential stop");
    let opens = 0;
    const client = new HttpObjectsV1({ endpoint: "http://localhost", credential: () => { reached.resolve(); return release.promise; }, fetch: async () => { opens++; throw new Error("unexpected open"); } });
    const opening = client.availability(digest, stop.signal);
    await reached.promise;
    stop.abort(reason);
    try { await assert.rejects(opening, error => error === reason); }
    finally { release.resolve("fixture"); }
    assert.equal(opens, 0);
  });
  await t.test("cancelling mid-open settles even an unresponsive transport", async () => {
    const original = new ObjectsV1Memory();
    const opened = Promise.withResolvers(), release = Promise.withResolvers();
    const stop = new AbortController(), reason = new Error("opening stop");
    const opening = endpoint(original, { opened, open: release.promise }).availability(digest, stop.signal);
    await opened.promise;
    stop.abort(reason);
    try { await assert.rejects(opening, error => error === reason); }
    finally { release.reject(reason); original.free(); }
  });
  await t.test("HTTP decodes fragmented real frames and retains catch-up", async () => {
    const original = new ObjectsV1Memory();
    const body = new Uint8Array([43, 47, 53]);
    const digest = await blobDigest(body);
    const selected = await endpoint(original, { fragment: true }).availability(digest);
    try {
      const initial = selected.cursor();
      const { PutBlobRequestSchema } = await import("@acyclic-labs/objects/proto");
      await original.invoke("blobs/put", toBinary(PutBlobRequestSchema, create(PutBlobRequestSchema, { digest, body })));
      const changed = await selected.changed();
      assert.deepEqual(changed.incarnation, initial.incarnation);
      assert.ok(changed.sequence > initial.sequence);
    } finally { selected.close(); original.free(); }
  });
  await t.test("HTTP retains the original producer and rejects a replayed cursor", async () => {
    const original = new ObjectsV1Memory();
    const body = new Uint8Array([31, 37, 41]);
    const digest = await blobDigest(body);
    let first;
    const client = endpoint(original, { frame(bytes) {
      // This negative control replays the shipping source's initial frame in
      // place of its real change; no synthetic protocol fixture is invented.
      if (first === undefined) { first = bytes.slice(); return bytes; }
      return first;
    } });
    const selected = await client.availability(digest);
    try {
      const waiting = selected.changed();
      // The same original account publishes through the shipping route.
      const { PutBlobRequestSchema } = await import("@acyclic-labs/objects/proto");
      await original.invoke("blobs/put", toBinary(PutBlobRequestSchema, create(PutBlobRequestSchema, { digest, body })));
      await assert.rejects(waiting, failed(ErrorCode.UNAVAILABLE));
      await assert.rejects(selected.changed(), failed(ErrorCode.UNAVAILABLE));
    } finally { selected.close(); original.free(); }
  });
  await t.test("HTTP cancellation settles an idle wait and closes its source", async () => {
    const original = new ObjectsV1Memory();
    const stop = new AbortController(), reason = new Error("idle stop");
    const selected = await endpoint(original).availability(digest, stop.signal);
    try {
      const waiting = selected.changed();
      stop.abort(reason);
      await assert.rejects(waiting, error => error === reason);
    } finally { selected.close(); original.free(); }
  });
  await t.test("HTTP source loss ends the original wait with a typed failure", async () => {
    const original = new ObjectsV1Memory();
    const selected = await endpoint(original, { end: true }).availability(digest);
    try { await assert.rejects(selected.changed(), failed(ErrorCode.UNAVAILABLE)); }
    finally { selected.close(); original.free(); }
  });
  await t.test("source invalidation settles an idle HTTP wait", async () => {
    const original = new ObjectsV1Memory();
    const validity = new AbortController();
    // Credential watch is the original authenticated source's terminal event.
    const credential = Object.assign(() => "fixture", { watch: () => ({ validity: { nowUnixSeconds: 0, revision: "one" }, changed: validity.signal, close() {} }) });
    const watched = endpoint(original, {}, { credential });
    const selected = await watched.availability(digest);
    try {
      const waiting = selected.changed();
      validity.abort();
      await assert.rejects(waiting, failed(ErrorCode.ACCESS_DENIED));
    } finally { selected.close(); original.free(); }
  });
});
